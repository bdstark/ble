package att

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bdstark/ble"
)

// NotificationHandler handles notification or indication.
//
// The req slice is valid only for the duration of the HandleNotification
// call: it is backed by a pooled buffer that is reused for later
// notifications as soon as the handler returns. A handler that retains the
// data past its return must copy it.
type NotificationHandler interface {
	HandleNotification(req []byte)
}

// Client implementa an Attribute Protocol Client.
type Client struct {
	l2c ble.Conn

	// rspc carries response PDUs from Loop to the waiting sendReq. It is
	// buffered (cap 1) and Loop's send is non-blocking: ATT is a sequential
	// protocol, so at most one response is outstanding, but a request can be
	// abandoned (ctx cancellation or seqProtoTimeout) with its response still
	// in flight. An unbuffered send of that late response would park Loop
	// forever and wedge the whole inbound HCI path behind it.
	rspc chan []byte

	// rspDropped counts response PDUs Loop dropped because no request was
	// waiting for them and the rspc buffer already held an unclaimed
	// response (see RspDropped).
	rspDropped atomic.Uint64

	// bearerClosed is set when a transaction hits the ATT sequential-protocol
	// timeout. Per [Vol 3, Part F, 3.3.3] the bearer "shall be closed" at that
	// point: any response arriving later could otherwise be mis-attributed to
	// the next same-opcode request. Once set, every request fails fast with
	// ErrBearerClosed instead of touching the (closing) connection.
	bearerClosed atomic.Bool

	// abandonedRsp / abandonedDeadline record a request abandoned by context
	// cancellation after it reached the wire: the response opcode it still
	// expects and the absolute transaction deadline it was subject to. ATT is
	// a sequential protocol, so that transaction still owns the bearer — the
	// next request resolves it (sendReq waits for its response or its
	// deadline) before writing, otherwise a straggling same-opcode or Error
	// Response would be consumed as the new request's answer: silently stale
	// data. Both fields are touched only while holding the tx buffer
	// (chTxBuf), which serializes requests, so they need no locking. A zero
	// abandonedRsp means no abandoned transaction is outstanding.
	abandonedRsp      byte
	abandonedDeadline time.Time

	rxBuf   []byte
	chTxBuf chan []byte
	chErr   chan error
	handler NotificationHandler
}

// NewClient returns an Attribute Protocol Client.
func NewClient(l2c ble.Conn, h NotificationHandler) *Client {
	c := &Client{
		l2c:     l2c,
		rspc:    make(chan []byte, 1),
		chTxBuf: make(chan []byte, 1),
		rxBuf:   make([]byte, ble.MaxMTU),
		chErr:   make(chan error, 1),
		handler: h,
	}
	c.chTxBuf <- make([]byte, l2c.TxMTU(), l2c.TxMTU())
	return c
}

// ExchangeMTU informs the server of the client’s maximum receive MTU size and
// request the server to respond with its maximum receive MTU size. [Vol 3, Part F, 3.4.2.1]
func (c *Client) ExchangeMTU(ctx context.Context, clientRxMTU int) (serverRxMTU int, err error) {
	if clientRxMTU < ble.DefaultMTU || clientRxMTU > ble.MaxMTU {
		return 0, ErrInvalidArgument
	}

	// Acquire and reuse the txBuf, and release it after usage.
	// The same txBuf, or a newly allocate one, if the txMTU is changed,
	// will be released back to the channel.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ExchangeMTURequest(txBuf[:3])
	req.SetAttributeOpcode()
	req.SetClientRxMTU(uint16(clientRxMTU))

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return 0, err
	}

	// Convert and validate the response.
	rsp := ExchangeMTUResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return 0, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) != 3:
		return 0, ErrInvalidResponse
	}

	// Validate the server's MTU before adopting it: a value below the spec
	// minimum would shrink txBuf under the fixed-size request headers and
	// panic a later txBuf[:n] slice.
	serverRxMTU = int(rsp.ServerRxMTU())
	if serverRxMTU < ble.DefaultMTU {
		return 0, fmt.Errorf("server MTU %d below minimum %d: %w", serverRxMTU, ble.DefaultMTU, ErrInvalidMTU)
	}

	// ATT_MTU is the minimum of what each side can receive [Vol 3, Part F,
	// 3.4.2.2] — a server advertising more than we asked for must not
	// tempt us past our own request. The new MTU applies only now, after a
	// successful exchange: failure paths above leave the connection's
	// RxMTU untouched instead of raising it for an exchange that never
	// happened.
	txMTU := min(serverRxMTU, clientRxMTU)
	c.l2c.SetRxMTU(clientRxMTU)
	if len(txBuf) != txMTU {
		// Let L2CAP know the MTU that the remote device can handle.
		c.l2c.SetTxMTU(txMTU)
		// Put a re-allocated txBuf back to the channel.
		// The txBuf has been captured in deferred function.
		txBuf = make([]byte, txMTU)
	}

	return txMTU, nil
}

// roundTrip performs one ATT request-response transaction: it acquires the
// shared tx buffer, lets build marshal the request PDU into it, sends the
// request, and validates the response envelope. A well-formed ATT Error
// Response (5 bytes) surfaces as ble.ATTError; a malformed error response,
// an unexpected opcode, or a response shorter than minLen is
// ErrInvalidResponse.
//
// The returned PDU is the response's own allocation (never the tx buffer),
// so callers may hand out aliasing sub-slices with unbounded lifetime.
func (c *Client) roundTrip(ctx context.Context, wantRsp byte, minLen int, build func(txBuf []byte) []byte) ([]byte, error) {
	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	rsp, err := c.sendReq(ctx, build(txBuf))
	if err != nil {
		return nil, err
	}

	// Validate the response.
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != wantRsp:
		fallthrough
	case len(rsp) < minLen:
		return nil, ErrInvalidResponse
	}
	return rsp, nil
}

// FindInformation obtains the mapping of attribute handles with their associated types.
// This allows a Client to discover the list of attributes and their types on a server.
// [Vol 3, Part F, 3.4.3.1 & 3.4.3.2]
func (c *Client) FindInformation(ctx context.Context, starth, endh uint16) (fmt int, data []byte, err error) {
	if starth == 0 || starth > endh {
		return 0x00, nil, ErrInvalidArgument
	}

	b, err := c.roundTrip(ctx, FindInformationResponseCode, 6, func(txBuf []byte) []byte {
		req := FindInformationRequest(txBuf[:5])
		req.SetAttributeOpcode()
		req.SetStartingHandle(starth)
		req.SetEndingHandle(endh)
		return req
	})
	if err != nil {
		return 0x00, nil, err
	}

	// The information data must hold complete entries of the declared
	// format, and the format itself must be one the spec defines — an
	// unknown format would make the caller guess an entry size and walk
	// off the end of the data.
	rsp := FindInformationResponse(b)
	switch rsp.Format() {
	case 0x01:
		if ((len(rsp) - 2) % 4) != 0 {
			return 0x00, nil, ErrInvalidResponse
		}
	case 0x02:
		if ((len(rsp) - 2) % 18) != 0 {
			return 0x00, nil, ErrInvalidResponse
		}
	default:
		return 0x00, nil, ErrInvalidResponse
	}
	return int(rsp.Format()), rsp.InformationData(), nil
}

// // HandleInformationList ...
// type HandleInformationList []byte
//
// // FoundAttributeHandle ...
// func (l HandleInformationList) FoundAttributeHandle() []byte { return l[:2] }
//
// // GroupEndHandle ...
// func (l HandleInformationList) GroupEndHandle() []byte { return l[2:4] }
//
// // FindByTypeValue ...
// func (c *Client) FindByTypeValue(starth, endh, attrType uint16, value []byte) ([]HandleInformationList, error) {
// 	return nil, nil
// }

// ReadByType obtains the values of attributes where the attribute type is known
// but the handle is not known. [Vol 3, Part F, 3.4.4.1 & 3.4.4.2]
func (c *Client) ReadByType(ctx context.Context, starth, endh uint16, uuid ble.UUID) (int, []byte, error) {
	if starth > endh || (len(uuid) != 2 && len(uuid) != 16) {
		return 0, nil, ErrInvalidArgument
	}

	b, err := c.roundTrip(ctx, ReadByTypeResponseCode, 4, func(txBuf []byte) []byte {
		req := ReadByTypeRequest(txBuf[:5+len(uuid)])
		req.SetAttributeOpcode()
		req.SetStartingHandle(starth)
		req.SetEndingHandle(endh)
		req.SetAttributeType(uuid)
		return req
	})
	if err != nil {
		return 0, nil, err
	}

	// The data list must hold complete entries of the declared length. The
	// length byte is peer-controlled; reject zero before it divides.
	rsp := ReadByTypeResponse(b)
	if rsp.Length() < 1 || len(rsp.AttributeDataList())%int(rsp.Length()) != 0 {
		return 0, nil, ErrInvalidResponse
	}
	return int(rsp.Length()), rsp.AttributeDataList(), nil
}

// Read requests the server to read the value of an attribute and return its
// value in a Read Response. [Vol 3, Part F, 3.4.4.3 & 3.4.4.4]
func (c *Client) Read(ctx context.Context, handle uint16) ([]byte, error) {
	b, err := c.roundTrip(ctx, ReadResponseCode, 1, func(txBuf []byte) []byte {
		req := ReadRequest(txBuf[:3])
		req.SetAttributeOpcode()
		req.SetAttributeHandle(handle)
		return req
	})
	if err != nil {
		return nil, err
	}
	return ReadResponse(b).AttributeValue(), nil
}

// ReadBlob requests the server to read part of the value of an attribute at a
// given offset and return a specific part of the value in a Read Blob Response.
// [Vol 3, Part F, 3.4.4.5 & 3.4.4.6]
func (c *Client) ReadBlob(ctx context.Context, handle, offset uint16) ([]byte, error) {
	b, err := c.roundTrip(ctx, ReadBlobResponseCode, 1, func(txBuf []byte) []byte {
		req := ReadBlobRequest(txBuf[:5])
		req.SetAttributeOpcode()
		req.SetAttributeHandle(handle)
		req.SetValueOffset(offset)
		return req
	})
	if err != nil {
		return nil, err
	}
	return ReadBlobResponse(b).PartAttributeValue(), nil
}

// ReadMultiple requests the server to read two or more values of a set of
// attributes and return their values in a Read Multiple Response.
// Only values that have a known fixed size can be read, with the exception of
// the last value that can have a variable length. The knowledge of whether
// attributes have a known fixed size is defined in a higher layer specification.
// [Vol 3, Part F, 3.4.4.7 & 3.4.4.8]
func (c *Client) ReadMultiple(ctx context.Context, handles []uint16) ([]byte, error) {
	// Should request to read two or more values.
	if len(handles) < 2 || len(handles)*2 > c.l2c.TxMTU()-1 {
		return nil, ErrInvalidArgument
	}

	b, err := c.roundTrip(ctx, ReadMultipleResponseCode, 1, func(txBuf []byte) []byte {
		req := ReadMultipleRequest(txBuf[:1+len(handles)*2])
		req.SetAttributeOpcode()
		p := req.SetOfHandles()
		for _, h := range handles {
			binary.LittleEndian.PutUint16(p, h)
			p = p[2:]
		}
		return req
	})
	if err != nil {
		return nil, err
	}
	return ReadMultipleResponse(b).SetOfValues(), nil
}

// ReadByGroupType obtains the values of attributes where the attribute type is known,
// the type of a grouping attribute as defined by a higher layer specification, but
// the handle is not known. [Vol 3, Part F, 3.4.4.9 & 3.4.4.10]
func (c *Client) ReadByGroupType(ctx context.Context, starth, endh uint16, uuid ble.UUID) (int, []byte, error) {
	if starth > endh || (len(uuid) != 2 && len(uuid) != 16) {
		return 0, nil, ErrInvalidArgument
	}

	b, err := c.roundTrip(ctx, ReadByGroupTypeResponseCode, 4, func(txBuf []byte) []byte {
		req := ReadByGroupTypeRequest(txBuf[:5+len(uuid)])
		req.SetAttributeOpcode()
		req.SetStartingHandle(starth)
		req.SetEndingHandle(endh)
		req.SetAttributeGroupType(uuid)
		return req
	})
	if err != nil {
		return 0, nil, err
	}

	// The data list must hold complete entries of the declared length. The
	// length byte is peer-controlled; reject zero before it divides.
	rsp := ReadByGroupTypeResponse(b)
	if rsp.Length() < 1 || len(rsp.AttributeDataList())%int(rsp.Length()) != 0 {
		return 0, nil, ErrInvalidResponse
	}
	return int(rsp.Length()), rsp.AttributeDataList(), nil
}

// Write requests the server to write the value of an attribute and acknowledge that
// this has been achieved in a Write Response. [Vol 3, Part F, 3.4.5.1 & 3.4.5.2]
func (c *Client) Write(ctx context.Context, handle uint16, value []byte) error {
	if len(value) > c.l2c.TxMTU()-3 {
		return ErrInvalidArgument
	}

	_, err := c.roundTrip(ctx, WriteResponseCode, 0, func(txBuf []byte) []byte {
		req := WriteRequest(txBuf[:3+len(value)])
		req.SetAttributeOpcode()
		req.SetAttributeHandle(handle)
		req.SetAttributeValue(value)
		return req
	})
	return err
}

// WriteCommand requests the server to write the value of an attribute, typically
// into a control-point attribute. [Vol 3, Part F, 3.4.5.3]
func (c *Client) WriteCommand(ctx context.Context, handle uint16, value []byte) error {
	if len(value) > c.l2c.TxMTU()-3 {
		return ErrInvalidArgument
	}

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := WriteCommand(txBuf[:3+len(value)])
	req.SetAttributeOpcode()
	req.SetAttributeHandle(handle)
	req.SetAttributeValue(value)

	return c.sendCmd(req)
}

// SignedWrite requests the server to write the value of an attribute with an authentication
// signature, typically into a control-point attribute. [Vol 3, Part F, 3.4.5.4]
func (c *Client) SignedWrite(ctx context.Context, handle uint16, value []byte, signature [12]byte) error {
	if len(value) > c.l2c.TxMTU()-15 {
		return ErrInvalidArgument
	}

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := SignedWriteCommand(txBuf[:15+len(value)])
	req.SetAttributeOpcode()
	req.SetAttributeHandle(handle)
	req.SetAttributeValue(value)
	req.SetAuthenticationSignature(signature)

	return c.sendCmd(req)
}

// PrepareWrite requests the server to prepare to write the value of an attribute.
// The server will respond to this request with a Prepare Write Response, so that
// the Client can verify that the value was received correctly.
// [Vol 3, Part F, 3.4.6.1 & 3.4.6.2]
func (c *Client) PrepareWrite(ctx context.Context, handle uint16, offset uint16, value []byte) (uint16, uint16, []byte, error) {
	if len(value) > c.l2c.TxMTU()-5 {
		return 0, 0, nil, ErrInvalidArgument
	}

	b, err := c.roundTrip(ctx, PrepareWriteResponseCode, 5, func(txBuf []byte) []byte {
		req := PrepareWriteRequest(txBuf[:5+len(value)])
		req.SetAttributeOpcode()
		req.SetAttributeHandle(handle)
		req.SetValueOffset(offset)
		req.SetPartAttributeValue(value)
		return req
	})
	if err != nil {
		return 0, 0, nil, err
	}
	rsp := PrepareWriteResponse(b)
	return rsp.AttributeHandle(), rsp.ValueOffset(), rsp.PartAttributeValue(), nil
}

// ExecuteWrite requests the server to write or cancel the write of all the prepared
// values currently held in the prepare queue from this Client. This request shall be
// handled by the server as an atomic operation. [Vol 3, Part F, 3.4.6.3 & 3.4.6.4]
func (c *Client) ExecuteWrite(ctx context.Context, flags uint8) error {
	_, err := c.roundTrip(ctx, ExecuteWriteResponseCode, 0, func(txBuf []byte) []byte {
		req := ExecuteWriteRequest(txBuf[:2])
		req.SetAttributeOpcode()
		req.SetFlags(flags)
		return req
	})
	return err
}

// acquireTxBuf obtains the shared transmit buffer, giving up when ctx is
// done. The buffer is returned only by the previous request method's
// deferred release; if that request is wedged (e.g. an in-flight ACL write
// on a dead link), an unbounded receive would park this caller too.
func (c *Client) acquireTxBuf(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.chTxBuf:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) sendCmd(b []byte) error {
	if c.bearerClosed.Load() {
		return fmt.Errorf("send ATT command failed: %w", ErrBearerClosed)
	}
	_, err := c.l2c.Write(b)
	return err
}

// seqProtoTimeout is the ATT sequential-protocol transaction timeout
// [Vol 3, Part F, 3.3.3]: the spec gives the server 30 seconds to respond.
var seqProtoTimeout = 30 * time.Second

// sendReq writes the request PDU and waits for its response. The wait is
// bounded by ctx and by the 30s ATT sequential-protocol timeout. The l2c
// write itself cannot be interrupted mid-write; on the linux stack it is
// independently bounded by hci.ACLWriteTimeout.
func (c *Client) sendReq(ctx context.Context, b []byte) (rsp []byte, err error) {
	// A previous transaction timed out and closed the bearer [Vol 3, Part F,
	// 3.3.3]. Fail fast: writing here could pair this request with the timed-
	// out transaction's late response — silently stale data.
	if c.bearerClosed.Load() {
		return nil, fmt.Errorf("ATT request refused: %w", ErrBearerClosed)
	}
	// A previously abandoned request may still own the bearer; resolve it
	// before writing, or its straggling response would be consumed as this
	// request's answer.
	if err := c.resolveAbandoned(ctx); err != nil {
		return nil, err
	}
	if logDebugEnabled() {
		ble.Logger.Debug("client req", "pdu", fmt.Sprintf("% X", b))
	}
	// Defense in depth: with no abandoned transaction outstanding, anything
	// sitting in rspc is a response PDU the peer sent unsolicited — drop it
	// rather than let it pair with this request. rspc is buffered (cap 1)
	// and Loop's send is non-blocking, so an unclaimed response sits in the
	// buffer rather than parking Loop — and there is never more than one,
	// because Loop drops (and counts) further responses while the buffer is
	// full. A single non-blocking receive therefore fully empties it.
	select {
	case stale := <-c.rspc:
		if logDebugEnabled() {
			ble.Logger.Debug("client: dropping unsolicited rsp", "pdu", fmt.Sprintf("% X", stale))
		}
	default:
	}
	if _, err := c.l2c.Write(b); err != nil {
		return nil, fmt.Errorf("send ATT request failed: %w", err)
	}
	// One absolute deadline for the whole transaction [Vol 3, Part F, 3.3.3]:
	// a per-iteration time.After would be reset by every unexpected PDU,
	// letting a chatty peer extend the timeout indefinitely.
	deadline := time.Now().Add(seqProtoTimeout)
	t := time.NewTimer(seqProtoTimeout)
	defer t.Stop()
	for {
		select {
		case rsp := <-c.rspc:
			if rsp[0] == ErrorResponseCode || rsp[0] == rspOfReq[b[0]] {
				return rsp, nil
			}
			// Sometimes when we connect to an Apple device, it sends
			// ATT requests asynchronously to us. // In this case, we
			// returns an ErrReqNotSupp response, and continue to wait
			// the response to our request.
			errRsp := newErrorResponse(rsp[0], 0x0000, ble.ErrReqNotSupp)
			if logDebugEnabled() {
				ble.Logger.Debug("client req", "pdu", fmt.Sprintf("% X", b))
			}
			_, err := c.l2c.Write(errRsp)
			if err != nil {
				return nil, fmt.Errorf("unexpected ATT response received: %w", err)
			}
		case err := <-c.chErr:
			return nil, fmt.Errorf("ATT request failed: %w", err)
		case <-ctx.Done():
			// Abandonment by the caller, not a protocol failure: the peer may
			// still answer, so the bearer stays usable. Deliberately NOT
			// poisoned — callers cancel requests during normal shutdown and
			// reconnect, where connection teardown follows immediately. But
			// the transaction is still outstanding on the wire: record what
			// it expects and when it expires, so the next request resolves
			// it (resolveAbandoned) instead of racing its late response.
			c.abandonedRsp = rspOfReq[b[0]]
			c.abandonedDeadline = deadline
			return nil, ctx.Err()
		case <-t.C:
			// The transaction timed out: per [Vol 3, Part F, 3.3.3] no
			// further ATT traffic is valid on this bearer — it "shall be
			// closed". Poison the client (subsequent requests fail fast with
			// ErrBearerClosed) and close the connection; Loop then exits via
			// the failing Read. Without this, a response straggling in after
			// the next same-opcode request was written would be silently
			// mis-attributed as that request's response.
			c.bearerClosed.Store(true)
			if err := c.l2c.Close(); err != nil {
				ble.Logger.Error("client: closing bearer after ATT timeout", "err", err)
			}
			return nil, fmt.Errorf("ATT request timeout: %w", ErrSeqProtoTimeout)
		}
	}
}

// resolveAbandoned settles a transaction whose request reached the wire but
// was abandoned by context cancellation before its response arrived. Called
// by sendReq (under the tx-buffer slot) before it writes a new request: the
// abandoned transaction still owns the bearer, and writing over it would let
// its straggling response — same opcode, or an Error Response — be consumed
// as the new request's answer.
//
// Outcomes: the late response arrives (dropped; the bearer is free again),
// the abandoned transaction's own absolute deadline passes (a transaction
// timeout — the bearer is poisoned and closed exactly as if the request had
// timed out live [Vol 3, Part F, 3.3.3]), or ctx/transport gives out first
// (the abandoned state persists for the next attempt).
func (c *Client) resolveAbandoned(ctx context.Context) error {
	if c.abandonedRsp == 0 {
		return nil
	}
	// A negative remaining duration fires the timer immediately.
	t := time.NewTimer(time.Until(c.abandonedDeadline))
	defer t.Stop()
	for {
		select {
		case rsp := <-c.rspc:
			if rsp[0] == ErrorResponseCode || rsp[0] == c.abandonedRsp {
				c.abandonedRsp = 0
				if logDebugEnabled() {
					ble.Logger.Debug("client: dropping late rsp of an abandoned request", "pdu", fmt.Sprintf("% X", rsp))
				}
				return nil
			}
			// Same peer quirk sendReq handles: some peers issue ATT requests
			// asynchronously. Refuse and keep waiting for the abandoned
			// transaction's response.
			errRsp := newErrorResponse(rsp[0], 0x0000, ble.ErrReqNotSupp)
			if _, err := c.l2c.Write(errRsp); err != nil {
				return fmt.Errorf("unexpected ATT response received: %w", err)
			}
		case err := <-c.chErr:
			return fmt.Errorf("ATT request failed: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.bearerClosed.Store(true)
			if err := c.l2c.Close(); err != nil {
				ble.Logger.Error("client: closing bearer after ATT timeout", "err", err)
			}
			return fmt.Errorf("ATT request timeout (abandoned request never answered): %w", ErrSeqProtoTimeout)
		}
	}
}

// RspDropped returns the number of response PDUs dropped because no request
// was waiting for them, i.e. their requests had been abandoned by context
// cancellation or the ATT sequential-protocol timeout.
func (c *Client) RspDropped() uint64 {
	return c.rspDropped.Load()
}

// pduPool recycles the buffers behind PDUs that are dispatched to the
// asyncWork consumer (notifications, indications, and incoming requests);
// the consumer puts them back once the handler has returned, which is what
// makes the NotificationHandler "valid only during the call" contract hold.
//
// Response PDUs are deliberately NOT pooled: sendReq hands them to the
// request methods, which return aliasing sub-slices (e.g. Read returns
// rsp.AttributeValue()) to callers with unbounded lifetime.
var pduPool = sync.Pool{New: func() any {
	b := make([]byte, ble.MaxMTU)
	return &b
}}

// Loop ...
func (c *Client) Loop() {

	type asyncWork struct {
		handle func([]byte)
		data   []byte
		buf    *[]byte // pooled backing buffer; returned after handle runs
	}

	ch := make(chan asyncWork, 16)
	defer close(ch)
	go func() {
		for w := range ch {
			w.handle(w.data)
			pduPool.Put(w.buf)
		}
	}()

	confirmation := []byte{HandleValueConfirmationCode}
	for {
		n, err := c.l2c.Read(c.rxBuf)
		if logDebugEnabled() {
			ble.Logger.Debug("client rsp", "pdu", fmt.Sprintf("% X", c.rxBuf[:n]))
		}
		if err != nil {
			// We don't expect any error from the bearer (L2CAP ACL-U)
			// Pass it along to the pending request, if any, and escape.
			c.chErr <- err
			return
		}
		if n == 0 {
			// A header-only L2CAP frame is delivered as (0, nil): there is
			// no ATT opcode, and c.rxBuf[0] is a stale byte from the
			// previous PDU. Classifying on it would ship an empty PDU as a
			// response (sendReq indexes rsp[0]) or as a notification (the
			// gatt dispatcher parses a handle from it) — both panic.
			ble.Logger.Warn("client: dropping zero-length ATT PDU")
			continue
		}

		op := c.rxBuf[0]

		if (op == HandleValueNotificationCode || op == HandleValueIndicationCode) && n < 3 {
			// Opcode + 2-byte attribute handle is the spec minimum
			// [Vol 3, Part F, 3.4.7.1-2]; anything shorter would panic the
			// handle parse downstream. Still acknowledge a runt indication
			// so the peer's sequential protocol isn't left waiting on us.
			ble.Logger.Warn("client: dropping runt notification/indication", "len", n)
			if op == HandleValueIndicationCode {
				_, _ = c.l2c.Write(confirmation)
			}
			continue
		}

		if (op != ExchangeMTURequestCode) && (op != HandleValueNotificationCode) && (op != HandleValueIndicationCode) {
			// Response PDU: it escapes to the sendReq caller and, aliased,
			// to the library user — it needs its own allocation.
			b := make([]byte, n)
			copy(b, c.rxBuf)
			// Non-blocking: if the buffer already holds an unclaimed
			// response (its request was abandoned by ctx cancellation or
			// the ATT timeout), this one is stale too — drop it rather
			// than park the read loop, which would wedge the whole
			// inbound HCI path behind an ATT PDU nobody will receive.
			select {
			case c.rspc <- b:
			default:
				c.rspDropped.Add(1)
				if logDebugEnabled() {
					ble.Logger.Debug("client: dropping unclaimed rsp", "pdu", fmt.Sprintf("% X", b))
				}
			}
			continue
		}

		// Copy into a pooled buffer; c.rxBuf is len(ble.MaxMTU), so n always
		// fits. Do not touch b after a successful enqueue — the consumer may
		// already have released it for reuse.
		buf := pduPool.Get().(*[]byte)
		b := (*buf)[:n]
		copy(b, c.rxBuf)

		// TODO: better request identification
		if op == ExchangeMTURequestCode {
			// Schedule this to be taken care of
			select {
			case ch <- asyncWork{handle: c.handleRequest, data: b, buf: buf}:
			default:
				pduPool.Put(buf)
				// If this really happens, especially on a slow machine, enlarge the channel buffer.
				ble.Logger.Error("client: can't enqueue incoming request")
			}
			continue
		}

		// Deliver the full request to upper layer.
		select {
		case ch <- asyncWork{handle: c.handler.HandleNotification, data: b, buf: buf}:
		default:
			pduPool.Put(buf)
			// If this really happens, especially on a slow machine, enlarge the channel buffer.
			ble.Logger.Error("client: can't enqueue incoming notification")
		}

		// Always write aknowledgement for an indication, even it was an invalid request.
		if op == HandleValueIndicationCode {
			if logDebugEnabled() {
				ble.Logger.Debug("client req", "pdu", fmt.Sprintf("% X", c.rxBuf[:n]))
			}
			_, _ = c.l2c.Write(confirmation)
		}
	}
}

func (c *Client) handleRequest(b []byte) {
	switch b[0] {
	case ExchangeMTURequestCode:
		resp := c.handleExchangeMTURequest(b)
		if len(resp) != 0 {
			err := c.sendCmd(resp)
			if err != nil {
				ble.Logger.Error("client: error sending MTU response", "err", err)
			}
		}
	default:
		errRsp := newErrorResponse(b[0], 0x0000, ble.ErrReqNotSupp)
		_ = c.sendCmd(errRsp)
		// Abnormal path; eager formatting is acceptable here.
		ble.Logger.Warn("client: received unhandled request", "pdu", fmt.Sprintf("[0x%X]", b))
	}
}

// handle MTU Exchange request. [Vol 3, Part F, 3.4.2]
// ExchangeMTU informs the server of the client’s maximum receive MTU size and
// request the server to respond with its maximum receive MTU size. [Vol 3, Part F, 3.4.2.1]
func (c *Client) handleExchangeMTURequest(r ExchangeMTURequest) []byte {
	// Acquire and reuse the txBuf, and release it after usage.
	// The same txBuf, or a newly allocate one, if the txMTU is changed,
	// will be released back to the channel.

	// We do this first to prevent races with ExchangeMTURequest
	txBuf := <-c.chTxBuf

	// The effective transmit MTU after this exchange. Until the request is
	// validated it stays at the current value, so the deferred release
	// restores the buffer unchanged on the error paths.
	txMTU := len(txBuf)

	// Release the buffer unconditionally, registered BEFORE any validation:
	// an early return on a malformed request must not leave chTxBuf drained
	// for good, which would deadlock every later request on this connection.
	defer func() {
		// Update the tx buffer if needed
		if len(txBuf) != txMTU {
			c.chTxBuf <- make([]byte, txMTU)
		} else {
			c.chTxBuf <- txBuf
		}
	}()

	// Validate the request.
	switch {
	case len(r) != 3:
		fallthrough
	case r.ClientRxMTU() < ble.DefaultMTU:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	txMTU = int(r.ClientRxMTU())
	// Don't trust the peer's advertised MTU past our own limit: rxBuf and
	// the tx buffer are sized for at most ble.MaxMTU.
	if txMTU > ble.MaxMTU {
		txMTU = ble.MaxMTU
	}
	// Our rxMTU for the response
	rxMTU := c.l2c.RxMTU()

	// Update transmit MTU to max supported by the other side
	ble.Logger.Debug("client: server requested an MTU change", "tx", txMTU, "rx", rxMTU)
	c.l2c.SetTxMTU(txMTU)

	// Build the response in its own slice, never in txBuf: the deferred
	// release can hand txBuf to a concurrent request before the caller
	// writes this response to the wire.
	rsp := ExchangeMTUResponse(make([]byte, 3))
	rsp.SetAttributeOpcode()
	rsp.SetServerRxMTU(uint16(rxMTU))
	return rsp
}
