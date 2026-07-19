package att

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/go-ble/ble"
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
	l2c  ble.Conn
	rspc chan []byte

	rxBuf   []byte
	chTxBuf chan []byte
	chErr   chan error
	handler NotificationHandler
}

// NewClient returns an Attribute Protocol Client.
func NewClient(l2c ble.Conn, h NotificationHandler) *Client {
	c := &Client{
		l2c:     l2c,
		rspc:    make(chan []byte),
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

	// Let L2CAP know the MTU we can handle.
	c.l2c.SetRxMTU(clientRxMTU)

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

	txMTU := int(rsp.ServerRxMTU())
	if len(txBuf) != txMTU {
		// Let L2CAP know the MTU that the remote device can handle.
		c.l2c.SetTxMTU(txMTU)
		// Put a re-allocated txBuf back to the channel.
		// The txBuf has been captured in deferred function.
		txBuf = make([]byte, txMTU, txMTU)
	}

	return txMTU, nil
}

// FindInformation obtains the mapping of attribute handles with their associated types.
// This allows a Client to discover the list of attributes and their types on a server.
// [Vol 3, Part F, 3.4.3.1 & 3.4.3.2]
func (c *Client) FindInformation(ctx context.Context, starth, endh uint16) (fmt int, data []byte, err error) {
	if starth == 0 || starth > endh {
		return 0x00, nil, ErrInvalidArgument
	}

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return 0x00, nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := FindInformationRequest(txBuf[:5])
	req.SetAttributeOpcode()
	req.SetStartingHandle(starth)
	req.SetEndingHandle(endh)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return 0x00, nil, err
	}

	// Convert and validate the response.
	rsp := FindInformationResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return 0x00, nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 6:
		fallthrough
	case rsp.Format() == 0x01 && ((len(rsp)-2)%4) != 0:
		fallthrough
	case rsp.Format() == 0x02 && ((len(rsp)-2)%18) != 0:
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

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ReadByTypeRequest(txBuf[:5+len(uuid)])
	req.SetAttributeOpcode()
	req.SetStartingHandle(starth)
	req.SetEndingHandle(endh)
	req.SetAttributeType(uuid)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return 0, nil, err
	}

	// Convert and validate the response.
	rsp := ReadByTypeResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return 0, nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 4 || len(rsp.AttributeDataList())%int(rsp.Length()) != 0:
		return 0, nil, ErrInvalidResponse
	}
	return int(rsp.Length()), rsp.AttributeDataList(), nil
}

// Read requests the server to read the value of an attribute and return its
// value in a Read Response. [Vol 3, Part F, 3.4.4.3 & 3.4.4.4]
func (c *Client) Read(ctx context.Context, handle uint16) ([]byte, error) {

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ReadRequest(txBuf[:3])
	req.SetAttributeOpcode()
	req.SetAttributeHandle(handle)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return nil, err
	}

	// Convert and validate the response.
	rsp := ReadResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 1:
		return nil, ErrInvalidResponse
	}
	return rsp.AttributeValue(), nil
}

// ReadBlob requests the server to read part of the value of an attribute at a
// given offset and return a specific part of the value in a Read Blob Response.
// [Vol 3, Part F, 3.4.4.5 & 3.4.4.6]
func (c *Client) ReadBlob(ctx context.Context, handle, offset uint16) ([]byte, error) {

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ReadBlobRequest(txBuf[:5])
	req.SetAttributeOpcode()
	req.SetAttributeHandle(handle)
	req.SetValueOffset(offset)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return nil, err
	}

	// Convert and validate the response.
	rsp := ReadBlobResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 1:
		return nil, ErrInvalidResponse
	}
	return rsp.PartAttributeValue(), nil
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

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ReadMultipleRequest(txBuf[:1+len(handles)*2])
	req.SetAttributeOpcode()
	p := req.SetOfHandles()
	for _, h := range handles {
		binary.LittleEndian.PutUint16(p, h)
		p = p[2:]
	}

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return nil, err
	}

	// Convert and validate the response.
	rsp := ReadMultipleResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 1:
		return nil, ErrInvalidResponse
	}
	return rsp.SetOfValues(), nil
}

// ReadByGroupType obtains the values of attributes where the attribute type is known,
// the type of a grouping attribute as defined by a higher layer specification, but
// the handle is not known. [Vol 3, Part F, 3.4.4.9 & 3.4.4.10]
func (c *Client) ReadByGroupType(ctx context.Context, starth, endh uint16, uuid ble.UUID) (int, []byte, error) {
	if starth > endh || (len(uuid) != 2 && len(uuid) != 16) {
		return 0, nil, ErrInvalidArgument
	}

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ReadByGroupTypeRequest(txBuf[:5+len(uuid)])
	req.SetAttributeOpcode()
	req.SetStartingHandle(starth)
	req.SetEndingHandle(endh)
	req.SetAttributeGroupType(uuid)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return 0, nil, err
	}

	// Convert and validate the response.
	rsp := ReadByGroupTypeResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return 0, nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 4:
		fallthrough
	case len(rsp.AttributeDataList())%int(rsp.Length()) != 0:
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

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := WriteRequest(txBuf[:3+len(value)])
	req.SetAttributeOpcode()
	req.SetAttributeHandle(handle)
	req.SetAttributeValue(value)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return err
	}

	// Convert and validate the response.
	rsp := WriteResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		return ErrInvalidResponse
	}
	return nil
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

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := PrepareWriteRequest(txBuf[:5+len(value)])
	req.SetAttributeOpcode()
	req.SetAttributeHandle(handle)
	req.SetValueOffset(offset)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return 0, 0, nil, err
	}

	// Convert and validate the response.
	rsp := PrepareWriteResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return 0, 0, nil, ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		fallthrough
	case len(rsp) < 5:
		return 0, 0, nil, ErrInvalidResponse
	}
	return rsp.AttributeHandle(), rsp.ValueOffset(), rsp.PartAttributeValue(), nil
}

// ExecuteWrite requests the server to write or cancel the write of all the prepared
// values currently held in the prepare queue from this Client. This request shall be
// handled by the server as an atomic operation. [Vol 3, Part F, 3.4.6.3 & 3.4.6.4]
func (c *Client) ExecuteWrite(ctx context.Context, flags uint8) error {

	// Acquire and reuse the txBuf, and release it after usage.
	txBuf, err := c.acquireTxBuf(ctx)
	if err != nil {
		return err
	}
	defer func() { c.chTxBuf <- txBuf }()

	req := ExecuteWriteRequest(txBuf[:1])
	req.SetAttributeOpcode()
	req.SetFlags(flags)

	b, err := c.sendReq(ctx, req)
	if err != nil {
		return err
	}

	// Convert and validate the response.
	rsp := ExecuteWriteResponse(b)
	switch {
	case rsp[0] == ErrorResponseCode && len(rsp) == 5:
		return ble.ATTError(rsp[4])
	case rsp[0] == ErrorResponseCode && len(rsp) != 5:
		fallthrough
	case rsp[0] != rsp.AttributeOpcode():
		return ErrInvalidResponse
	}
	return nil
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
	_, err := c.l2c.Write(b)
	return err
}

// sendReq writes the request PDU and waits for its response. The wait is
// bounded by ctx and by the 30s ATT sequential-protocol timeout. The l2c
// write itself cannot be interrupted mid-write; on the linux stack it is
// independently bounded by hci.ACLWriteTimeout.
func (c *Client) sendReq(ctx context.Context, b []byte) (rsp []byte, err error) {
	if logDebugEnabled() {
		ble.Logger.Debug("client req", "pdu", fmt.Sprintf("% X", b))
	}
	// Drain the response of an earlier abandoned request (ctx cancellation
	// or ATT timeout), if one straggled in since: it would otherwise be
	// mistaken for the response to this request, or park the read loop on
	// the unbuffered rspc send.
	select {
	case stale := <-c.rspc:
		if logDebugEnabled() {
			ble.Logger.Debug("client: dropping stale rsp of an abandoned request", "pdu", fmt.Sprintf("% X", stale))
		}
	default:
	}
	if _, err := c.l2c.Write(b); err != nil {
		return nil, fmt.Errorf("send ATT request failed: %w", err)
	}
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
			return nil, ctx.Err()
		case <-time.After(30 * time.Second):
			return nil, fmt.Errorf("ATT request timeout: %w", ErrSeqProtoTimeout)
		}
	}
}

// pduPool recycles the buffers behind PDUs that are dispatched to the
// asyncWork consumer (notifications, indications, and incoming requests);
// the consumer puts them back once the handler has returned, which is what
// makes the NotificationHandler "valid only during the call" contract hold.
//
// Response PDUs are deliberately NOT pooled: sendReq hands them to the
// request methods, which return aliasing sub-slices (e.g. Read returns
// rsp.AttributeValue()) to callers with unbounded lifetime.
var pduPool = sync.Pool{New: func() interface{} {
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

		op := c.rxBuf[0]

		if (op != ExchangeMTURequestCode) && (op != HandleValueNotificationCode) && (op != HandleValueIndicationCode) {
			// Response PDU: it escapes to the sendReq caller and, aliased,
			// to the library user — it needs its own allocation.
			b := make([]byte, n)
			copy(b, c.rxBuf)
			c.rspc <- b
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

	// Validate the request.
	switch {
	case len(r) != 3:
		fallthrough
	case r.ClientRxMTU() < 23:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	txMTU := int(r.ClientRxMTU())
	// Our rxMTU for the response
	rxMTU := c.l2c.RxMTU()

	// Update transmit MTU to max supported by the other side
	ble.Logger.Debug("client: server requested an MTU change", "tx", txMTU, "rx", rxMTU)
	c.l2c.SetTxMTU(txMTU)

	defer func() {
		// Update the tx buffer if needed
		if len(txBuf) != txMTU {
			c.chTxBuf <- make([]byte, txMTU, txMTU)
		} else {
			c.chTxBuf <- txBuf
		}
	}()

	rsp := ExchangeMTUResponse(txBuf)
	rsp.SetAttributeOpcode()
	rsp.SetServerRxMTU(uint16(rxMTU))
	return rsp[:3]
}
