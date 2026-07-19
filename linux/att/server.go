package att

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/go-ble/ble"
)

type conn struct {
	ble.Conn
	svr  *Server
	cccs map[uint16]uint16
	nn   map[uint16]ble.Notifier
	in   map[uint16]ble.Notifier
}

// Server implements an ATT (Attribute Protocol) server.
type Server struct {
	conn *conn
	db   *DB

	// Refer to [Vol 3, Part F, 3.3.2 & 3.3.3] for the requirement of
	// sequential request-response protocol, and transactions.
	rxMTU     int
	txBuf     []byte
	chNotBuf  chan []byte
	chIndBuf  chan []byte
	chConfirm chan bool

	dummyRspWriter ble.ResponseWriter

	// Store a write handler for defer execute once receiving ExecuteWriteRequest
	prepareWriteRequestAttr *attr
	prepareWriteRequestData bytes.Buffer
}

// NewServer returns an ATT (Attribute Protocol) server.
func NewServer(db *DB, l2c ble.Conn) (*Server, error) {
	mtu := l2c.RxMTU()
	if mtu < ble.DefaultMTU || mtu > ble.MaxMTU {
		return nil, fmt.Errorf("MTU %d out of range [%d, %d]: %w", mtu, ble.DefaultMTU, ble.MaxMTU, ErrInvalidMTU)
	}
	// Although the rxBuf is initialized with the capacity of rxMTU, it is
	// not discovered, and only the default ATT_MTU (23 bytes) of it shall
	// be used until remote central request ExchangeMTU.
	s := &Server{
		conn: &conn{
			Conn: l2c,
			cccs: make(map[uint16]uint16),
			in:   make(map[uint16]ble.Notifier),
			nn:   make(map[uint16]ble.Notifier),
		},
		db: db,

		rxMTU:     mtu,
		txBuf:     make([]byte, ble.DefaultMTU, ble.DefaultMTU),
		chNotBuf:  make(chan []byte, 1),
		chIndBuf:  make(chan []byte, 1),
		chConfirm: make(chan bool),

		dummyRspWriter: ble.NewResponseWriter(nil),
	}
	s.conn.svr = s
	s.chNotBuf <- make([]byte, ble.DefaultMTU, ble.DefaultMTU)
	s.chIndBuf <- make([]byte, ble.DefaultMTU, ble.DefaultMTU)
	return s, nil
}

// notify sends notification to remote central. The value is truncated to
// what the negotiated MTU allows. [Vol 3, Part F, 3.4.7.1]
func (s *Server) notify(h uint16, data []byte) (int, error) {
	// Acquire and reuse notifyBuffer. Release it after usage.
	nBuf := <-s.chNotBuf
	defer func() { s.chNotBuf <- nBuf }()

	rsp := HandleValueNotification(nBuf)
	rsp.SetAttributeOpcode()
	rsp.SetAttributeHandle(h)
	n := copy(rsp.AttributeValue(), data)
	return s.conn.Write(rsp[:3+n])
}

// indicate sends indication to remote central and waits for the confirmation
// (bounded by the ATT sequential-protocol timeout [Vol 3, Part F, 3.3.3]).
// The value is truncated to what the negotiated MTU allows.
func (s *Server) indicate(h uint16, data []byte) (int, error) {
	// Acquire and reuse indicateBuffer. Release it after usage.
	iBuf := <-s.chIndBuf
	defer func() { s.chIndBuf <- iBuf }()

	rsp := HandleValueIndication(iBuf)
	rsp.SetAttributeOpcode()
	rsp.SetAttributeHandle(h)
	m := copy(rsp.AttributeValue(), data)
	n, err := s.conn.Write(rsp[:3+m])
	if err != nil {
		return n, err
	}
	select {
	case _, ok := <-s.chConfirm:
		if !ok {
			return 0, io.ErrClosedPipe
		}
		return n, nil
	case <-time.After(seqProtoTimeout):
		return 0, ErrSeqProtoTimeout
	}
}

// Loop accepts incoming ATT request, and respond response.
func (s *Server) Loop() {
	type sbuf struct {
		buf []byte
		len int
	}
	pool := make(chan *sbuf, 2)
	pool <- &sbuf{buf: make([]byte, s.rxMTU)}
	pool <- &sbuf{buf: make([]byte, s.rxMTU)}

	seq := make(chan *sbuf)
	go func() {
		b := <-pool
		for {
			n, err := s.conn.Read(b.buf)
			if n == 0 || err != nil {
				close(seq)
				close(s.chConfirm)
				_ = s.conn.Close()
				return
			}
			if b.buf[0] == HandleValueConfirmationCode {
				select {
				case s.chConfirm <- true:
				default:
					ble.Logger.Error("server: received a spurious confirmation")
				}
				continue
			}
			b.len = n
			seq <- b   // Send the current request for handling
			b = <-pool // Swap the buffer for next incoming request.
		}
	}()
	for req := range seq {
		if rsp := s.handleRequest(req.buf[:req.len]); len(rsp) != 0 {
			if _, err := s.conn.Write(rsp); err != nil {
				// The bearer is dying; the reader goroutine will observe the
				// same failure and shut the loop down.
				ble.Logger.Error("server: failed to write response", "err", err)
			}
		}
		pool <- req
	}
	for h, ccc := range s.conn.cccs {
		if ccc != 0 {
			ble.Logger.Info("server cleanup", "ccc", fmt.Sprintf("0x%02X", ccc))
		}
		if ccc&cccIndicate != 0 {
			s.conn.in[h].Close()
		}
		if ccc&cccNotify != 0 {
			s.conn.nn[h].Close()
		}
	}
}

func (s *Server) handleRequest(b []byte) []byte {
	var resp []byte
	if logDebugEnabled() {
		ble.Logger.Debug("server req", "pdu", fmt.Sprintf("% X", b))
	}
	switch reqType := b[0]; reqType {
	case ExchangeMTURequestCode:
		resp = s.handleExchangeMTURequest(b)
	case FindInformationRequestCode:
		resp = s.handleFindInformationRequest(b)
	case FindByTypeValueRequestCode:
		resp = s.handleFindByTypeValueRequest(b)
	case ReadByTypeRequestCode:
		resp = s.handleReadByTypeRequest(b)
	case ReadRequestCode:
		resp = s.handleReadRequest(b)
	case ReadBlobRequestCode:
		resp = s.handleReadBlobRequest(b)
	case ReadByGroupTypeRequestCode:
		resp = s.handleReadByGroupRequest(b)
	case WriteRequestCode:
		resp = s.handleWriteRequest(b)
	case WriteCommandCode:
		s.handleWriteCommand(b)
	case PrepareWriteRequestCode:
		resp = s.handlePrepareWriteRequest(b)
	case ExecuteWriteRequestCode:
		resp = s.handleExecuteWriteRequest(b)
	case ReadMultipleRequestCode,
		SignedWriteCommandCode:
		fallthrough
	default:
		resp = newErrorResponse(reqType, 0x0000, ble.ErrReqNotSupp)
	}
	if logDebugEnabled() {
		ble.Logger.Debug("server rsp", "pdu", fmt.Sprintf("% X", resp))
	}
	return resp
}

// handle MTU Exchange request. [Vol 3, Part F, 3.4.2]
func (s *Server) handleExchangeMTURequest(r ExchangeMTURequest) []byte {
	// Validate the request.
	switch {
	case len(r) != 3:
		fallthrough
	case r.ClientRxMTU() < 23:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	txMTU := int(r.ClientRxMTU())
	// Don't trust the peer's advertised MTU past our own limit: the tx,
	// notification, and indication buffers are sized for at most ble.MaxMTU
	// (mirrors the client-side clamp in handleExchangeMTURequest).
	if txMTU > ble.MaxMTU {
		txMTU = ble.MaxMTU
	}
	s.conn.SetTxMTU(txMTU)

	if txMTU != len(s.txBuf) {
		// Apply the txMTU afer this response has been sent and before
		// any other attribute protocol PDU is sent.
		defer func() {
			s.txBuf = make([]byte, txMTU)
			<-s.chNotBuf
			s.chNotBuf <- make([]byte, txMTU)
			<-s.chIndBuf
			s.chIndBuf <- make([]byte, txMTU)
		}()
	}

	rsp := ExchangeMTUResponse(s.txBuf)
	rsp.SetAttributeOpcode()
	rsp.SetServerRxMTU(uint16(s.rxMTU))
	return rsp[:3]
}

// handle Find Information request. [Vol 3, Part F, 3.4.3.1 & 3.4.3.2]
func (s *Server) handleFindInformationRequest(r FindInformationRequest) []byte {
	// Validate the request.
	switch {
	case len(r) != 5:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	case r.StartingHandle() == 0 || r.StartingHandle() > r.EndingHandle():
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrInvalidHandle)
	}

	rsp := FindInformationResponse(s.txBuf)
	rsp.SetAttributeOpcode()
	rsp.SetFormat(0x00)
	data := rsp.InformationData()
	n := 0

	// Each response shall contain Types of the same format.
	for _, a := range s.db.subrange(r.StartingHandle(), r.EndingHandle()) {
		if rsp.Format() == 0 {
			rsp.SetFormat(0x01)
			if a.typ.Len() == 16 {
				rsp.SetFormat(0x02)
			}
		}
		if rsp.Format() == 0x01 && a.typ.Len() != 2 {
			break
		}
		if rsp.Format() == 0x02 && a.typ.Len() != 16 {
			break
		}

		if n+2+a.typ.Len() > len(data) {
			break
		}
		binary.LittleEndian.PutUint16(data[n:], a.h)
		n += 2
		n += copy(data[n:], a.typ)
	}

	// Nothing has been found.
	if rsp.Format() == 0 {
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrAttrNotFound)
	}
	return rsp[:2+n]
}

// handle Find By Type Value request. [Vol 3, Part F, 3.4.3.3 & 3.4.3.4]
func (s *Server) handleFindByTypeValueRequest(r FindByTypeValueRequest) []byte {
	// Validate the request.
	switch {
	case len(r) < 7:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	case r.StartingHandle() == 0 || r.StartingHandle() > r.EndingHandle():
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrInvalidHandle)
	}

	rsp := FindByTypeValueResponse(s.txBuf)
	rsp.SetAttributeOpcode()
	list := rsp.HandleInformationList()
	n := 0

	for _, a := range s.db.subrange(r.StartingHandle(), r.EndingHandle()) {
		v, starth, endh := a.v, a.h, a.endh
		if !a.typ.Equal(ble.UUID16(r.AttributeType())) {
			continue
		}
		if v == nil {
			// The value shall not exceed ATT_MTU - 7 bytes.
			// Since ResponseWriter caps the value at the capacity,
			// we allocate one extra byte, and the written length.
			buf2 := bytes.NewBuffer(make([]byte, 0, len(s.txBuf)-7+1))
			e := handleATT(a, s, r, ble.NewResponseWriter(buf2))
			if e != ble.ErrSuccess || buf2.Len() > len(s.txBuf)-7 {
				return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrInvalidHandle)
			}
			// BUG FIX: the handler-produced value was never adopted, so
			// dynamic attributes could not match the requested value.
			v = buf2.Bytes()
			endh = a.h
		}
		if !(ble.UUID(v).Equal(ble.UUID(r.AttributeValue()))) {
			continue
		}

		if n+4 > len(list) {
			break
		}
		binary.LittleEndian.PutUint16(list[n:], starth)
		binary.LittleEndian.PutUint16(list[n+2:], endh)
		n += 4
	}
	if n == 0 {
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrAttrNotFound)
	}

	return rsp[:1+n]
}

// handle Read By Type request. [Vol 3, Part F, 3.4.4.1 & 3.4.4.2]
func (s *Server) handleReadByTypeRequest(r ReadByTypeRequest) []byte {
	// Validate the request.
	switch {
	case len(r) != 7 && len(r) != 21:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	case r.StartingHandle() == 0 || r.StartingHandle() > r.EndingHandle():
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrInvalidHandle)
	}

	rsp := ReadByTypeResponse(s.txBuf)
	rsp.SetAttributeOpcode()
	list := rsp.AttributeDataList()
	n := 0

	// handle length (2 bytes) + value length.
	// Each response shall only contains values with the same size.
	dlen := 0
	for _, a := range s.db.subrange(r.StartingHandle(), r.EndingHandle()) {
		if !a.typ.Equal(ble.UUID(r.AttributeType())) {
			continue
		}
		v := a.v
		if v == nil {
			buf2 := bytes.NewBuffer(make([]byte, 0, len(s.txBuf)-2))
			if e := handleATT(a, s, r, ble.NewResponseWriter(buf2)); e != ble.ErrSuccess {
				// Return if the first value read cause an error.
				if dlen == 0 {
					return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), e)
				}
				// Otherwise, skip to the next one.
				break
			}
			v = buf2.Bytes()
		}
		if dlen == 0 {
			// Found the first value.
			dlen = 2 + len(v)
			if dlen > 255 {
				dlen = 255
			}
			if dlen > len(list) {
				dlen = len(list)
			}
			rsp.SetLength(uint8(dlen))
		} else if 2+len(v) != dlen {
			break
		}

		if n+dlen > len(list) {
			break
		}
		binary.LittleEndian.PutUint16(list[n:], a.h)
		copy(list[n+2:], v[:dlen-2])
		n += dlen
	}
	if dlen == 0 {
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrAttrNotFound)
	}
	return rsp[:2+n]
}

// handle Read request. [Vol 3, Part F, 3.4.4.3 & 3.4.4.4]
func (s *Server) handleReadRequest(r ReadRequest) []byte {
	// Validate the request.
	switch {
	case len(r) != 3:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	rsp := ReadResponse(s.txBuf)
	rsp.SetAttributeOpcode()

	a, ok := s.db.at(r.AttributeHandle())
	if !ok {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrInvalidHandle)
	}

	// Simple case. Read-only, no-authorization, no-authentication.
	// The value is truncated to ATT_MTU-1 bytes [Vol 3, Part F, 3.4.4.4];
	// the client reads the remainder with Read Blob. (The old bytes.Buffer
	// grew silently past the tx buffer and panicked on the final slice.)
	if a.v != nil {
		n := copy(rsp.AttributeValue(), a.v)
		return rsp[:1+n]
	}

	// Pass the request to upper layer with the ResponseWriter, which caps
	// the buffer to a valid length of payload.
	buf := bytes.NewBuffer(rsp.AttributeValue())
	buf.Reset()
	if e := handleATT(a, s, r, ble.NewResponseWriter(buf)); e != ble.ErrSuccess {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), e)
	}
	return rsp[:1+buf.Len()]
}

// handle Read Blob request. [Vol 3, Part F, 3.4.4.5 & 3.4.4.6]
func (s *Server) handleReadBlobRequest(r ReadBlobRequest) []byte {
	// Validate the request.
	switch {
	case len(r) != 5:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	a, ok := s.db.at(r.AttributeHandle())
	if !ok {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrInvalidHandle)
	}

	rsp := ReadBlobResponse(s.txBuf)
	rsp.SetAttributeOpcode()

	// Simple case. Read-only, no-authorization, no-authentication.
	// Serve the part starting at the requested offset, truncated to
	// ATT_MTU-1 bytes [Vol 3, Part F, 3.4.4.5]. (The old code ignored the
	// offset and re-sent the value from the start, and its bytes.Buffer
	// grew silently past the tx buffer and panicked on the final slice.)
	if a.v != nil {
		off := int(r.ValueOffset())
		if off > len(a.v) {
			return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrInvalidOffset)
		}
		n := copy(rsp.PartAttributeValue(), a.v[off:])
		return rsp[:1+n]
	}

	// Pass the request to upper layer with the ResponseWriter, which caps
	// the buffer to a valid length of payload.
	buf := bytes.NewBuffer(rsp.PartAttributeValue())
	buf.Reset()
	if e := handleATT(a, s, r, ble.NewResponseWriter(buf)); e != ble.ErrSuccess {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), e)
	}
	return rsp[:1+buf.Len()]
}

// handle Read Blob request. [Vol 3, Part F, 3.4.4.9 & 3.4.4.10]
func (s *Server) handleReadByGroupRequest(r ReadByGroupTypeRequest) []byte {
	// Validate the request.
	switch {
	case len(r) != 7 && len(r) != 21:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	case r.StartingHandle() == 0 || r.StartingHandle() > r.EndingHandle():
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrInvalidHandle)
	}

	rsp := ReadByGroupTypeResponse(s.txBuf)
	rsp.SetAttributeOpcode()
	list := rsp.AttributeDataList()
	n := 0

	dlen := 0
	for _, a := range s.db.subrange(r.StartingHandle(), r.EndingHandle()) {
		// BUG FIX: only grouping attributes of the requested type belong in
		// the response [Vol 3, Part F, 3.4.4.10]; the old code returned
		// every attribute in the handle range regardless of its type.
		if !a.typ.Equal(ble.UUID(r.AttributeGroupType())) {
			continue
		}
		v := a.v
		if v == nil {
			free := len(list) - n - 4
			if free < 0 {
				break
			}
			// BUG FIX: the scratch buffer was allocated with a non-zero
			// length, so the handler's output landed after that many
			// zero bytes and the response carried zero padding as data.
			buf2 := bytes.NewBuffer(make([]byte, 0, free))
			if e := handleATT(a, s, r, ble.NewResponseWriter(buf2)); e != ble.ErrSuccess {
				return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), e)
			}
			v = buf2.Bytes()
		}
		if dlen == 0 {
			dlen = min(4+len(v), 255, len(list))
			rsp.SetLength(uint8(dlen))
		} else if 4+len(v) != dlen {
			break
		}

		if n+dlen > len(list) {
			break
		}
		binary.LittleEndian.PutUint16(list[n:], a.h)
		binary.LittleEndian.PutUint16(list[n+2:], a.endh)
		copy(list[n+4:], v[:dlen-4])
		n += dlen
	}
	if dlen == 0 {
		return newErrorResponse(r.AttributeOpcode(), r.StartingHandle(), ble.ErrAttrNotFound)
	}
	return rsp[:2+n]
}

// handle Write request. [Vol 3, Part F, 3.4.5.1 & 3.4.5.2]
func (s *Server) handleWriteRequest(r WriteRequest) []byte {
	// Validate the request.
	switch {
	case len(r) < 3:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	a, ok := s.db.at(r.AttributeHandle())
	if !ok {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrInvalidHandle)
	}

	// We don't support write to static value. Pass the request to upper layer.
	if a == nil {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrWriteNotPerm)
	}
	if e := handleATT(a, s, r, ble.NewResponseWriter(nil)); e != ble.ErrSuccess {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), e)
	}
	return []byte{WriteResponseCode}
}

func (s *Server) handlePrepareWriteRequest(r PrepareWriteRequest) []byte {
	// Validate the request BEFORE touching any field: the fixed header is
	// opcode + handle + offset = 5 bytes [Vol 3, Part F, 3.4.6.1]. The old
	// < 3 check let a truncated PDU reach PartAttributeValue's r[5:] slice
	// and panic the server.
	switch {
	case len(r) < 5:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}
	if logDebugEnabled() {
		ble.Logger.Debug("handlePrepareWriteRequest", "handle", r.AttributeHandle())
	}

	a, ok := s.db.at(r.AttributeHandle())
	if !ok {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrInvalidHandle)
	}

	// We don't support write to static value. Pass the request to upper layer.
	if a == nil {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), ble.ErrWriteNotPerm)
	}

	if e := handleATT(a, s, r, ble.NewResponseWriter(nil)); e != ble.ErrSuccess {
		return newErrorResponse(r.AttributeOpcode(), r.AttributeHandle(), e)
	}

	// Convert and validate the response.
	rsp := PrepareWriteResponse(r)
	rsp.SetAttributeOpcode()
	return rsp
}

func (s *Server) handleExecuteWriteRequest(r ExecuteWriteRequest) []byte {
	// Validate the request.
	switch {
	case len(r) < 2:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	switch r.Flags() {
	case 0:
		// 0x00 – Cancel all prepared writes
		s.prepareWriteRequestAttr = nil
		s.prepareWriteRequestData.Reset()
	case 1:
		// 0x01 – Immediately write all pending prepared values.
		// An empty queue executes trivially [Vol 3, Part F, 3.4.6.3]; the
		// old code passed the nil attr into handleATT and panicked.
		a := s.prepareWriteRequestAttr
		if a == nil {
			break
		}
		if e := handleATT(a, s, r, ble.NewResponseWriter(nil)); e != ble.ErrSuccess {
			return newErrorResponse(r.AttributeOpcode(), 0, e)
		}
	default:
		return newErrorResponse(r.AttributeOpcode(), 0x0000, ble.ErrInvalidPDU)
	}

	return []byte{ExecuteWriteResponseCode}
}

// handle Write command. [Vol 3, Part F, 3.4.5.3]
func (s *Server) handleWriteCommand(r WriteCommand) []byte {
	// Validate the request.
	switch {
	case len(r) <= 3:
		return nil
	}

	a, ok := s.db.at(r.AttributeHandle())
	if !ok {
		return nil
	}

	// We don't support write to static value. Pass the request to upper layer.
	if a == nil {
		return nil
	}
	if e := handleATT(a, s, r, s.dummyRspWriter); e != ble.ErrSuccess {
		return nil
	}
	return nil
}

// maxPreparedWriteBytes caps the total payload a peer may stage across
// Prepare Write requests before Execute Write; beyond it the server answers
// Prepare Queue Full [Vol 3, Part F, 3.4.6.1]. A variable so tests (or
// embedders) can tune it.
var maxPreparedWriteBytes = 512

func newErrorResponse(op byte, h uint16, s ble.ATTError) []byte {
	r := ErrorResponse(make([]byte, 5))
	r.SetAttributeOpcode()
	r.SetRequestOpcodeInError(op)
	r.SetAttributeInError(h)
	r.SetErrorCode(uint8(s))
	return r
}

func handleATT(a *attr, s *Server, req []byte, rsp ble.ResponseWriter) ble.ATTError {
	rsp.SetStatus(ble.ErrSuccess)
	var offset int
	var data []byte
	conn := s.conn
	switch req[0] {
	case FindByTypeValueRequestCode:
		// The dynamic attribute's current value is needed for the match;
		// without this case the request always failed with ErrReqNotSupp.
		fallthrough
	case ReadByGroupTypeRequestCode:
		fallthrough
	case ReadByTypeRequestCode:
		fallthrough
	case ReadRequestCode:
		if a.rh == nil {
			return ble.ErrReadNotPerm
		}
		a.rh.ServeRead(ble.NewRequest(conn, data, offset), rsp)
	case ReadBlobRequestCode:
		if a.rh == nil {
			return ble.ErrReadNotPerm
		}
		offset = int(ReadBlobRequest(req).ValueOffset())
		a.rh.ServeRead(ble.NewRequest(conn, data, offset), rsp)
	case PrepareWriteRequestCode:
		if a.wh == nil {
			return ble.ErrWriteNotPerm
		}
		data = PrepareWriteRequest(req).PartAttributeValue()
		if logDebugEnabled() {
			ble.Logger.Debug("handleATT PartAttributeValue",
				"data", fmt.Sprintf("%x", data),
				"offset", int(PrepareWriteRequest(req).ValueOffset()),
				"attr", fmt.Sprintf("%p", s.prepareWriteRequestAttr))
		}

		if s.prepareWriteRequestAttr == nil {
			s.prepareWriteRequestAttr = a
			s.prepareWriteRequestData.Reset()
		}
		// Bound the queue: the peer controls both the number of prepare
		// writes and their payload sizes, so an unbounded append is a
		// remote memory DoS.
		if s.prepareWriteRequestData.Len()+len(data) > maxPreparedWriteBytes {
			return ble.ErrPrepQueueFull
		}
		s.prepareWriteRequestData.Write(data)

	case ExecuteWriteRequestCode:
		if a.wh == nil {
			return ble.ErrWriteNotPerm
		}
		data = s.prepareWriteRequestData.Bytes()
		a.wh.ServeWrite(ble.NewRequest(conn, data, offset), rsp)
		s.prepareWriteRequestAttr = nil
		s.prepareWriteRequestData.Reset()
	case WriteRequestCode:
		fallthrough
	case WriteCommandCode:
		if a.wh == nil {
			return ble.ErrWriteNotPerm
		}
		data = WriteRequest(req).AttributeValue()
		a.wh.ServeWrite(ble.NewRequest(conn, data, offset), rsp)
	// case SignedWriteCommandCode:
	// case ReadByGroupTypeRequestCode:
	// case ReadMultipleRequestCode:
	default:
		return ble.ErrReqNotSupp
	}

	return rsp.Status()
}
