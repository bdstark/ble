package att

import "errors"

var (
	// ErrInvalidArgument means one or more of the arguments are invalid.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrInvalidResponse means one or more of the response fields are invalid.
	ErrInvalidResponse = errors.New("invalid response")

	// ErrSeqProtoTimeout means the request hasn't been acknowledged in 30 seconds.
	// [Vol 3, Part F, 3.3.3]
	ErrSeqProtoTimeout = errors.New("req timeout")

	// ErrInvalidMTU means the peer proposed an ATT MTU below the spec minimum
	// of 23 bytes (ble.DefaultMTU). [Vol 3, Part F, 3.4.2]
	ErrInvalidMTU = errors.New("invalid MTU")
)

var rspOfReq = map[byte]byte{
	ExchangeMTURequestCode:     ExchangeMTUResponseCode,
	FindInformationRequestCode: FindInformationResponseCode,
	FindByTypeValueRequestCode: FindByTypeValueResponseCode,
	ReadByTypeRequestCode:      ReadByTypeResponseCode,
	ReadRequestCode:            ReadResponseCode,
	ReadBlobRequestCode:        ReadBlobResponseCode,
	ReadMultipleRequestCode:    ReadMultipleResponseCode,
	ReadByGroupTypeRequestCode: ReadByGroupTypeResponseCode,
	WriteRequestCode:           WriteResponseCode,
	PrepareWriteRequestCode:    PrepareWriteResponseCode,
	ExecuteWriteRequestCode:    ExecuteWriteResponseCode,
	HandleValueIndicationCode:  HandleValueConfirmationCode,
}
