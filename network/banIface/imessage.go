package banIface

type IMessage interface {
	GetMsgID() string
	GetData() []byte
	GetMsgLen() uint32

	SetMsgLen(uint32)
	SetData([]byte)
	SetMsgID(string)
}
