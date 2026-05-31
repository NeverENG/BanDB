package banIface

type IRequest interface {
	GetConnection() IConnect
	GetMsgData() []byte
	GetMsgID() string
}
