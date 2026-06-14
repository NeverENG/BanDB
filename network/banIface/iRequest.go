package banIface

type IRequest interface {
	GetConnection() IConnect
	GetMsgData() []byte
	GetMsgID() string
	// SetMsgData 改写本帧负载，供 PreHandle 钩子做脱敏/裁剪。
	SetMsgData([]byte)
}
