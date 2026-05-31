package banIface

type IServer interface {
	Start()
	Stop()
	Serve()
	AddRouter(msgID string, router IRouter)
	GetConnMgr() IConnManager

	SetConnStartFunc(func(connect IConnect))
	SetConnStopFunc(func(connect IConnect))
	CallConnStartFunc(connect IConnect)
	CallConnStopFunc(connect IConnect)
}
