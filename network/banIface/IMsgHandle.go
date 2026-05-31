package banIface

type IMsgHandle interface {
	AddRouter(msgID string, router IRouter)
	DoMsgHandle(request IRequest)
	StartWorkerPool()
	SendMsgToTaskQueue(request IRequest)
	Stop()
}
