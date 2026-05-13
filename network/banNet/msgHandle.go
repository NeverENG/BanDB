package banNet

import (
	"fmt"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

type MsgHandle struct {
	Arip           map[uint32]banIface.IRouter
	WorkerPoolSize uint32
	TaskQueue      []chan banIface.IRequest
}

func NewMsgHandle() *MsgHandle {
	return &MsgHandle{
		Arip:           make(map[uint32]banIface.IRouter),
		WorkerPoolSize: config.G.WorkerPoolSize,
		TaskQueue:      make([]chan banIface.IRequest, config.G.WorkerPoolSize),
	}
}

var _ banIface.IMsgHandle = &MsgHandle{}

func (m *MsgHandle) AddRouter(msgID uint32, r banIface.IRouter) {
	if _, ok := m.Arip[msgID]; ok {
		fmt.Println("[WARN] duplicate route registration:", m.Arip)
		return
	}
	m.Arip[msgID] = r
}

func (m *MsgHandle) DoMsgHandle(request banIface.IRequest) {
	handler, ok := m.Arip[request.GetMsgID()]
	if !ok {
		fmt.Println("[ERROR] unregistered MsgID:", request.GetMsgID())
		return
	}
	handler.PreHandle(request)
	handler.Handle(request)
	handler.PostHandle(request)
}

func (m *MsgHandle) StartWorkerPool() {
	for i := 0; i < int(m.WorkerPoolSize); i++ {
		m.TaskQueue[i] = make(chan banIface.IRequest, config.G.MaxWorkerTaskLen)
		go m.StartOneWorker(i, m.TaskQueue[i])
	}
}

func (m *MsgHandle) SendMsgToTaskQueue(request banIface.IRequest) {

	workerID := request.GetConnection().GetConnID() % m.WorkerPoolSize
	m.TaskQueue[workerID] <- request
}

func (m *MsgHandle) StartOneWorker(workerId int, taskQueue chan banIface.IRequest) {
	fmt.Println("[Worker] commenced — ID:", workerId)
	for {
		select {
		case request, ok := <-taskQueue:
			if !ok {
				fmt.Println("[ERROR] taskQueue is closed")
				return
			}
			m.DoMsgHandle(request)
		}
	}
}

func (m *MsgHandle) Stop() {
	fmt.Println("[MsgHandle] dispatching shutdown signal")

	for i := 0; i < int(m.WorkerPoolSize); i++ {
		if m.TaskQueue[i] != nil {
			close(m.TaskQueue[i])
		}
	}
	fmt.Println("[WorkPool] shutting down")
}
