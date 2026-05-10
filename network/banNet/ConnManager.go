package banNet

import (
	"sync"

	"github.com/NeverENG/BanDB/network/banIface"
)

type ConnManager struct {
	mu          sync.RWMutex
	connections map[uint32]banIface.IConnect
}

var _ banIface.IConnManager = &ConnManager{}

func NewConnManager() *ConnManager {
	return &ConnManager{
		connections: make(map[uint32]banIface.IConnect),
	}
}

func (cm *ConnManager) Add(conn banIface.IConnect) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.connections[conn.GetConnID()] = conn
}

func (cm *ConnManager) Remove(conn banIface.IConnect) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.connections, conn.GetConnID())
}

func (cm *ConnManager) Get(connId uint32) banIface.IConnect {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if conn, ok := cm.connections[connId]; ok {
		return conn
	}
	return nil
}

func (cm *ConnManager) Len() int {
	return len(cm.connections)
}

func (cm *ConnManager) ClearConn() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for connId, conn := range cm.connections {
		conn.Stop()
		delete(cm.connections, connId)
	}
}
