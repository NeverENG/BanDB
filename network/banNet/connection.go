package banNet

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

var _ banIface.IConnect = &Connection{}

type Connection struct {
	TCPServer banIface.IServer // 注入 ConnMgr
	// 主要维护链接
	Conn *net.TCPConn
	// 链接的唯一 ID
	ConnID uint32
	// 该链接是否关�?	isClose   bool
	MsgHandle banIface.IMsgHandle
	// 该链接状�?	ExitBuffChan chan bool

	ctx    context.Context
	cancel context.CancelFunc

	msgChan chan []byte

	msgBuffChan chan []byte

	property     map[string]interface{}
	propertyLock sync.RWMutex
}

func NewConnection(conn *net.TCPConn, ConnID uint32, handle banIface.IMsgHandle, server banIface.IServer) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		TCPServer:    server,
		Conn:         conn,
		ConnID:       ConnID,
		MsgHandle:    handle,
		ExitBuffChan: make(chan bool, 1),
		isClose:      false,
		ctx:          ctx,
		cancel:       cancel,
		msgChan:      make(chan []byte),
		msgBuffChan:  make(chan []byte, config.G.MaxMsgChanLen),
	}
	c.TCPServer.GetConnMgr().Add(c)
	return c
}
func (c *Connection) StartReader() {
	fmt.Println("[StartReader]")
	defer fmt.Println("[Conn] 完美退�?)
	defer c.Stop()

	for {
		if c.Conn == nil {
			return
		}

		dp := NewDataPack()

		headData := make([]byte, dp.GetHeadLen())
		if _, err := io.ReadFull(c.Conn, headData); err != nil {
			fmt.Println("read head err:", err)
			c.ExitBuffChan <- true
			return
		}
		msg, err := dp.UnPack(headData)
		if err != nil {
			fmt.Println("unpack err:", err)
			c.ExitBuffChan <- true
			return
		}

		var data []byte
		if msg.GetMsgLen() > 0 {
			data = make([]byte, msg.GetMsgLen())

			if _, err := io.ReadFull(c.Conn, data); err != nil {
				fmt.Println("read msg err:", err)
				c.ExitBuffChan <- true
				return
			}
		}
		msg.SetData(data)
		req := NewRequest(msg, c)
		// 根据有没有启�?WorkPool 选择不同的结�?		if config.G.WorkerPoolSize > 0 {
			c.MsgHandle.SendMsgToTaskQueue(req)
		} else {
			go c.MsgHandle.DoMsgHandle(req)
		}
	}
}

func (c *Connection) StartWriter() {
	fmt.Println("[StartWriter]")
	defer fmt.Println("[INFO] Writer 已关闭")
	defer c.Stop()

	for {
		// 检查最高优先级
		select {
		case <-c.ExitBuffChan:
			return
		case data, ok := <-c.msgChan:
			if !ok {
				return
			}
			if _, err := c.Conn.Write(data); err != nil {
				fmt.Println("Write err:", err)
				return
			}
		default:
			// 高优通道全空时，在此静默等待
			select {
			case <-c.ExitBuffChan:
				return
			case data, ok := <-c.msgChan:
				if !ok {
					return
				}
				if _, err := c.Conn.Write(data); err != nil {
					fmt.Println("Write err:", err)
					return
				}
			case data, ok := <-c.msgBuffChan:
				if !ok {
					return
				}
				if _, err := c.Conn.Write(data); err != nil {
					fmt.Println("Write err:", err)
					return
				}
			}
		}
	}
}

func (c *Connection) Start() {
	fmt.Println("[Connection] Start Connection", c.ConnID)
	go c.StartReader()
	go c.StartWriter()
	c.TCPServer.CallConnStartFunc(c)
	for {
		select {
		case <-c.ExitBuffChan:
			return
		}
	}
}

func (c *Connection) Stop() {
	fmt.Println("[Connection] Stop Connection", c.ConnID, "[ZINX]链接正在关闭")
	if c.isClose == true {
		return
	}
	c.isClose = true
	c.TCPServer.CallConnStopFunc(c)
	c.cancel()
	c.Conn.Close()
	c.ExitBuffChan <- true
	c.TCPServer.GetConnMgr().Remove(c)
	defer func() {
		recover()
	}()
	close(c.ExitBuffChan)
	close(c.msgChan)
	close(c.msgBuffChan)
}
func (c *Connection) GetConnID() uint32 {
	return c.ConnID
}
func (c *Connection) GetTcpConn() *net.TCPConn {
	return c.Conn
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *Connection) SendMsg(msgId uint32, data []byte) error {
	dp := NewDataPack()

	msg := NewMessage(msgId, data)
	Gdata, err := dp.Pack(msg)
	if err != nil {
		return err
	}
	if c.msgChan != nil {
		c.msgChan <- Gdata
	}
	return nil
}

func (c *Connection) SendBuffMsg(msgId uint32, data []byte) error {
	dp := NewDataPack()
	msg := NewMessage(msgId, data)
	Gdata, err := dp.Pack(msg)
	if err != nil {
		return err
	}
	if c.msgBuffChan != nil {
		c.msgBuffChan <- Gdata
	}

	return nil
}

func (c *Connection) SetProperty(key string, value interface{}) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	c.property[key] = value
}

func (c *Connection) GetProperty(key string) interface{} {
	c.propertyLock.RLock()
	defer c.propertyLock.RUnlock()
	return c.property[key]
}
func (c *Connection) RemoveProperty(key string) {
	c.propertyLock.Lock()
	defer c.propertyLock.Unlock()
	delete(c.property, key)
}
