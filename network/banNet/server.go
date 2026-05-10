package banNet

import (
	"fmt"
	"net"
	"os"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

type Server struct {
	IP        string
	Port      int
	Name      string
	IPVersion string
	ExitCh    chan os.Signal
	MsgHandle banIface.IMsgHandle
	ConnMgr   banIface.IConnManager

	ConnStartFunc func(conn banIface.IConnect)
	ConnStopFunc  func(conn banIface.IConnect)
	listener      *net.TCPListener
}

func (s *Server) AddRouter(msgId uint32, router banIface.IRouter) {
	s.MsgHandle.AddRouter(msgId, router)
}

func NewServer() banIface.IServer {
	return &Server{
		IPVersion: "tcp4",
		IP:        config.G.Host,
		Name:      config.G.Name,
		Port:      config.G.Port,
		ExitCh:    make(chan os.Signal),
		MsgHandle: NewMsgHandle(),
		ConnMgr:   NewConnManager(),
	}
}

func (s *Server) GetConnMgr() banIface.IConnManager {
	return s.ConnMgr
}

func (s *Server) Start() {
	fmt.Printf("[START]bandbNetWork:%s ip: %s port:%d \n", s.Name, s.IP, s.Port)

	go func() {

		s.MsgHandle.StartWorkerPool()

		TcPAddr, err := net.ResolveTCPAddr(s.IPVersion, fmt.Sprintf("%s:%d", s.IP, s.Port))
		if err != nil {
			fmt.Println("[ERROR] Get the Tcp Addr err :", err)
			return
		}
		listener, err := net.ListenTCP(s.IPVersion, TcPAddr)
		if err != nil {
			fmt.Println("[ERROR] ListenTcp err :", err)
			return
		}
		s.listener = listener

		var cid uint32
		cid = 0

		for {
			select {
			case <-s.ExitCh:
				s.Stop()
				fmt.Println("[INFO] Server Exit")

				return
			default:
				conn, err := listener.AcceptTCP()
				if err != nil {
					fmt.Println("[ERROR] Accept err :", err)
					continue
				}

				if s.ConnMgr.Len() >= config.G.MaxConn {
					conn.Close()
					continue
				}

				dealConn := NewConnection(conn, cid, s.MsgHandle, s)
				fmt.Println("链接启动中")
				go dealConn.Start()
				cid++
			}
		}
	}()
}

func (s *Server) Stop() {
	fmt.Println("[INFO]Server listener at IP : " + s.IP)

	s.ConnMgr.ClearConn()
	s.MsgHandle.Stop()

	if s.listener != nil {
		fmt.Println("[INFO]Server listener at Port : " + fmt.Sprint(s.Port))
		s.listener.Close()
	}
	fmt.Println("[INFO]安全退出")
}

func (s *Server) Serve() {
	s.Start()
	select {}
}

func (s *Server) SetConnStartFunc(f func(conn banIface.IConnect)) {
	s.ConnStartFunc = f
}
func (s *Server) SetConnStopFunc(f func(conn banIface.IConnect)) {
	s.ConnStopFunc = f
}
func (s *Server) CallConnStartFunc(conn banIface.IConnect) {
	if s.ConnStartFunc == nil {
		fmt.Println("[ERROR] CallConnStartFunc is nil!")
		return
	}
	s.ConnStartFunc(conn)
}

func (s *Server) CallConnStopFunc(conn banIface.IConnect) {
	if s.ConnStopFunc == nil {
		fmt.Println("[ERROR] CallConnStopFunc is nil!")
		return
	}
	s.ConnStopFunc(conn)
}
