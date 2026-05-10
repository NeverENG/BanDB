package banNet

import "github.com/NeverENG/BanDB/network/banIface"

type Request struct {
	msg  banIface.IMessage
	conn banIface.IConnect
}

var _ banIface.IRequest = &Request{}

func NewRequest(msg banIface.IMessage, conn banIface.IConnect) *Request {
	return &Request{
		msg:  msg,
		conn: conn,
	}
}
func (req *Request) GetMsgData() []byte {
	return req.msg.GetData()
}

func (req *Request) GetMsgID() uint32 {
	return req.msg.GetMsgID()
}

func (req *Request) GetConnection() banIface.IConnect {
	return req.conn
}
