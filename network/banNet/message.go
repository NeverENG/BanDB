package banNet

import (
	"github.com/NeverENG/BanDB/network/banIface"
)

type Message struct {
	Id uint32

	DataLen uint32
	Data    []byte
}

var _ banIface.IMessage = &Message{}

func NewMessage(id uint32, data []byte) *Message {
	return &Message{
		Id:      id,
		DataLen: uint32(len(data)),
		Data:    data,
	}
}

func (m *Message) GetMsgID() uint32 {
	return m.Id
}
func (m *Message) GetMsgLen() uint32 {
	return m.DataLen
}
func (m *Message) GetData() []byte {
	return m.Data
}

func (m *Message) SetMsgID(id uint32) {
	m.Id = id
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}

func (m *Message) SetMsgLen(id uint32) {
	m.DataLen = id
}
