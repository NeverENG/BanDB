package banNet

import (
	"github.com/NeverENG/BanDB/network/banIface"
)

type Message struct {
	Id string

	IDLen   uint16 // 仅在 UnPack 解析头部时使用, 调用方据此再读取 Id 字节
	DataLen uint32
	Data    []byte
}

var _ banIface.IMessage = &Message{}

func NewMessage(id string, data []byte) *Message {
	return &Message{
		Id:      id,
		DataLen: uint32(len(data)),
		Data:    data,
	}
}

func (m *Message) GetMsgID() string {
	return m.Id
}
func (m *Message) GetMsgLen() uint32 {
	return m.DataLen
}
func (m *Message) GetData() []byte {
	return m.Data
}

func (m *Message) SetMsgID(id string) {
	m.Id = id
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}

func (m *Message) SetMsgLen(id uint32) {
	m.DataLen = id
}
