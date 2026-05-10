package banNet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/network/banIface"
)

type DataPack struct{}

var _ banIface.IDataPack = &DataPack{}

func NewDataPack() *DataPack { return &DataPack{} }

func (dp *DataPack) GetHeadLen() uint32 {
	return 8 // uint32 * 2
}

func (dp *DataPack) Pack(msg banIface.IMessage) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetMsgLen()); err != nil {
		return nil, err
	}
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetMsgID()); err != nil {
		fmt.Println(msg.GetMsgID())
		return nil, err
	}
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetData()); err != nil {
		return nil, err
	}
	return dataBuff.Bytes(), nil
}

func (dp *DataPack) UnPack(data []byte) (banIface.IMessage, error) {
	dataBuff := bytes.NewReader(data)

	msg := &Message{}
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.DataLen); err != nil {
		return nil, err
	}
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.Id); err != nil {
		return nil, err
	}

	if config.G.MaxPackageSize > 0 && msg.DataLen > config.G.MaxPackageSize {
		fmt.Println(msg.GetMsgLen())
		return nil, errors.New("data too large")
	}
	return msg, nil
}
