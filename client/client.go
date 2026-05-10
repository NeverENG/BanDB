package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/NeverENG/bandb/network/banNet"
	"github.com/NeverENG/bandb/pkg/utils"
)

// Client KV 存储客户�?type Client struct {
	addr string
	conn net.Conn
}

// NewClient 创建新客户端
func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
	}
}

// Connect 连接到服务端
func (c *Client) Connect() error {
	var err error
	c.conn, err = net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", c.addr, err)
	}

	// 设置读写超时
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	return nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SendPut 发�?PUT 请求
func (c *Client) SendPut(key []byte, value []byte) error {
	// 构建消息：使�?utils.NewMessage
	msg := utils.NewMessage(1, key, value) // msgID=1 表示 PUT

	// 使用 banNet.DataPack 打包消息
	dp := banNet.NewDataPack()
	packet, err := dp.Pack(msg)
	if err != nil {
		return fmt.Errorf("failed to pack message: %v", err)
	}

	// 设置写超�?	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// 发送数�?	_, err = c.conn.Write(packet)
	if err != nil {
		return fmt.Errorf("failed to send PUT request: %v", err)
	}

	// 设置读超�?	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 读取响应
	response, err := c.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	// 检查响应状�?	if len(response) < 1 {
		return fmt.Errorf("invalid response")
	}

	if response[0] == 0x01 {
		return fmt.Errorf("Server error")
	}

	return nil
}

// SendGet 发�?GET 请求
func (c *Client) SendGet(key []byte) ([]byte, error) {
	// 构建 GET 消息：只需�?key
	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))

	data := utils.ByteBuilder(keyLenBytes, key)
	msg := utils.NewMessage2(2, data) // msgID=2 表示 GET

	// 使用 banNet.DataPack 打包
	dp := banNet.NewDataPack()
	packet, err := dp.Pack(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to pack message: %v", err)
	}

	// 设置写超�?	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// 发送数�?	_, err = c.conn.Write(packet)
	if err != nil {
		return nil, fmt.Errorf("failed to send GET request: %v", err)
	}

	// 设置读超�?	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 读取响应
	response, err := c.readResponse()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	// 检查响应状�?	if len(response) < 1 {
		return nil, fmt.Errorf("invalid response")
	}

	if response[0] == 0x01 {
		return nil, fmt.Errorf("key not found or Server error")
	}

	// 解析 value：状�?1字节) + value_len(4字节) + value
	if len(response) < 5 {
		return nil, fmt.Errorf("invalid response format")
	}

	valueLen := binary.LittleEndian.Uint32(response[1:5])
	if len(response) < 5+int(valueLen) {
		return nil, fmt.Errorf("incomplete response")
	}

	value := response[5 : 5+valueLen]
	return value, nil
}

// SendDelete 发�?DELETE 请求
func (c *Client) SendDelete(key []byte) error {
	// 构建 DELETE 消息：只需�?key
	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))

	data := utils.ByteBuilder(keyLenBytes, key)
	msg := utils.NewMessage2(3, data) // msgID=3 表示 DELETE

	// 使用 banNet.DataPack 打包
	dp := banNet.NewDataPack()
	packet, err := dp.Pack(msg)
	if err != nil {
		return fmt.Errorf("failed to pack message: %v", err)
	}

	// 设置写超�?	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// 发送数�?	_, err = c.conn.Write(packet)
	if err != nil {
		return fmt.Errorf("failed to send DELETE request: %v", err)
	}

	// 设置读超�?	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 读取响应
	response, err := c.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	// 检查响应状�?	if len(response) < 1 {
		return fmt.Errorf("invalid response")
	}

	if response[0] == 0x01 {
		return fmt.Errorf("Server error")
	}

	return nil
}

// readResponse 读取响应数据
func (c *Client) readResponse() ([]byte, error) {
	// 使用 banNet.DataPack 解包
	dp := banNet.NewDataPack()
	headLen := dp.GetHeadLen()

	// 先读取消息头
	header := make([]byte, headLen)
	_, err := c.conn.Read(header)
	if err != nil {
		return nil, fmt.Errorf("failed to read response header: %v", err)
	}

	// 解包头信�?	tempMsg, err := dp.UnPack(header)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack header: %v", err)
	}

	// 读取消息�?	dataLen := tempMsg.GetMsgLen()
	if dataLen > 0 {
		data := make([]byte, dataLen)
		_, err = c.conn.Read(data)
		if err != nil {
			return nil, fmt.Errorf("failed to read response data: %v", err)
		}
		return data, nil
	}

	return []byte{}, nil
}
