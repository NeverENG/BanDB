package service

import (
	"encoding/binary"
	"log/slog"

	"github.com/NeverENG/BanDB/network/banIface"
	"github.com/NeverENG/BanDB/pkg/proto"
)

// Router 基础路由处理器
type Router struct {
	kv *KVServer

	// 前置处理函数
	preHandleFunc func(request banIface.IRequest)
	// 后置处理函数
	postHandleFunc func(request banIface.IRequest)
}

// NewRouter 创建新的路由处理器
func NewRouter(kv *KVServer) *Router {
	return &Router{
		kv: kv,
	}
}

// SetPreHandle 设置前置处理函数
func (r *Router) SetPreHandle(f func(request banIface.IRequest)) {
	r.preHandleFunc = f
}

// SetPostHandle 设置后置处理函数
func (r *Router) SetPostHandle(f func(request banIface.IRequest)) {
	r.postHandleFunc = f
}

// PreHandle 前置处理
func (r *Router) PreHandle(request banIface.IRequest) {
	if r.preHandleFunc != nil {
		r.preHandleFunc(request)
	}
}

// Handle 处理请求
func (r *Router) Handle(request banIface.IRequest) {
	msgID := request.GetMsgID()
	data := request.GetMsgData()

	switch msgID {
	case proto.MsgPut:
		r.handlePut(data, request)
	case proto.MsgGet:
		r.handleGet(data, request)
	case proto.MsgDelete:
		r.handleDelete(data, request)
	}
}

// statusPayload 编码 [statusLen u8][status bytes]
func statusPayload(status string) []byte {
	buf := make([]byte, 1+len(status))
	buf[0] = byte(len(status))
	copy(buf[1:], status)
	return buf
}

// sendErr 写回错误响应
func sendErr(req banIface.IRequest) {
	req.GetConnection().SendBuffMsg(proto.MsgRespErr, statusPayload(proto.StatusError))
}

// sendOK 写回 PUT/DEL 成功响应
func sendOK(req banIface.IRequest) {
	req.GetConnection().SendBuffMsg(proto.MsgRespOK, statusPayload(proto.StatusOK))
}

// handlePut 处理 PUT 操作
func (r *Router) handlePut(data []byte, request banIface.IRequest) {
	// 解析数据格式：key_len + key + value_len + value
	if len(data) < 8 {
		slog.Warn("[WARN] handlePut: data too short", "len", len(data))
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	valueLen := int(binary.LittleEndian.Uint32(data[4:8]))

	if len(data) < 8+keyLen+valueLen {
		slog.Warn("[WARN] handlePut: incomplete data", "expected", 8+keyLen+valueLen, "got", len(data))
		return
	}

	key := data[8 : 8+keyLen]
	value := data[8+keyLen : 8+keyLen+valueLen]

	cmd := Command{
		Type:  "Put",
		Key:   key,
		Value: value,
	}

	index, err := r.kv.AppendEntry(cmd)
	if err != nil {
		slog.Error("[ERROR] handlePut: AppendEntry failed", "error", err)
		sendErr(request)
		return
	}

	if err := r.kv.WaitForCommit(index); err != nil {
		slog.Error("[ERROR] handlePut: WaitForCommit failed", "error", err)
		sendErr(request)
		return
	}

	sendOK(request)
}

// handleGet 处理 GET 操作
func (r *Router) handleGet(data []byte, request banIface.IRequest) {
	if len(data) < 4 {
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))

	if len(data) < 4+keyLen {
		return
	}

	key := data[4 : 4+keyLen]

	value, err := r.kv.Get(key)
	if err != nil {
		sendErr(request)
		return
	}

	// 响应负载: [statusLen u8][status bytes][valueLen u32 LE][value]
	status := proto.StatusOK
	response := make([]byte, 1+len(status)+4+len(value))
	response[0] = byte(len(status))
	copy(response[1:], status)
	binary.LittleEndian.PutUint32(response[1+len(status):1+len(status)+4], uint32(len(value)))
	copy(response[1+len(status)+4:], value)

	request.GetConnection().SendBuffMsg(proto.MsgRespOK, response)
}

// handleDelete 处理 DELETE 操作
func (r *Router) handleDelete(data []byte, request banIface.IRequest) {
	if len(data) < 4 {
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))

	if len(data) < 4+keyLen {
		return
	}

	key := data[4 : 4+keyLen]

	cmd := Command{
		Type: "Delete",
		Key:  key,
	}

	index, err := r.kv.AppendEntry(cmd)
	if err != nil {
		sendErr(request)
		return
	}

	if err := r.kv.WaitForCommit(index); err != nil {
		sendErr(request)
		return
	}

	sendOK(request)
}

// PostHandle 后置处理
func (r *Router) PostHandle(request banIface.IRequest) {
	if r.postHandleFunc != nil {
		r.postHandleFunc(request)
	}
}

// OnConnStart 连接建立回调。
// 不向客户端主动下发任何消息：这是纯请求-响应协议，连接建立时推送一条
// 未经请求的问候会让客户端把它误读为下一个请求的响应，造成整条连接的
// 响应错位（每条连接首个操作失败）。
func (r *Router) OnConnStart(conn banIface.IConnect) {}

// OnConnStop 连接关闭回调。同理不主动下发消息。
func (r *Router) OnConnStop(conn banIface.IConnect) {}

// GetFSM 获取 FSM 实例
func (r *Router) GetFSM() *KVServer {
	return r.kv
}
