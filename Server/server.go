package main

import (
	"fmt"

	"github.com/NeverENG/BanDB/network/banNet"
	"github.com/NeverENG/BanDB/pkg/proto"
	"github.com/NeverENG/BanDB/service"
)

func main() {
	// 初始化 FSM
	KVServer := service.NewKVServer()

	// 启动 FSM
	go KVServer.Run()

	// 初始化 HA
	ha := service.NewHA(KVServer)

	// 初始化网络服务
	server := banNet.NewServer()

	// 创建路由
	router := service.NewRouter(KVServer)

	// 注册路由
	server.AddRouter(proto.MsgPut, router)
	server.AddRouter(proto.MsgGet, router)
	server.AddRouter(proto.MsgDelete, router)

	// 注册连接生命周期回调
	server.SetConnStartFunc(router.OnConnStart)
	server.SetConnStopFunc(router.OnConnStop)

	// 启动服务
	fmt.Println("Starting Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	server.Serve()
}
