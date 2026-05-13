package main

import (
	"fmt"

	"github.com/NeverENG/BanDB/network/banNet"
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
	server.AddRouter(1, router) // PUT 操作
	server.AddRouter(2, router) // GET 操作
	server.AddRouter(3, router) // DELETE 操作

	// 注册连接生命周期回调
	server.SetConnStartFunc(router.OnConnStart)
	server.SetConnStopFunc(router.OnConnStop)

	// 启动服务
	fmt.Println("Starting Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	server.Serve()
}
