package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/NeverENG/bandb/network/banNet"
	"github.com/NeverENG/bandb/service"
)

func main() {
	go func() {
		fmt.Println("pprof is stating")

		if err := http.ListenAndServe(":6060", nil); err != nil {
			fmt.Println("[ERROR] pprof start err:", err)
		}
	}()
	KVServer := service.NewKVServer()

	// 启动 FSM
	go KVServer.Run()

	// 初始�?HA
	ha := service.NewHA(KVServer)

	// 初始化网络服�?
	server := banNet.NewServer()

	// 创建路由
	router := service.NewRouter(KVServer)

	// 注册路由
	server.AddRouter(1, router) // PUT 操作
	server.AddRouter(2, router) // GET 操作
	server.AddRouter(3, router) // DELETE 操作

	// 启动服务
	fmt.Println("Starting Server...")
	fmt.Printf("HA initialized, initial health status: %v\n", ha.IsHealthy())
	server.Serve()
}
