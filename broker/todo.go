package main

import (
	"fmt"
	"net"
)

func main() {
	ip := net.ParseIP("0.0.0.0")
	tcpAddr := &net.TCPAddr{IP: ip, Port: 0}
	tcpLn, _ := net.ListenTCP("tcp", tcpAddr)
	// 如果给定的配置端口为零，则使用第一个TCP监听器来挑选一个可用的端口，然后将其应用于其他所有的端口。
   // 返回随机端口
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
}
