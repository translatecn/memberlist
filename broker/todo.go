package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/hashicorp/go-sockaddr"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

func ma2in() {
	ip := net.ParseIP("0.0.0.0")
	tcpAddr := &net.TCPAddr{IP: ip, Port: 0}
	tcpLn, _ := net.ListenTCP("tcp", tcpAddr)
	// 如果给定的配置端口为零，则使用第一个TCP监听器来挑选一个可用的端口，然后将其应用于其他所有的端口。
	// 返回随机端口
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(tcpLn.Addr().(*net.TCPAddr).Port)
	fmt.Println(sockaddr.GetPrivateIP())
	fmt.Println(sockaddr.GetPrivateIP())
	fmt.Println(sockaddr.GetPrivateIP())
	fmt.Println(sockaddr.GetPrivateIP())
}

func main() {
	context.Background().Done()
	runtime.GOMAXPROCS(10)
	//for i := 0; i < 10; i++ {
	//	go fmt.Println(i)
	//}
	//time.Sleep(time.Second)
	fmt.Println(net.JoinHostPort("f8:ff:c2:29:49:4f", strconv.Itoa(2223)))

	f, err := os.OpenFile("./broker/dev/json.go", os.O_RDONLY, 0777)
	br := bufio.NewReader(f)
	peeked, err := br.Peek(1)
	fmt.Println(peeked, err)
	peeked, err = br.Peek(2)
	fmt.Println(peeked, err)
	peeked, err = br.Peek(br.Buffered())
	fmt.Println(peeked, err)
	fmt.Println(^uint32(0))
	var a uint32 = 4
	atomic.AddUint32(&a, ^uint32(0))
	fmt.Println(a)

	time.AfterFunc(time.Second, func() {
		fmt.Println("====")
	})
	time.Sleep(time.Second * 3)
}

//package dev
//
//import (
//	"encoding/json"
//	"fmt"
//)
//
//func Println(data interface{})  {
//	marshal, _ := json.Marshal(data)
//	fmt.Println(string(marshal))
//}
