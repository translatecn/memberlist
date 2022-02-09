package main

import (
	"flag"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/memberlist/broker/dev"
	"time"
)

var port int

func init() {
	flag.IntVar(&port, "port", 8000, "gossip port")
}
func main() {
	flag.Parse()
	conf := memberlist.DefaultLocalConfig() // 只有默认配置
	conf.AdvertisePort = port               // 向其他集群成员发布的地址
	conf.BindPort = port                    //TODO 集群中的节点通过绑定port进行通信
	dev.Println(conf)
	list, err := memberlist.Create(conf) // 节点探测、push\pull、gossip消息发送服务
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	// 通过指定至少一个已知的成员加入一个现有的集群。
	n, err := list.Join([]string{"127.0.0.1:8000", "127.0.0.1:9000"})
	if err != nil {
		panic("Failed to join cluster: " + err.Error())
	}
	time.Sleep(time.Second * 10)
	for _, member := range list.Members() { // 读取的m.nodes列表
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}
	fmt.Println(n)
	time.Sleep(time.Second * 1000)
}
