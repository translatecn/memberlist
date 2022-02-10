package main

import (
	"flag"
	"fmt"
	"github.com/hashicorp/memberlist"
	"net"
	"strconv"
	"time"
)

var port int

func init() {
	flag.IntVar(&port, "port", 9000, "gossip port")
}
func main() {
	flag.Parse()
	conf := memberlist.DefaultLocalConfig()
	conf.AdvertisePort = port
	conf.BindPort = port
	m, err := memberlist.Create(conf)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	// Ask for members of the cluster
	for _, member := range m.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}
	go func() {
		for {
			Addr, _ := net.ResolveUDPAddr("udp", net.JoinHostPort(net.IPv4(127, 0, 0, 1).String(), strconv.Itoa(port)))
			timeCost, err := m.Ping(m.Config.Name, Addr)
			fmt.Println("===>", timeCost, err)
			time.Sleep(time.Second)
		}
	}()
	time.Sleep(time.Second * 1000)
}
