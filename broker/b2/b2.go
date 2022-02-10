package main

import (
	"flag"
	"fmt"
	"github.com/hashicorp/memberlist"
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
	time.Sleep(time.Second * 1000)
}
