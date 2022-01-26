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
	list, err := memberlist.Create(conf)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	n, err := list.Join([]string{"127.0.0.1:8000", "127.0.0.1:9000"})
	if err != nil {
		panic("Failed to join cluster: " + err.Error())
	}

	// Ask for members of the cluster
	for _, member := range list.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}
	fmt.Println(n)
	time.Sleep(time.Second * 1000)
}
