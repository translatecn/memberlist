package test

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
	"os"
	"testing"
	"time"
)

// CheckInteg 如果没有启用集成测试，将跳过一个测试。
func CheckInteg(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}
}

func IsInteg() bool {
	return os.Getenv("INTEG_TESTS") != ""
}

func TestMemberlist_Integ(t *testing.T) {
	//CheckInteg(t)

	num := 16
	var members []*memberlist.Members

	secret := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	eventCh := make(chan memberlist.NodeEvent, num)

	Addr := "127.0.0.1"
	for i := 0; i < num; i++ {
		c := memberlist.DefaultLANConfig()
		c.Name = fmt.Sprintf("%s:%d", Addr, 12345+i)
		c.BindAddr = Addr
		c.BindPort = 12345 + i
		c.ProbeInterval = 20 * time.Millisecond
		c.ProbeTimeout = 100 * time.Millisecond
		c.GossipInterval = 20 * time.Millisecond
		c.PushPullInterval = 200 * time.Millisecond
		c.SecretKey = secret
		c.Logger = log.New(os.Stderr, c.Name, log.LstdFlags)

		if i == 0 {
			c.Events = &memberlist.ChannelEventDelegate{eventCh}
		}

		m, err := memberlist.Create(c)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		defer m.Shutdown()

		members = append(members, m)

		if i > 0 {
			last := members[i-1]
			num, err := m.Join([]string{last.Config.Name + "/" + last.Config.Name})
			if num == 0 || err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
		}
	}

	breakTimer := time.After(1 * time.Second)
WAIT:
	for {
		select {
		case e := <-eventCh:
			if e.Event == memberlist.NodeJoin {
				t.Logf("[DEBUG] Node join: %v (%d)", *e.Node, members[0].NumMembers())
			} else {
				t.Logf("[DEBUG] Node leave: %v (%d)", *e.Node, members[0].NumMembers())
			}
		case <-breakTimer:
			break WAIT
		}
	}

	for idx, m := range members {
		got := m.NumMembers()
		if got != num {
			t.Errorf("bad num members at idx %d. Expected %d. Got %d.", idx, num, got)
		}
	}
}
