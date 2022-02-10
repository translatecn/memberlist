package test

import (
	"fmt"
	"github.com/hashicorp/memberlist/pkg"
	"net"
	"testing"
)

func TestLogging_Address(t *testing.T) {
	s := pkg.LogAddress(nil)
	if s != "from=<unknown Address>" {
		t.Fatalf("bad: %s", s)
	}

	Addr, err := net.ResolveIPAddr("ip4", "127.0.0.1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	s = pkg.LogAddress(Addr)
	if s != "from=127.0.0.1" {
		t.Fatalf("bad: %s", s)
	}
}

func TestLogging_Conn(t *testing.T) {
	s := pkg.LogConn(nil)
	if s != "from=<unknown Address>" {
		t.Fatalf("bad: %s", s)
	}

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer conn.Close()

	s = pkg.LogConn(conn)
	if s != fmt.Sprintf("from=%s", conn.RemoteAddr().String()) {
		t.Fatalf("bad: %s", s)
	}
}
