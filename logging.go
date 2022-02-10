package memberlist

import (
	"fmt"
	"net"
)

func LogAddress(Addr net.Addr) string {
	if Addr == nil {
		return "from=<unknown Address>"
	}

	return fmt.Sprintf("from=%s", Addr.String())
}

func LogStringAddress(Addr string) string {
	if Addr == "" {
		return "from=<unknown Address>"
	}

	return fmt.Sprintf("from=%s", Addr)
}

func LogConn(conn net.Conn) string {
	if conn == nil {
		return LogAddress(nil)
	}

	return LogAddress(conn.RemoteAddr())
}
