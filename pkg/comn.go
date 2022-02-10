package pkg

import (
	"fmt"
	"github.com/sean-/seed"
	"math"
	"net"
	"strconv"
	"strings"
)

func init() {
	seed.Init()
}

type Address struct {
	//网络地址   IP:Port
	Addr string
	// 该地址的名字,可选
	Name string
}

func (a *Address) String() string {
	if a.Name != "" {
		return fmt.Sprintf("%s (%s)", a.Name, a.Addr)
	}
	return a.Addr
}

// RetransmitLimit 计算出重传的极限
func RetransmitLimit(retransmitMult, n int) int {
	nodeScale := math.Ceil(math.Log10(float64(n + 1)))
	limit := retransmitMult * int(nodeScale)
	return limit
}

// JoinHostPort host:port
func JoinHostPort(host string, port uint16) string {
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// HasPort "host", "host:port", "ipv6::Address",or "[ipv6::Address]:port"
// 是否包含端口
func HasPort(s string) bool {
	// IPv6 地址
	if strings.LastIndex(s, "[") == 0 {
		return strings.LastIndex(s, ":") > strings.LastIndex(s, "]")
	}
	//是否包含：
	return strings.Count(s, ":") == 1
}

// EnsurePort 确保给定了一个端口,如果没有设置，就使用默认的端口
func EnsurePort(s string, port int) string {
	if HasPort(s) {
		return s
	}
	// 如果是IPV6地址
	s = strings.Trim(s, "[]")
	s = net.JoinHostPort(s, strconv.Itoa(port))
	return s
}
