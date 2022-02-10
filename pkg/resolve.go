package pkg

import (
	"fmt"
	"github.com/miekg/dns"
	"net"
	"strings"
)

// IpPort 希望加入的节点信息
type IpPort struct {
	IP       net.IP
	Port     uint16
	NodeName string // optional
}

// TcpLookupIP 是一个辅助工具，用于启动对指定主机的基于TCP的DNS查询。
// 内置的Go解析器将首先进行UDP查询，只有在响应设置了truncate bit时才会使用TCP，这在像Consul的DNS服务器上并不常见。
// 通过直接进行TCP查询，我们得到了最大的主机列表加入的最佳机会。由于加入是相对罕见的事件，所以做这个相当昂贵的操作是可以的。
func TcpLookupIP(host string, defaultPort uint16, nodeName string, DNSConfigPath string) ([]IpPort, error) {
	// Don't attempt any TCP lookups against non-fully qualified domain
	// names, since those will likely come from the resolv.conf file.
	if !strings.Contains(host, ".") {
		return nil, nil
	}

	// Make sure the domain name is terminated with a dot (we know there's
	// at least one character at this point).
	dn := host
	if dn[len(dn)-1] != '.' {
		dn = dn + "."
	}

	// See if we can find a server to try.
	cc, err := dns.ClientConfigFromFile(DNSConfigPath)
	if err != nil {
		return nil, err
	}
	if len(cc.Servers) > 0 {
		// We support host:Port in the DNS Config, but need to add the
		// default Port if one is not supplied.
		server := cc.Servers[0]
		if !HasPort(server) {
			server = net.JoinHostPort(server, cc.Port)
		}

		// Do the lookup.
		c := new(dns.Client)
		c.Net = "tcp"
		msg := new(dns.Msg)
		msg.SetQuestion(dn, dns.TypeANY)
		in, _, err := c.Exchange(msg, server)
		if err != nil {
			return nil, err
		}

		// Handle any IPs we get back that we can attempt to join.
		var ips []IpPort
		for _, r := range in.Answer {
			switch rr := r.(type) {
			case *dns.A:
				ips = append(ips, IpPort{IP: rr.A, Port: defaultPort, NodeName: nodeName})
			case *dns.AAAA:
				ips = append(ips, IpPort{IP: rr.AAAA, Port: defaultPort, NodeName: nodeName})
			case *dns.CNAME:
				fmt.Println("[DEBUG] memberlist: 在TCP优先应答中忽略CNAME RR，", host)
			}
		}
		return ips, nil
	}

	return nil, nil
}

func UdpLookupIP(host string, defaultPort uint16, nodeName string) ([]IpPort, error) {
	// 尝试使用 udp 解析
	ans, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	ips := make([]IpPort, 0, len(ans))
	for _, ip := range ans {
		ips = append(ips, IpPort{IP: ip, Port: defaultPort, NodeName: nodeName})
	}
	return ips, nil
}
