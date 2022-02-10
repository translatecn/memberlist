package test

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iretry "github.com/hashicorp/memberlist/internal/retry"
	"github.com/stretchr/testify/require"
)

func HostMemberlist(host string, t *testing.T, f func(*memberlist.Config)) *memberlist.Members {
	t.Helper()

	c := memberlist.DefaultLANConfig()
	c.Name = host
	c.BindAddr = host
	c.BindPort = 0 // choose a free Port
	c.Logger = log.New(os.Stderr, host, log.LstdFlags)
	if f != nil {
		f(c)
	}

	m, err := memberlist.NewMembers(c)
	if err != nil {
		t.Fatalf("failed to get memberlist: %s", err)
	}
	return m
}

func TestMemberList_Probe(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{
		Node:        Addr1.String(),
		Addr:        []byte(Addr1),
		Port:        uint16(m1.Config.BindPort),
		Incarnation: 1,
		Vsn:         m1.Config.BuildVsnArray(),
	}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{
		Node:        Addr2.String(),
		Addr:        []byte(Addr2),
		Port:        uint16(m2.Config.BindPort),
		Incarnation: 1,
		Vsn:         m2.Config.BuildVsnArray(),
	}
	m1.AliveNode(&a2, nil, false)

	// should ping Addr2
	m1.Probe()

	// Should not be marked Suspect
	n := m1.NodeMap[Addr2.String()]
	if n.State != memberlist.StateAlive {
		t.Fatalf("Expect node to be Alive")
	}

	// Should increment seqno
	if m1.SequenceNum != 1 {
		t.Fatalf("bad seqno %v", m2.SequenceNum)
	}
}

func TestMemberList_ProbeNode_Suspect(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()
	m3 := HostMemberlist(Addr3.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()
	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: m3.Config.BuildVsnArray()}
	m1.AliveNode(&a3, nil, false)
	a4 := memberlist.Alive{Node: Addr4.String(), Addr: ip4, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a4, nil, false)

	n := m1.NodeMap[Addr4.String()]
	m1.ProbeNode(n)

	// Should be marked Suspect.
	if n.State != memberlist.StateSuspect {
		t.Fatalf("Expect node to be Suspect")
	}
	time.Sleep(10 * time.Millisecond)

	// One of the peers should have attempted an indirect probe.
	if s2, s3 := atomic.LoadUint32(&m2.SequenceNum), atomic.LoadUint32(&m3.SequenceNum); s2 != 1 && s3 != 1 {
		t.Fatalf("bad seqnos, expected both to be 1: %v, %v", s2, s3)
	}
}

func TestMemberList_ProbeNode_Suspect_Dogpile(t *testing.T) {
	cases := []struct {
		name          string
		numPeers      int
		confirmations int
		expected      time.Duration
	}{
		{"n=2, k=3 (max timeout disabled)", 1, 0, 500 * time.Millisecond},
		{"n=3, k=3", 2, 0, 500 * time.Millisecond},
		{"n=4, k=3", 3, 0, 500 * time.Millisecond},
		{"n=5, k=3 (max timeout starts to take effect)", 4, 0, 1000 * time.Millisecond},
		{"n=6, k=3", 5, 0, 1000 * time.Millisecond},
		{"n=6, k=3 (confirmations start to lower timeout)", 5, 1, 750 * time.Millisecond},
		{"n=6, k=3", 5, 2, 604 * time.Millisecond},
		{"n=6, k=3 (timeout driven to nominal value)", 5, 3, 500 * time.Millisecond},
		{"n=6, k=3", 5, 4, 500 * time.Millisecond},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Create the main memberlist under test.
			Addr := getBindAddr()

			m := HostMemberlist(Addr.String(), t, func(c *memberlist.Config) {
				c.ProbeTimeout = time.Millisecond
				c.ProbeInterval = 100 * time.Millisecond
				c.SuspicionMult = 5
				c.SuspicionMaxTimeoutMult = 2
			})
			defer m.Shutdown()

			bindPort := m.Config.BindPort

			a := memberlist.Alive{Node: Addr.String(), Addr: []byte(Addr), Port: uint16(bindPort), Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
			m.AliveNode(&a, nil, true)

			// Make all but one peer be an real, Alive instance.
			var peers []*memberlist.Members
			for j := 0; j < c.numPeers-1; j++ {
				peerAddr := getBindAddr()

				peer := HostMemberlist(peerAddr.String(), t, func(c *memberlist.Config) {
					c.BindPort = bindPort
				})
				defer peer.Shutdown()

				peers = append(peers, peer)

				a = memberlist.Alive{Node: peerAddr.String(), Addr: []byte(peerAddr), Port: uint16(bindPort), Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
				m.AliveNode(&a, nil, false)
			}

			// Just use a bogus Address for the last peer so it doesn't respond
			// to pings, but tell the memberlist it's Alive.
			badPeerAddr := getBindAddr()
			a = memberlist.Alive{Node: badPeerAddr.String(), Addr: []byte(badPeerAddr), Port: uint16(bindPort), Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
			m.AliveNode(&a, nil, false)

			// Force a probe, which should start us into the Suspect state.
			m.ProbeNodeByAddr(badPeerAddr.String())

			if m.GetNodeState(badPeerAddr.String()) != memberlist.StateSuspect {
				t.Fatalf("case %d: expected node to be Suspect", i)
			}

			// Add the requested number of confirmations.
			for j := 0; j < c.confirmations; j++ {
				from := fmt.Sprintf("peer%d", j)
				s := memberlist.Suspect{Node: badPeerAddr.String(), Incarnation: 1, From: from}
				m.SuspectNode(&s)
			}

			// Wait until right before the timeout and make sure the timer
			// hasn't fired.
			fudge := 25 * time.Millisecond
			time.Sleep(c.expected - fudge)

			if m.GetNodeState(badPeerAddr.String()) != memberlist.StateSuspect {
				t.Fatalf("case %d: expected node to still be Suspect", i)
			}

			// Wait through the timeout and a little after to make sure the
			// timer fires.
			time.Sleep(2 * fudge)

			if m.GetNodeState(badPeerAddr.String()) != memberlist.StateDead {
				t.Fatalf("case %d: expected node to be Dead", i)
			}
		})
	}
}

/*
func TestMemberList_ProbeNode_FallbackTCP(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(Addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	m4 := HostMemberlist(Addr4.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m4.Shutdown()

	a1 := Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a1, nil, true)
	a2 := Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a2, nil, false)
	a3 := Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a3, nil, false)

	// Make sure m4 is configured with the same 协议版本 as m1 so
	// the TCP fallback behavior is enabled.
	a4 := Alive{
		Node:        Addr4.String(),
		Addr:        ip4,
		Port:        uint16(bindPort),
		Incarnation: 1,
		Vsn: []uint8{
			ProtocolVersionMin,
			ProtocolVersionMax,
			m1.Config.ProtocolVersion,
			m1.Config.DelegateProtocolMin,
			m1.Config.DelegateProtocolMax,
			m1.Config.DelegateProtocolVersion,
		},
	}
	m1.AliveNode(&a4, nil, false)

	// Isolate m4 from UDP traffic by re-opening its listener on the wrong
	// Port. This should force the TCP fallback path to be used.
	var err error
	if err = m4.UdpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	udpAddr := &net.UDPAddr{IP: ip4, Port: 9999}
	if m4.UdpListener, err = net.ListenUDP("udp", udpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Have node m1 probe m4.
	n := m1.NodeMap[Addr4.String()]
	startProbe := time.Now()
	m1.ProbeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Should be marked Alive because of the TCP fallback ping.
	if n.State != stateAlive {
		t.Fatalf("expect node to be Alive")
	}

	// Make sure TCP activity completed in a timely manner.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.SequenceNum != 1 && m3.SequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.SequenceNum, m3.SequenceNum)
	}

	// Now shutdown all inbound TCP traffic to make sure the TCP fallback
	// path properly fails when the node is really unreachable.
	if err = m4.tcpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	tcpAddr := &net.TCPAddr{IP: ip4, Port: 9999}
	if m4.tcpListener, err = net.ListenTCP("tcp", tcpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Probe again, this time there should be no contact.
	startProbe = time.Now()
	m1.ProbeNode(n)
	probeTime = time.Now().Sub(startProbe)

	// Node should be reported Suspect.
	if n.State != stateSuspect {
		t.Fatalf("expect node to be Suspect")
	}

	// Make sure TCP activity didn't cause us to wait too long before
	// timing out.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.SequenceNum != 2 && m3.SequenceNum != 2 {
		t.Fatalf("bad seqnos %v, %v", m2.SequenceNum, m3.SequenceNum)
	}
}

func TestMemberList_ProbeNode_FallbackTCP_Disabled(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(Addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	m4 := HostMemberlist(Addr4.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m4.Shutdown()

	a1 := Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a1, nil, true)
	a2 := Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a2, nil, false)
	a3 := Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a3, nil, false)

	// Make sure m4 is configured with the same 协议版本 as m1 so
	// the TCP fallback behavior is enabled.
	a4 := Alive{
		Node:        Addr4.String(),
		Addr:        ip4,
		Port:        uint16(bindPort),
		Incarnation: 1,
		Vsn: []uint8{
			ProtocolVersionMin,
			ProtocolVersionMax,
			m1.Config.ProtocolVersion,
			m1.Config.DelegateProtocolMin,
			m1.Config.DelegateProtocolMax,
			m1.Config.DelegateProtocolVersion,
		},
	}
	m1.AliveNode(&a4, nil, false)

	// Isolate m4 from UDP traffic by re-opening its listener on the wrong
	// Port. This should force the TCP fallback path to be used.
	var err error
	if err = m4.UdpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	udpAddr := &net.UDPAddr{IP: ip4, Port: 9999}
	if m4.UdpListener, err = net.ListenUDP("udp", udpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Disable the TCP pings using the Config mechanism.
	m1.Config.DisableTcpPings = true

	// Have node m1 probe m4.
	n := m1.NodeMap[Addr4.String()]
	startProbe := time.Now()
	m1.ProbeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported Suspect.
	if n.State != stateSuspect {
		t.Fatalf("expect node to be Suspect")
	}

	// Make sure TCP activity didn't cause us to wait too long before
	// timing out.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.SequenceNum != 1 && m3.SequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.SequenceNum, m3.SequenceNum)
	}
}

func TestMemberList_ProbeNode_FallbackTCP_OldProtocol(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(Addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	m4 := HostMemberlist(Addr4.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m4.Shutdown()

	a1 := Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a1, nil, true)
	a2 := Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a2, nil, false)
	a3 := Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a3, nil, false)

	// Set up m4 so that it doesn't understand a version of the protocol
	// that supports TCP pings.
	a4 := Alive{
		Node:        Addr4.String(),
		Addr:        ip4,
		Port:        uint16(bindPort),
		Incarnation: 1,
		Vsn: []uint8{
			ProtocolVersionMin,
			ProtocolVersion2Compatible,
			ProtocolVersion2Compatible,
			m1.Config.DelegateProtocolMin,
			m1.Config.DelegateProtocolMax,
			m1.Config.DelegateProtocolVersion,
		},
	}
	m1.AliveNode(&a4, nil, false)

	// Isolate m4 from UDP traffic by re-opening its listener on the wrong
	// Port. This should force the TCP fallback path to be used.
	var err error
	if err = m4.UdpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	udpAddr := &net.UDPAddr{IP: ip4, Port: 9999}
	if m4.UdpListener, err = net.ListenUDP("udp", udpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Have node m1 probe m4.
	n := m1.NodeMap[Addr4.String()]
	startProbe := time.Now()
	m1.ProbeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported Suspect.
	if n.State != stateSuspect {
		t.Fatalf("expect node to be Suspect")
	}

	// Make sure TCP activity didn't cause us to wait too long before
	// timing out.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.SequenceNum != 1 && m3.SequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.SequenceNum, m3.SequenceNum)
	}
}
*/

func TestMemberList_ProbeNode_Awareness_Degraded(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	var probeTimeMin time.Duration
	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMin = 2*c.ProbeInterval - 50*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr3.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m3.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: m3.Config.BuildVsnArray()}
	m1.AliveNode(&a3, nil, false)

	vsn4 := []uint8{
		memberlist.ProtocolVersionMin, memberlist.ProtocolVersionMax, memberlist.ProtocolVersionMin,
		1, 1, 1,
	}
	// Node 4 never gets started.
	a4 := memberlist.Alive{Node: Addr4.String(), Addr: ip4, Port: uint16(bindPort), Incarnation: 1, Vsn: vsn4}
	m1.AliveNode(&a4, nil, false)

	// Start the health in a degraded state.
	m1.Awareness.ApplyDelta(1)
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m4.
	n := m1.NodeMap[Addr4.String()]
	startProbe := time.Now()
	m1.ProbeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported Suspect.
	if n.State != memberlist.StateSuspect {
		t.Fatalf("expect node to be Suspect")
	}

	// Make sure we timed out approximately on time (note that we accounted
	// for the slowed-down failure detector in the probeTimeMin calculation.
	if probeTime < probeTimeMin {
		t.Fatalf("probed too quickly, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	if m2.SequenceNum != 1 && m3.SequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.SequenceNum, m3.SequenceNum)
	}

	// We should have gotten all the nacks, so our score should remain the
	// same, since we didn't get a successful probe.
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_ProbeNode_Wrong_VSN(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr3.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m3.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: m3.Config.BuildVsnArray()}
	m1.AliveNode(&a3, nil, false)

	vsn4 := []uint8{
		0, 0, 0,
		0, 0, 0,
	}
	// Node 4 never gets started.
	a4 := memberlist.Alive{Node: Addr4.String(), Addr: ip4, Port: uint16(bindPort), Incarnation: 1, Vsn: vsn4}
	m1.AliveNode(&a4, nil, false)

	// Start the health in a degraded state.
	m1.Awareness.ApplyDelta(1)
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m4.
	n, ok := m1.NodeMap[Addr4.String()]
	if ok || n != nil {
		t.Fatalf("expect node a4 to be not taken into account, because of its wrong version")
	}
}

func TestMemberList_ProbeNode_Awareness_Improved(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)

	// Start the health in a degraded state.
	m1.Awareness.ApplyDelta(1)
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m2.
	n := m1.NodeMap[Addr2.String()]
	m1.ProbeNode(n)

	// Node should be reported Alive.
	if n.State != memberlist.StateAlive {
		t.Fatalf("expect node to be Suspect")
	}

	// Our score should have improved since we did a good probe.
	if score := m1.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_ProbeNode_Awareness_MissedNack(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 50*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)

	vsn := m1.Config.BuildVsnArray()
	// Node 3 and node 4 never get started.
	a3 := memberlist.Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: vsn}
	m1.AliveNode(&a3, nil, false)
	a4 := memberlist.Alive{Node: Addr4.String(), Addr: ip4, Port: uint16(bindPort), Incarnation: 1, Vsn: vsn}
	m1.AliveNode(&a4, nil, false)

	// Make sure health looks good.
	if score := m1.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m4.
	n := m1.NodeMap[Addr4.String()]
	startProbe := time.Now()
	m1.ProbeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported Suspect.

	m1.NodeLock.Lock()
	if n.State != memberlist.StateSuspect {
		t.Fatalf("expect node to be Suspect")
	}
	m1.NodeLock.Unlock()

	// Make sure we timed out approximately on time.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// We should have gotten dinged for the missed nack. Note that the code under
	// test is waiting for probeTimeMax and then doing some other work before it
	// updates the awareness, so we need to wait some extra time. Rather than just
	// add longer and longer sleeps, we'll retry a few times.
	iretry.Run(t, func(r *iretry.R) {
		if score := m1.GetHealthScore(); score != 1 {
			r.Fatalf("expected health score to decrement on missed nack. want %d, "+
				"got: %d", 1, score)
		}
	})
}

func TestMemberList_ProbeNode_Awareness_OldProtocol(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	Addr4 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)
	ip4 := []byte(Addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr3.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a3, nil, false)

	// Node 4 never gets started.
	a4 := memberlist.Alive{Node: Addr4.String(), Addr: ip4, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a4, nil, false)

	// Make sure health looks good.
	if score := m1.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m4.
	n := m1.NodeMap[Addr4.String()]
	startProbe := time.Now()
	m1.ProbeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported Suspect.
	if n.State != memberlist.StateSuspect {
		t.Fatalf("expect node to be Suspect")
	}

	// Make sure we timed out approximately on time.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.SequenceNum != 1 && m3.SequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.SequenceNum, m3.SequenceNum)
	}

	// Since we are using the old protocol here, we should have gotten dinged
	// for a failed health check.
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_ProbeNode_Buddy(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}

	m1.AliveNode(&a1, nil, true)
	m1.AliveNode(&a2, nil, false)
	m2.AliveNode(&a2, nil, true)

	// Force the state to Suspect so we piggyback a Suspect message with the ping.
	// We should see this get refuted later, and the ping will succeed.
	n := m1.NodeMap[Addr2.String()]
	n.State = memberlist.StateSuspect
	m1.ProbeNode(n)

	// Make sure a ping was sent.
	if m1.SequenceNum != 1 {
		t.Fatalf("bad seqno %v", m1.SequenceNum)
	}

	// Check a broadcast is queued.
	if num := m2.Broadcasts.NumQueued(); num != 1 {
		t.Fatalf("expected only one queued message: %d", num)
	}

	// Should be Alive msg.
	if memberlist.MessageType(m2.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.AliveMsg {
		t.Fatalf("expected queued Alive msg")
	}
}

func TestMemberList_ProbeNode(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a2, nil, false)

	n := m1.NodeMap[Addr2.String()]
	m1.ProbeNode(n)

	// Should be marked Alive
	if n.State != memberlist.StateAlive {
		t.Fatalf("Expect node to be Alive")
	}

	// Should increment seqno
	if m1.SequenceNum != 1 {
		t.Fatalf("bad seqno %v", m1.SequenceNum)
	}
}

func TestMemberList_Ping(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.ProbeTimeout = 1 * time.Second
		c.ProbeInterval = 10 * time.Second
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.AliveNode(&a2, nil, false)

	// Do a legit ping.
	n := m1.NodeMap[Addr2.String()]
	Addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(Addr2.String(), strconv.Itoa(bindPort)))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	rtt, err := m1.Ping(n.Name, Addr)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !(rtt > 0) {
		t.Fatalf("bad: %v", rtt)
	}

	// This ping has a bad node name so should timeout.
	_, err = m1.Ping("bad", Addr)
	if _, ok := err.(memberlist.NoPingResponseError); !ok || err == nil {
		t.Fatalf("bad: %v", err)
	}
}

func TestMemberList_ResetNodes(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.GossipToTheDeadTime = 100 * time.Millisecond
	})
	defer m.Shutdown()

	a1 := memberlist.Alive{Node: "test1", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a1, nil, false)
	a2 := memberlist.Alive{Node: "test2", Addr: []byte{127, 0, 0, 2}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: "test3", Addr: []byte{127, 0, 0, 3}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a3, nil, false)
	d := memberlist.Dead{Node: "test2", Incarnation: 1}
	m.DeadNode(&d)

	m.ResetNodes()
	if len(m.Nodes) != 3 {
		t.Fatalf("Bad length")
	}
	if _, ok := m.NodeMap["test2"]; !ok {
		t.Fatalf("test2 should not be unmapped")
	}

	time.Sleep(200 * time.Millisecond)
	m.ResetNodes()
	if len(m.Nodes) != 2 {
		t.Fatalf("Bad length")
	}
	if _, ok := m.NodeMap["test2"]; ok {
		t.Fatalf("test2 should be unmapped")
	}
}

func TestMemberList_NextSeq(t *testing.T) {
	m := &memberlist.Members{}
	if m.NextSeqNo() != 1 {
		t.Fatalf("bad sequence no")
	}
	if m.NextSeqNo() != 2 {
		t.Fatalf("bad sequence no")
	}
}

func AckHandlerExists(t *testing.T, m *memberlist.Members, idx uint32) bool {
	t.Helper()

	m.AckLock.Lock()
	_, ok := m.AckHandlers[idx]
	m.AckLock.Unlock()

	return ok
}

func TestMemberList_SetProbeChannels(t *testing.T) {
	m := &memberlist.Members{AckHandlers: make(map[uint32]*memberlist.AckHandler)}

	ch := make(chan memberlist.AckMessage, 1)
	m.SetProbeChannels(0, ch, nil, 10*time.Millisecond)

	require.True(t, AckHandlerExists(t, m, 0), "missing handler")

	time.Sleep(20 * time.Millisecond)

	require.False(t, AckHandlerExists(t, m, 0), "non-reaped handler")
}

func TestMemberList_SetAckHandler(t *testing.T) {
	m := &memberlist.Members{AckHandlers: make(map[uint32]*memberlist.AckHandler)}

	f := func([]byte, time.Time) {}
	m.SetAckHandler(0, f, 10*time.Millisecond)

	require.True(t, AckHandlerExists(t, m, 0), "missing handler")

	time.Sleep(20 * time.Millisecond)

	require.False(t, AckHandlerExists(t, m, 0), "non-reaped handler")
}

func TestMemberList_InvokeAckHandler(t *testing.T) {
	m := &memberlist.Members{AckHandlers: make(map[uint32]*memberlist.AckHandler)}

	// Does nothing
	m.InvokeAckHandler(memberlist.AckResp{}, time.Now())

	var b bool
	f := func(payload []byte, timestamp time.Time) { b = true }
	m.SetAckHandler(0, f, 10*time.Millisecond)

	// Should set b
	m.InvokeAckHandler(memberlist.AckResp{}, time.Now())
	if !b {
		t.Fatalf("b not set")
	}

	require.False(t, AckHandlerExists(t, m, 0), "non-reaped handler")
}

func TestMemberList_InvokeAckHandler_Channel_Ack(t *testing.T) {
	m := &memberlist.Members{AckHandlers: make(map[uint32]*memberlist.AckHandler)}

	ack := memberlist.AckResp{Payload: []byte{0, 0, 0}}

	// Does nothing
	m.InvokeAckHandler(ack, time.Now())

	ackCh := make(chan memberlist.AckMessage, 1)
	nackCh := make(chan struct{}, 1)
	m.SetProbeChannels(0, ackCh, nackCh, 10*time.Millisecond)

	// Should send message
	m.InvokeAckHandler(ack, time.Now())

	select {
	case v := <-ackCh:
		if v.Complete != true {
			t.Fatalf("Bad value")
		}
		if bytes.Compare(v.Payload, ack.Payload) != 0 {
			t.Fatalf("wrong payload. expected: %v; actual: %v", ack.Payload, v.Payload)
		}

	case <-nackCh:
		t.Fatalf("should not get a nack")

	default:
		t.Fatalf("message not sent")
	}

	require.False(t, AckHandlerExists(t, m, 0), "non-reaped handler")
}

func TestMemberList_InvokeAckHandler_Channel_Nack(t *testing.T) {
	m := &memberlist.Members{AckHandlers: make(map[uint32]*memberlist.AckHandler)}

	nack := memberlist.NAckResp{}

	// Does nothing.
	m.InvokeNAckHandler(nack)

	ackCh := make(chan memberlist.AckMessage, 1)
	nackCh := make(chan struct{}, 1)
	m.SetProbeChannels(0, ackCh, nackCh, 10*time.Millisecond)

	// Should send message.
	m.InvokeNAckHandler(nack)

	select {
	case <-ackCh:
		t.Fatalf("should not get an ack")

	case <-nackCh:
		// Good.

	default:
		t.Fatalf("message not sent")
	}

	// Getting a nack doesn't reap the handler so that we can still forward
	// an ack up to the reap time, if we get one.
	require.True(t, AckHandlerExists(t, m, 0), "handler should not be reaped")

	ack := memberlist.AckResp{Payload: []byte{0, 0, 0}}
	m.InvokeAckHandler(ack, time.Now())

	select {
	case v := <-ackCh:
		if v.Complete != true {
			t.Fatalf("Bad value")
		}
		if bytes.Compare(v.Payload, ack.Payload) != 0 {
			t.Fatalf("wrong payload. expected: %v; actual: %v", ack.Payload, v.Payload)
		}

	case <-nackCh:
		t.Fatalf("should not get a nack")

	default:
		t.Fatalf("message not sent")
	}

	require.False(t, AckHandlerExists(t, m, 0), "non-reaped handler")
}

func TestMemberList_AliveNode_NewNode(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.Events = &memberlist.ChannelEventDelegate{Ch: ch}
	})
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	if len(m.Nodes) != 1 {
		t.Fatalf("should add node")
	}

	state, ok := m.NodeMap["test"]
	if !ok {
		t.Fatalf("should map node")
	}

	if state.Incarnation != 1 {
		t.Fatalf("bad incarnation")
	}
	if state.State != memberlist.StateAlive {
		t.Fatalf("bad state")
	}
	if time.Now().Sub(state.StateChange) > time.Second {
		t.Fatalf("bad change delta")
	}

	// Check for a join message
	select {
	case e := <-ch:
		if e.Node.Name != "test" {
			t.Fatalf("bad node name")
		}
	default:
		t.Fatalf("no join message")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected queued message")
	}
}

func TestMemberList_AliveNode_SuspectNode(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)
	ted := &toggledEventDelegate{
		real: &memberlist.ChannelEventDelegate{Ch: ch},
	}
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.Events = ted
	})
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	// Listen only after first join
	ted.Toggle(true)

	// Make Suspect
	state := m.NodeMap["test"]
	state.State = memberlist.StateSuspect
	state.StateChange = state.StateChange.Add(-time.Hour)

	// Old incarnation number, should not change
	m.AliveNode(&a, nil, false)
	if state.State != memberlist.StateSuspect {
		t.Fatalf("update with old incarnation!")
	}

	// Should reset to Alive now
	a.Incarnation = 2
	m.AliveNode(&a, nil, false)
	if state.State != memberlist.StateAlive {
		t.Fatalf("no update with new incarnation!")
	}

	if time.Now().Sub(state.StateChange) > time.Second {
		t.Fatalf("bad change delta")
	}

	// Check for a no join message
	select {
	case <-ch:
		t.Fatalf("got bad join message")
	default:
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected queued message")
	}
}

func TestMemberList_AliveNode_Idempotent(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)
	ted := &toggledEventDelegate{
		real: &memberlist.ChannelEventDelegate{Ch: ch},
	}
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.Events = ted
	})
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	// Listen only after first join
	ted.Toggle(true)

	// Make Suspect
	state := m.NodeMap["test"]
	stateTime := state.StateChange

	// Should reset to Alive now
	a.Incarnation = 2
	m.AliveNode(&a, nil, false)
	if state.State != memberlist.StateAlive {
		t.Fatalf("non idempotent")
	}

	if stateTime != state.StateChange {
		t.Fatalf("should not change state")
	}

	// Check for a no join message
	select {
	case <-ch:
		t.Fatalf("got bad join message")
	default:
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}
}

type toggledEventDelegate struct {
	mu      sync.Mutex
	real    memberlist.EventDelegate
	enabled bool
}

func (d *toggledEventDelegate) Toggle(enabled bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enabled = enabled
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (d *toggledEventDelegate) NotifyJoin(n *memberlist.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.enabled {
		d.real.NotifyJoin(n)
	}
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (d *toggledEventDelegate) NotifyLeave(n *memberlist.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.enabled {
		d.real.NotifyLeave(n)
	}
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (d *toggledEventDelegate) NotifyUpdate(n *memberlist.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.enabled {
		d.real.NotifyUpdate(n)
	}
}

// Serf Bug: GH-58, Meta data does not update
func TestMemberList_AliveNode_ChangeMeta(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)
	ted := &toggledEventDelegate{
		real: &memberlist.ChannelEventDelegate{Ch: ch},
	}

	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.Events = ted
	})
	defer m.Shutdown()

	a := memberlist.Alive{
		Node:        "test",
		Addr:        []byte{127, 0, 0, 1},
		Meta:        []byte("val1"),
		Incarnation: 1,
		Vsn:         m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	// Listen only after first join
	ted.Toggle(true)

	// Make Suspect
	state := m.NodeMap["test"]

	// Should reset to Alive now
	a.Incarnation = 2
	a.Meta = []byte("val2")
	m.AliveNode(&a, nil, false)

	// Check updates
	if bytes.Compare(state.Meta, a.Meta) != 0 {
		t.Fatalf("meta did not update")
	}

	// Check for a NotifyUpdate
	select {
	case e := <-ch:
		if e.Event != memberlist.NodeUpdate {
			t.Fatalf("bad event: %v", e)
		}
		if !reflect.DeepEqual(*e.Node, state.Node) {
			t.Fatalf("expected %v, got %v", *e.Node, state.Node)
		}
		if bytes.Compare(e.Node.Meta, a.Meta) != 0 {
			t.Fatalf("meta did not update")
		}
	default:
		t.Fatalf("missing event!")
	}

}

func TestMemberList_AliveNode_Refute(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: m.Config.Name, Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, true)

	// Clear queue_broadcast
	m.Broadcasts.Reset()

	// Conflicting Alive
	s := memberlist.Alive{
		Node:        m.Config.Name,
		Addr:        []byte{127, 0, 0, 1},
		Incarnation: 2,
		Meta:        []byte("foo"),
		Vsn:         m.Config.BuildVsnArray(),
	}
	m.AliveNode(&s, nil, false)

	state := m.NodeMap[m.Config.Name]
	if state.State != memberlist.StateAlive {
		t.Fatalf("should still be Alive")
	}
	if state.Meta != nil {
		t.Fatalf("meta should still be nil")
	}

	// Check a broad cast is queued
	if num := m.Broadcasts.NumQueued(); num != 1 {
		t.Fatalf("expected only one queued message: %d",
			num)
	}

	// Should be Alive mesg
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.AliveMsg {
		t.Fatalf("expected queued Alive msg")
	}
}

func TestMemberList_AliveNode_Conflict(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.DeadNodeReclaimTime = 10 * time.Millisecond
	})
	defer m.Shutdown()

	nodeName := "test"
	a := memberlist.Alive{Node: nodeName, Addr: []byte{127, 0, 0, 1}, Port: 8000, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, true)

	// Clear queue_broadcast
	m.Broadcasts.Reset()

	// Conflicting Alive
	s := memberlist.Alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 2},
		Port:        9000,
		Incarnation: 2,
		Meta:        []byte("foo"),
		Vsn:         m.Config.BuildVsnArray(),
	}
	m.AliveNode(&s, nil, false)

	state := m.NodeMap[nodeName]
	if state.State != memberlist.StateAlive {
		t.Fatalf("should still be Alive")
	}
	if state.Meta != nil {
		t.Fatalf("meta should still be nil")
	}
	if bytes.Equal(state.Addr, []byte{127, 0, 0, 2}) {
		t.Fatalf("Address should not be updated")
	}
	if state.Port == 9000 {
		t.Fatalf("Port should not be updated")
	}

	// Check a broad cast is queued
	if num := m.Broadcasts.NumQueued(); num != 0 {
		t.Fatalf("expected 0 queued messages: %d", num)
	}

	// Change the node to Dead
	d := memberlist.Dead{Node: nodeName, Incarnation: 2}
	m.DeadNode(&d)
	m.Broadcasts.Reset()

	state = m.NodeMap[nodeName]
	if state.State != memberlist.StateDead {
		t.Fatalf("should be Dead")
	}

	time.Sleep(m.Config.DeadNodeReclaimTime)

	// New Alive node
	s2 := memberlist.Alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 2},
		Port:        9000,
		Incarnation: 3,
		Meta:        []byte("foo"),
		Vsn:         m.Config.BuildVsnArray(),
	}
	m.AliveNode(&s2, nil, false)

	state = m.NodeMap[nodeName]
	if state.State != memberlist.StateAlive {
		t.Fatalf("should still be Alive")
	}
	if !bytes.Equal(state.Meta, []byte("foo")) {
		t.Fatalf("meta should be updated")
	}
	if !bytes.Equal(state.Addr, []byte{127, 0, 0, 2}) {
		t.Fatalf("Address should be updated")
	}
	if state.Port != 9000 {
		t.Fatalf("Port should be updated")
	}
}

func TestMemberList_SuspectNode_NoNode(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	s := memberlist.Suspect{Node: "test", Incarnation: 1}
	m.SuspectNode(&s)
	if len(m.Nodes) != 0 {
		t.Fatalf("don't expect Nodes")
	}
}

func TestMemberList_SuspectNode(t *testing.T) {
	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.ProbeInterval = time.Millisecond
		c.SuspicionMult = 1
	})
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	m.ChangeNode("test", func(state *memberlist.NodeState) {
		state.StateChange = state.StateChange.Add(-time.Hour)
	})

	s := memberlist.Suspect{Node: "test", Incarnation: 1}
	m.SuspectNode(&s)

	if m.GetNodeState("test") != memberlist.StateSuspect {
		t.Fatalf("Bad state")
	}

	change := m.GetNodeStateChange("test")
	if time.Now().Sub(change) > time.Second {
		t.Fatalf("bad change delta")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a Suspect message
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.SuspectMsg {
		t.Fatalf("expected queued Suspect msg")
	}

	// Wait for the timeout
	time.Sleep(10 * time.Millisecond)

	if m.GetNodeState("test") != memberlist.StateDead {
		t.Fatalf("Bad state")
	}

	newChange := m.GetNodeStateChange("test")
	if time.Now().Sub(newChange) > time.Second {
		t.Fatalf("bad change delta")
	}
	if !newChange.After(change) {
		t.Fatalf("should increment time")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a Suspect message
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.DeadMsg {
		t.Fatalf("expected queued Dead msg")
	}
}

func TestMemberList_SuspectNode_DoubleSuspect(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	state := m.NodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	s := memberlist.Suspect{Node: "test", Incarnation: 1}
	m.SuspectNode(&s)

	if state.State != memberlist.StateSuspect {
		t.Fatalf("Bad state")
	}

	change := state.StateChange
	if time.Now().Sub(change) > time.Second {
		t.Fatalf("bad change delta")
	}

	// clear the broadcast queue_broadcast
	m.Broadcasts.Reset()

	// Suspect again
	m.SuspectNode(&s)

	if state.StateChange != change {
		t.Fatalf("unexpected state change")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 0 {
		t.Fatalf("expected only one queued message")
	}

}

func TestMemberList_SuspectNode_OldSuspect(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 10, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	state := m.NodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	// Clear queue_broadcast
	m.Broadcasts.Reset()

	s := memberlist.Suspect{Node: "test", Incarnation: 1}
	m.SuspectNode(&s)

	if state.State != memberlist.StateAlive {
		t.Fatalf("Bad state")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 0 {
		t.Fatalf("expected only one queued message")
	}
}

func TestMemberList_SuspectNode_Refute(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: m.Config.Name, Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, true)

	// Clear queue_broadcast
	m.Broadcasts.Reset()

	// Make sure health is in a good state
	if score := m.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}

	s := memberlist.Suspect{Node: m.Config.Name, Incarnation: 1}
	m.SuspectNode(&s)

	state := m.NodeMap[m.Config.Name]
	if state.State != memberlist.StateAlive {
		t.Fatalf("should still be Alive")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Should be Alive mesg
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.AliveMsg {
		t.Fatalf("expected queued Alive msg")
	}

	// Health should have been dinged
	if score := m.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_DeadNode_NoNode(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	d := memberlist.Dead{Node: "test", Incarnation: 1}
	m.DeadNode(&d)
	if len(m.Nodes) != 0 {
		t.Fatalf("don't expect Nodes")
	}
}

func TestMemberList_DeadNodeLeft(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)

	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.Events = &memberlist.ChannelEventDelegate{Ch: ch}
	})
	defer m.Shutdown()

	nodeName := "node1"
	s1 := memberlist.Alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 1},
		Port:        8000,
		Incarnation: 1,
		Vsn:         m.Config.BuildVsnArray(),
	}
	m.AliveNode(&s1, nil, false)

	// Read the join event
	<-ch

	d := memberlist.Dead{Node: nodeName, From: nodeName, Incarnation: 1}
	m.DeadNode(&d)

	// Read the Dead event
	<-ch

	state := m.NodeMap[nodeName]
	if state.State != memberlist.StateLeft {
		t.Fatalf("Bad state")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a Dead message
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.DeadMsg {
		t.Fatalf("expected queued Dead msg")
	}

	// Clear queue_broadcast
	// m.Broadcasts.Reset()

	// New Alive node
	s2 := memberlist.Alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 2},
		Port:        9000,
		Incarnation: 3,
		Meta:        []byte("foo"),
		Vsn:         m.Config.BuildVsnArray(),
	}
	m.AliveNode(&s2, nil, false)

	// Read the join event
	<-ch

	state = m.NodeMap[nodeName]
	if state.State != memberlist.StateAlive {
		t.Fatalf("should still be Alive")
	}
	if !bytes.Equal(state.Meta, []byte("foo")) {
		t.Fatalf("meta should be updated")
	}
	if !bytes.Equal(state.Addr, []byte{127, 0, 0, 2}) {
		t.Fatalf("Address should be updated")
	}
	if state.Port != 9000 {
		t.Fatalf("Port should be updated")
	}
}

func TestMemberList_DeadNode(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)

	m := GetMemberlist(t, func(c *memberlist.Config) {
		c.Events = &memberlist.ChannelEventDelegate{Ch: ch}
	})
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	// Read the join event
	<-ch

	state := m.NodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	d := memberlist.Dead{Node: "test", Incarnation: 1}
	m.DeadNode(&d)

	if state.State != memberlist.StateDead {
		t.Fatalf("Bad state")
	}

	change := state.StateChange
	if time.Now().Sub(change) > time.Second {
		t.Fatalf("bad change delta")
	}

	select {
	case leave := <-ch:
		if leave.Event != memberlist.NodeLeave || leave.Node.Name != "test" {
			t.Fatalf("bad node name")
		}
	default:
		t.Fatalf("no leave message")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a Dead message
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.DeadMsg {
		t.Fatalf("expected queued Dead msg")
	}
}

func TestMemberList_DeadNode_Double(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 1)
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	state := m.NodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	d := memberlist.Dead{Node: "test", Incarnation: 1}
	m.DeadNode(&d)

	// Clear queue_broadcast
	m.Broadcasts.Reset()

	// Notify after the first Dead
	m.Config.Events = &memberlist.ChannelEventDelegate{Ch: ch}

	// Should do nothing
	d.Incarnation = 2
	m.DeadNode(&d)

	select {
	case <-ch:
		t.Fatalf("should not get leave")
	default:
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 0 {
		t.Fatalf("expected only one queued message")
	}
}

func TestMemberList_DeadNode_OldDead(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 10, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	state := m.NodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	d := memberlist.Dead{Node: "test", Incarnation: 1}
	m.DeadNode(&d)

	if state.State != memberlist.StateAlive {
		t.Fatalf("Bad state")
	}
}

func TestMemberList_DeadNode_AliveReplay(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 10, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, false)

	d := memberlist.Dead{Node: "test", Incarnation: 10}
	m.DeadNode(&d)

	// Replay Alive at same incarnation
	m.AliveNode(&a, nil, false)

	// Should remain Dead
	state, ok := m.NodeMap["test"]
	if ok && state.State != memberlist.StateDead {
		t.Fatalf("Bad state")
	}
}

func TestMemberList_DeadNode_Refute(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a := memberlist.Alive{Node: m.Config.Name, Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a, nil, true)

	// Clear queue_broadcast
	m.Broadcasts.Reset()

	// Make sure health is in a good state
	if score := m.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}

	d := memberlist.Dead{Node: m.Config.Name, Incarnation: 1}
	m.DeadNode(&d)

	state := m.NodeMap[m.Config.Name]
	if state.State != memberlist.StateAlive {
		t.Fatalf("should still be Alive")
	}

	// Check a broad cast is queued
	if m.Broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Should be Alive mesg
	if memberlist.MessageType(m.Broadcasts.OrderedView(true)[0].B.Message()[0]) != memberlist.AliveMsg {
		t.Fatalf("expected queued Alive msg")
	}

	// We should have been dinged
	if score := m.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_MergeState(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	a1 := memberlist.Alive{Node: "test1", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a1, nil, false)
	a2 := memberlist.Alive{Node: "test2", Addr: []byte{127, 0, 0, 2}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: "test3", Addr: []byte{127, 0, 0, 3}, Incarnation: 1, Vsn: m.Config.BuildVsnArray()}
	m.AliveNode(&a3, nil, false)

	s := memberlist.Suspect{Node: "test1", Incarnation: 1}
	m.SuspectNode(&s)

	remote := []memberlist.PushNodeState{
		memberlist.PushNodeState{
			Name:        "test1",
			Addr:        []byte{127, 0, 0, 1},
			Incarnation: 2,
			State:       memberlist.StateAlive,
		},
		memberlist.PushNodeState{
			Name:        "test2",
			Addr:        []byte{127, 0, 0, 2},
			Incarnation: 1,
			State:       memberlist.StateSuspect,
		},
		memberlist.PushNodeState{
			Name:        "test3",
			Addr:        []byte{127, 0, 0, 3},
			Incarnation: 1,
			State:       memberlist.StateDead,
		},
		memberlist.PushNodeState{
			Name:        "test4",
			Addr:        []byte{127, 0, 0, 4},
			Incarnation: 2,
			State:       memberlist.StateAlive,
		},
	}

	// Listen for changes
	eventCh := make(chan memberlist.NodeEvent, 1)
	m.Config.Events = &memberlist.ChannelEventDelegate{Ch: eventCh}

	// Merge remote state
	m.MergeState(remote)

	// Check the states
	state := m.NodeMap["test1"]
	if state.State != memberlist.StateAlive || state.Incarnation != 2 {
		t.Fatalf("Bad state %v", state)
	}

	state = m.NodeMap["test2"]
	if state.State != memberlist.StateSuspect || state.Incarnation != 1 {
		t.Fatalf("Bad state %v", state)
	}

	state = m.NodeMap["test3"]
	if state.State != memberlist.StateSuspect {
		t.Fatalf("Bad state %v", state)
	}

	state = m.NodeMap["test4"]
	if state.State != memberlist.StateAlive || state.Incarnation != 2 {
		t.Fatalf("Bad state %v", state)
	}

	// Check the channels
	select {
	case e := <-eventCh:
		if e.Event != memberlist.NodeJoin || e.Node.Name != "test4" {
			t.Fatalf("bad node %v", e)
		}
	default:
		t.Fatalf("Expect join")
	}

	select {
	case e := <-eventCh:
		t.Fatalf("Unexpect event: %v", e)
	default:
	}
}

func TestMemberlist_Gossip(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 3)

	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	Addr3 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)
	ip3 := []byte(Addr3)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		// Set the gossip interval fast enough to get a reasonable test,
		// but slow enough to avoid "sendto: operation not permitted"
		c.GossipInterval = 10 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.Events = &memberlist.ChannelEventDelegate{Ch: ch}
		// Set the gossip interval fast enough to get a reasonable test,
		// but slow enough to avoid "sendto: operation not permitted"
		c.GossipInterval = 10 * time.Millisecond
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
	})
	defer m3.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)
	a3 := memberlist.Alive{Node: Addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: m3.Config.BuildVsnArray()}
	m1.AliveNode(&a3, nil, false)

	// Gossip should send all this to m2. Retry a few times because it's UDP and
	// timing and stuff makes this flaky without.
	retry(t, 15, 250*time.Millisecond, func(failf func(string, ...interface{})) {
		m1.Gossip()

		time.Sleep(3 * time.Millisecond)

		if len(ch) < 3 {
			failf("expected 3 messages from gossip but only got %d", len(ch))
		}
	})
}

func retry(t *testing.T, n int, w time.Duration, fn func(func(string, ...interface{}))) {
	t.Helper()
	for try := 1; try <= n; try++ {
		failed := false
		failFormat := ""
		var failArgs []interface{}
		failf := func(format string, args ...interface{}) {
			failed = true
			failFormat = format
			failArgs = args
		}

		fn(failf)

		if !failed {
			return
		}
		if try == n {
			t.Fatalf(failFormat, failArgs...)
		}
		time.Sleep(w)
	}
}

func TestMemberlist_GossipToDead(t *testing.T) {
	ch := make(chan memberlist.NodeEvent, 2)

	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.GossipInterval = time.Millisecond
		c.GossipToTheDeadTime = 100 * time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.Events = &memberlist.ChannelEventDelegate{Ch: ch}
	})

	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)

	// Shouldn't send anything to m2 here, node has been Dead for 2x the GossipToTheDeadTime
	m1.NodeMap[Addr2.String()].State = memberlist.StateDead
	m1.NodeMap[Addr2.String()].StateChange = time.Now().Add(-200 * time.Millisecond)
	m1.Gossip()

	select {
	case <-ch:
		t.Fatalf("shouldn't get gossip")
	case <-time.After(50 * time.Millisecond):
	}

	// Should gossip to m2 because its state has changed within GossipToTheDeadTime
	m1.NodeMap[Addr2.String()].StateChange = time.Now().Add(-20 * time.Millisecond)

	retry(t, 5, 10*time.Millisecond, func(failf func(string, ...interface{})) {
		m1.Gossip()

		time.Sleep(3 * time.Millisecond)

		if len(ch) < 2 {
			failf("expected 2 messages from gossip")
		}
	})
}

func TestMemberlist_FailedRemote(t *testing.T) {
	type test struct {
		name     string
		err      error
		expected bool
	}
	tests := []test{
		{"nil error", nil, false},
		{"normal error", fmt.Errorf(""), false},
		{"net.OpError for file", &net.OpError{Net: "file"}, false},
		{"net.OpError for udp", &net.OpError{Net: "udp"}, false},
		{"net.OpError for udp4", &net.OpError{Net: "udp4"}, false},
		{"net.OpError for udp6", &net.OpError{Net: "udp6"}, false},
		{"net.OpError for tcp", &net.OpError{Net: "tcp"}, false},
		{"net.OpError for tcp4", &net.OpError{Net: "tcp4"}, false},
		{"net.OpError for tcp6", &net.OpError{Net: "tcp6"}, false},
		{"net.OpError for tcp with dial", &net.OpError{Net: "tcp", Op: "dial"}, true},
		{"net.OpError for tcp with write", &net.OpError{Net: "tcp", Op: "write"}, true},
		{"net.OpError for tcp with read", &net.OpError{Net: "tcp", Op: "read"}, true},
		{"net.OpError for udp with write", &net.OpError{Net: "udp", Op: "write"}, true},
		{"net.OpError for udp with read", &net.OpError{Net: "udp", Op: "read"}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := memberlist.FailedRemote(test.err)
			if actual != test.expected {
				t.Fatalf("expected %t, got %t", test.expected, actual)
			}
		})
	}
}

func TestMemberlist_PushPull(t *testing.T) {
	Addr1 := getBindAddr()
	Addr2 := getBindAddr()
	ip1 := []byte(Addr1)
	ip2 := []byte(Addr2)

	ch := make(chan memberlist.NodeEvent, 3)

	m1 := HostMemberlist(Addr1.String(), t, func(c *memberlist.Config) {
		c.GossipInterval = 10 * time.Second
		c.PushPullInterval = time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.Config.BindPort

	m2 := HostMemberlist(Addr2.String(), t, func(c *memberlist.Config) {
		c.BindPort = bindPort
		c.GossipInterval = 10 * time.Second
		c.Events = &memberlist.ChannelEventDelegate{Ch: ch}
	})
	defer m2.Shutdown()

	a1 := memberlist.Alive{Node: Addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.Config.BuildVsnArray()}
	m1.AliveNode(&a1, nil, true)
	a2 := memberlist.Alive{Node: Addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.Config.BuildVsnArray()}
	m1.AliveNode(&a2, nil, false)

	// Gossip should send all this to m2. It's UDP though so retry a few times
	retry(t, 5, 10*time.Millisecond, func(failf func(string, ...interface{})) {
		m1.PushPull()

		time.Sleep(3 * time.Millisecond)

		if len(ch) < 2 {
			failf("expected 2 messages from PushPull")
		}
	})
}

func TestVerifyProtocol(t *testing.T) {
	cases := []struct {
		Anodes   [][3]uint8
		Bnodes   [][3]uint8
		expected bool
	}{
		// Both running identical everything
		{
			Anodes: [][3]uint8{
				{0, 0, 0},
			},
			Bnodes: [][3]uint8{
				{0, 0, 0},
			},
			expected: true,
		},

		// One can understand newer, but speaking same protocol
		{
			Anodes: [][3]uint8{
				{0, 0, 0},
			},
			Bnodes: [][3]uint8{
				{0, 1, 0},
			},
			expected: true,
		},

		// One is speaking outside the range
		{
			Anodes: [][3]uint8{
				{0, 0, 0},
			},
			Bnodes: [][3]uint8{
				{1, 1, 1},
			},
			expected: false,
		},

		// Transitively outside the range
		{
			Anodes: [][3]uint8{
				{0, 1, 0},
				{0, 2, 1},
			},
			Bnodes: [][3]uint8{
				{1, 3, 1},
			},
			expected: false,
		},

		// Multi-node
		{
			Anodes: [][3]uint8{
				{0, 3, 2},
				{0, 2, 0},
			},
			Bnodes: [][3]uint8{
				{0, 2, 1},
				{0, 5, 0},
			},
			expected: true,
		},
	}

	for _, tc := range cases {
		aCore := make([][6]uint8, len(tc.Anodes))
		aApp := make([][6]uint8, len(tc.Anodes))
		for i, n := range tc.Anodes {
			aCore[i] = [6]uint8{n[0], n[1], n[2], 0, 0, 0}
			aApp[i] = [6]uint8{0, 0, 0, n[0], n[1], n[2]}
		}

		bCore := make([][6]uint8, len(tc.Bnodes))
		bApp := make([][6]uint8, len(tc.Bnodes))
		for i, n := range tc.Bnodes {
			bCore[i] = [6]uint8{n[0], n[1], n[2], 0, 0, 0}
			bApp[i] = [6]uint8{0, 0, 0, n[0], n[1], n[2]}
		}

		// Test core protocol verification
		testVerifyProtocolSingle(t, aCore, bCore, tc.expected)
		testVerifyProtocolSingle(t, bCore, aCore, tc.expected)

		//  Test app protocol verification
		testVerifyProtocolSingle(t, aApp, bApp, tc.expected)
		testVerifyProtocolSingle(t, bApp, aApp, tc.expected)
	}
}

func testVerifyProtocolSingle(t *testing.T, A [][6]uint8, B [][6]uint8, expect bool) {
	m := GetMemberlist(t, nil)
	defer m.Shutdown()

	m.Nodes = make([]*memberlist.NodeState, len(A))
	for i, n := range A {
		m.Nodes[i] = &memberlist.NodeState{
			Node: memberlist.Node{
				PMin: n[0],
				PMax: n[1],
				PCur: n[2],
				DMin: n[3],
				DMax: n[4],
				DCur: n[5],
			},
		}
	}

	remote := make([]memberlist.PushNodeState, len(B))
	for i, n := range B {
		remote[i] = memberlist.PushNodeState{
			Name: fmt.Sprintf("node %d", i),
			Vsn:  []uint8{n[0], n[1], n[2], n[3], n[4], n[5]},
		}
	}

	err := m.VerifyProtocol(remote)
	if (err == nil) != expect {
		t.Fatalf("bad:\nA: %v\nB: %v\nErr: %s", A, B, err)
	}
}
