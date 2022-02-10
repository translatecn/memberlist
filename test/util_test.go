package test

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/memberlist/pkg"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUtil_PortFunctions(t *testing.T) {
	tests := []struct {
		Addr       string
		hasPort    bool
		ensurePort string
	}{
		{"1.2.3.4", false, "1.2.3.4:8301"},
		{"1.2.3.4:1234", true, "1.2.3.4:1234"},
		{"2600:1f14:e22:1501:f9a:2e0c:a167:67e8", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"},
		{"[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"},
		{"[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234", true, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234"},
		{"localhost", false, "localhost:8301"},
		{"localhost:1234", true, "localhost:1234"},
		{"hashicorp.com", false, "hashicorp.com:8301"},
		{"hashicorp.com:1234", true, "hashicorp.com:1234"},
	}
	for _, tt := range tests {
		t.Run(tt.Addr, func(t *testing.T) {
			if got, want := pkg.HasPort(tt.Addr), tt.hasPort; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
			if got, want := pkg.EnsurePort(tt.Addr, 8301), tt.ensurePort; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	msg := &memberlist.Ping{SeqNo: 100}
	buf, err := memberlist.Encode(memberlist.PingMsg, msg)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	var out memberlist.Ping
	if err := memberlist.Decode(buf.Bytes()[1:], &out); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if msg.SeqNo != out.SeqNo {
		t.Fatalf("bad sequence no")
	}
}

func TestRandomOffset(t *testing.T) {
	vals := make(map[int]struct{})
	for i := 0; i < 100; i++ {
		offset := memberlist.RandomOffset(2 << 30)
		if _, ok := vals[offset]; ok {
			t.Fatalf("got collision")
		}
		vals[offset] = struct{}{}
	}
}

func TestRandomOffset_Zero(t *testing.T) {
	offset := memberlist.RandomOffset(0)
	if offset != 0 {
		t.Fatalf("bad offset")
	}
}

func TestSuspicionTimeout(t *testing.T) {
	timeouts := map[int]time.Duration{
		5:    1000 * time.Millisecond,
		10:   1000 * time.Millisecond,
		50:   1698 * time.Millisecond,
		100:  2000 * time.Millisecond,
		500:  2698 * time.Millisecond,
		1000: 3000 * time.Millisecond,
	}
	for n, expected := range timeouts {
		timeout := memberlist.SuspicionTimeout(3, n, time.Second) / 3
		if timeout != expected {
			t.Fatalf("bad: %v, %v", expected, timeout)
		}
	}
}

func TestShuffleNodes(t *testing.T) {
	orig := []*memberlist.NodeState{
		&memberlist.NodeState{
			State: memberlist.StateDead,
		},
		&memberlist.NodeState{
			State: memberlist.StateAlive,
		},
		&memberlist.NodeState{
			State: memberlist.StateAlive,
		},
		&memberlist.NodeState{
			State: memberlist.StateDead,
		},
		&memberlist.NodeState{
			State: memberlist.StateAlive,
		},
		&memberlist.NodeState{
			State: memberlist.StateAlive,
		},
		&memberlist.NodeState{
			State: memberlist.StateDead,
		},
		&memberlist.NodeState{
			State: memberlist.StateAlive,
		},
	}
	nodes := make([]*memberlist.NodeState, len(orig))
	copy(nodes[:], orig[:])

	if !reflect.DeepEqual(nodes, orig) {
		t.Fatalf("should match")
	}

	memberlist.ShuffleNodes(nodes)

	if reflect.DeepEqual(nodes, orig) {
		t.Fatalf("should not match")
	}
}

func TestPushPullScale(t *testing.T) {
	sec := time.Second
	for i := 0; i <= 32; i++ {
		if s := memberlist.PushPullScale(sec, i); s != sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
	for i := 33; i <= 64; i++ {
		if s := memberlist.PushPullScale(sec, i); s != 2*sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
	for i := 65; i <= 128; i++ {
		if s := memberlist.PushPullScale(sec, i); s != 3*sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
}

func TestMoveDeadNodes(t *testing.T) {
	nodes := []*memberlist.NodeState{
		&memberlist.NodeState{
			State:       memberlist.StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&memberlist.NodeState{
			State:       memberlist.StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		// This Dead node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		&memberlist.NodeState{
			State:       memberlist.StateDead,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		// This left node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		&memberlist.NodeState{
			State:       memberlist.StateLeft,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		&memberlist.NodeState{
			State:       memberlist.StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&memberlist.NodeState{
			State:       memberlist.StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&memberlist.NodeState{
			State:       memberlist.StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&memberlist.NodeState{
			State:       memberlist.StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&memberlist.NodeState{
			State:       memberlist.StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
	}

	idx := memberlist.MoveDeadNodes(nodes, (15 * time.Second))
	if idx != 5 {
		t.Fatalf("bad index")
	}
	for i := 0; i < idx; i++ {
		switch i {
		case 2:
			// Recently Dead node remains at index 2,
			// since Nodes are swapped out to move to end.
			if nodes[i].State != memberlist.StateDead {
				t.Fatalf("Bad state %d", i)
			}
		case 3:
			//Recently left node should remain at 3
			if nodes[i].State != memberlist.StateLeft {
				t.Fatalf("Bad State %d", i)
			}
		default:
			if nodes[i].State != memberlist.StateAlive {
				t.Fatalf("Bad state %d", i)
			}
		}
	}
	for i := idx; i < len(nodes); i++ {
		if !nodes[i].DeadOrLeft() {
			t.Fatalf("Bad state %d", i)
		}
	}
}

func TestKRandomNodes(t *testing.T) {
	nodes := []*memberlist.NodeState{}
	for i := 0; i < 90; i++ {
		// Half the Nodes are in a bad state
		state := memberlist.StateAlive
		switch i % 3 {
		case 0:
			state = memberlist.StateAlive
		case 1:
			state = memberlist.StateSuspect
		case 2:
			state = memberlist.StateDead
		}
		nodes = append(nodes, &memberlist.NodeState{
			Node: memberlist.Node{
				Name: fmt.Sprintf("test%d", i),
			},
			State: state,
		})
	}

	filterFunc := func(n *memberlist.NodeState) bool {
		if n.Name == "test0" || n.State != memberlist.StateAlive {
			return true
		}
		return false
	}

	s1 := memberlist.KRandomNodes(3, nodes, filterFunc)
	s2 := memberlist.KRandomNodes(3, nodes, filterFunc)
	s3 := memberlist.KRandomNodes(3, nodes, filterFunc)

	if reflect.DeepEqual(s1, s2) {
		t.Fatalf("unexpected equal")
	}
	if reflect.DeepEqual(s1, s3) {
		t.Fatalf("unexpected equal")
	}
	if reflect.DeepEqual(s2, s3) {
		t.Fatalf("unexpected equal")
	}

	for _, s := range [][]memberlist.Node{s1, s2, s3} {
		if len(s) != 3 {
			t.Fatalf("bad len")
		}
		for _, n := range s {
			if n.Name == "test0" {
				t.Fatalf("Bad name")
			}
			if n.State != memberlist.StateAlive {
				t.Fatalf("Bad state")
			}
		}
	}
}

func TestMakeCompoundMessage(t *testing.T) {
	msg := &memberlist.Ping{SeqNo: 100}
	buf, err := memberlist.Encode(memberlist.PingMsg, msg)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()}
	compound := memberlist.MakeCompoundMessage(msgs)

	if compound.Len() != 3*buf.Len()+3*memberlist.CompoundOverhead+memberlist.CompoundHeaderOverhead {
		t.Fatalf("bad len")
	}
}

func TestDecodeCompoundMessage(t *testing.T) {
	msg := &memberlist.Ping{SeqNo: 100}
	buf, err := memberlist.Encode(memberlist.PingMsg, msg)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()}
	compound := memberlist.MakeCompoundMessage(msgs)

	trunc, parts, err := memberlist.DecodeCompoundMessage(compound.Bytes()[1:])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if trunc != 0 {
		t.Fatalf("should not truncate")
	}
	if len(parts) != 3 {
		t.Fatalf("bad parts")
	}
	for _, p := range parts {
		if len(p) != buf.Len() {
			t.Fatalf("bad part len")
		}
	}
}

func TestDecodeCompoundMessage_NumberOfPartsOverflow(t *testing.T) {
	buf := []byte{0x80}
	_, _, err := memberlist.DecodeCompoundMessage(buf)
	require.Error(t, err)
	require.Equal(t, err.Error(), "truncated len slice")
}

func TestDecodeCompoundMessage_Trunc(t *testing.T) {
	msg := &memberlist.Ping{SeqNo: 100}
	buf, err := memberlist.Encode(memberlist.PingMsg, msg)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()}
	compound := memberlist.MakeCompoundMessage(msgs)

	trunc, parts, err := memberlist.DecodeCompoundMessage(compound.Bytes()[1:38])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if trunc != 1 {
		t.Fatalf("truncate: %d", trunc)
	}
	if len(parts) != 2 {
		t.Fatalf("bad parts")
	}
	for _, p := range parts {
		if len(p) != buf.Len() {
			t.Fatalf("bad part len")
		}
	}
}

func TestCompressDeCompressPayload(t *testing.T) {
	buf, err := memberlist.CompressPayload([]byte("testing"))
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	decomp, err := memberlist.DeCompressPayload(buf.Bytes()[1:])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if !reflect.DeepEqual(decomp, []byte("testing")) {
		t.Fatalf("bad payload: %v", decomp)
	}
}
