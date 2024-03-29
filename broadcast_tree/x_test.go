package broadcast_tree

import (
	"github.com/google/btree"
	"github.com/hashicorp/memberlist/pkg"
	"github.com/stretchr/testify/require"
	"testing"
)

// OK
func TestLimitedBroadcastLess(t *testing.T) {
	cases := []struct {
		Name string
		A    *limitedBroadcast // lesser
		B    *limitedBroadcast
	}{
		{
			"diff-transmits",
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100}, // transmits  尝试传输的次数
			&limitedBroadcast{transmits: 1, msgLen: 10, id: 100},
		},
		{
			"same-transmits--diff-len",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
		},
		{
			"same-transmits--same-len--diff-id",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 90},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			a, b := c.A, c.B

			require.True(t, a.Less(b))

			tree := btree.New(32)

			tree.ReplaceOrInsert(b)
			tree.ReplaceOrInsert(a)

			min := tree.Min().(*limitedBroadcast)
			require.Equal(t, a.transmits, min.transmits)
			require.Equal(t, a.msgLen, min.msgLen)
			require.Equal(t, a.id, min.id)

			max := tree.Max().(*limitedBroadcast)
			require.Equal(t, b.transmits, max.transmits)
			require.Equal(t, b.msgLen, max.msgLen)
			require.Equal(t, b.id, max.id)
		})
	}
}

// OK
func TestTransmitLimited_Queue(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 1 }}
	// 向三个节点发送广播消息
	q.QueueBroadcast(&MemberlistBroadcast{"test", nil, nil}) // idGen =1
	q.QueueBroadcast(&MemberlistBroadcast{"foo", nil, nil})  // idGen =2
	q.QueueBroadcast(&MemberlistBroadcast{"bar", nil, nil})  // idGen =3

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump := q.OrderedView(true) // 生序
	if dump[0].B.(*MemberlistBroadcast).Node != "test" {
		t.Fatalf("missing test")
	}
	if dump[1].B.(*MemberlistBroadcast).Node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[2].B.(*MemberlistBroadcast).Node != "bar" {
		t.Fatalf("missing bar")
	}

	// 应使以前的信息无效
	q.QueueBroadcast(&MemberlistBroadcast{"test", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump = q.OrderedView(true)
	if dump[0].B.(*MemberlistBroadcast).Node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[1].B.(*MemberlistBroadcast).Node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[2].B.(*MemberlistBroadcast).Node != "test" {
		t.Fatalf("missing test")
	}
}

// OK
func TestTransmitLimited_GetBroadcasts(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 3, NumNodes: func() int { return 10 }}
	// 每条消息18byte
	q.QueueBroadcast(&MemberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// 每条消息2byte的开销，应该能获得4条消息
	all := q.GetBroadcasts(2, 80) // 获取80长度以内的任意节点可以发送的消息
	require.Equal(t, 4, len(all), "missing messages: %v", prettyPrintMessages(all))

	// 每条消息3byte的开销，应该能获得3条消息
	partial := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial), "missing messages: %v", prettyPrintMessages(partial))
}

func TestTransmitLimited_GetBroadcasts_Limit(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	require.Equal(t, int64(0), q.idGen, "the id generator seed starts at zero")
	require.Equal(t, 2, pkg.RetransmitLimit(q.RetransmitMult, q.NumNodes()), "sanity check transmit limits")

	// 18 bytes per message
	q.QueueBroadcast(&MemberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	require.Equal(t, int64(4), q.idGen, "we handed out 4 IDs")

	// 3 byte overhead, should only get 3 messages back
	partial1 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial1), "missing messages: %v", prettyPrintMessages(partial1))

	require.Equal(t, int64(4), q.idGen, "id generator doesn't reset until empty")

	partial2 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial2), "missing messages: %v", prettyPrintMessages(partial2))

	require.Equal(t, int64(4), q.idGen, "id generator doesn't reset until empty")

	// Only two not expired
	partial3 := q.GetBroadcasts(3, 80)
	require.Equal(t, 2, len(partial3), "missing messages: %v", prettyPrintMessages(partial3))

	require.Equal(t, int64(0), q.idGen, "id generator resets on empty")

	// Should get nothing
	partial5 := q.GetBroadcasts(3, 80)
	require.Equal(t, 0, len(partial5), "missing messages: %v", prettyPrintMessages(partial5))

	require.Equal(t, int64(0), q.idGen, "id generator resets on empty")
}

func prettyPrintMessages(msgs [][]byte) []string {
	var out []string
	for _, msg := range msgs {
		out = append(out, "'"+string(msg)+"'")
	}
	return out
}

func TestTransmitLimited_Prune(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)

	// 18 bytes per message
	q.QueueBroadcast(&MemberlistBroadcast{"test", []byte("1. this is a test."), ch1})
	q.QueueBroadcast(&MemberlistBroadcast{"foo", []byte("2. this is a test."), ch2})
	q.QueueBroadcast(&MemberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&MemberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// Keep only 2
	q.Prune(2)

	require.Equal(t, 2, q.NumQueued())

	// Should notify the first two
	select {
	case <-ch1:
	default:
		t.Fatalf("expected invalidation")
	}
	select {
	case <-ch2:
	default:
		t.Fatalf("expected invalidation")
	}

	dump := q.OrderedView(true)

	if dump[0].B.(*MemberlistBroadcast).Node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[1].B.(*MemberlistBroadcast).Node != "baz" {
		t.Fatalf("missing baz")
	}
}

func TestTransmitLimited_ordering(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	insert := func(name string, transmits int) {
		q.queueBroadcast(&MemberlistBroadcast{name, []byte(name), make(chan struct{})}, transmits)
	}

	insert("node0", 0)
	insert("node1", 10)
	insert("node2", 3)
	insert("node3", 4)
	insert("node4", 7)

	dump := q.OrderedView(true)

	if dump[0].transmits != 10 {
		t.Fatalf("bad val %v, %d", dump[0].B.(*MemberlistBroadcast).Node, dump[0].transmits)
	}
	if dump[1].transmits != 7 {
		t.Fatalf("bad val %v, %d", dump[7].B.(*MemberlistBroadcast).Node, dump[7].transmits)
	}
	if dump[2].transmits != 4 {
		t.Fatalf("bad val %v, %d", dump[2].B.(*MemberlistBroadcast).Node, dump[2].transmits)
	}
	if dump[3].transmits != 3 {
		t.Fatalf("bad val %v, %d", dump[3].B.(*MemberlistBroadcast).Node, dump[3].transmits)
	}
	if dump[4].transmits != 0 {
		t.Fatalf("bad val %v, %d", dump[4].B.(*MemberlistBroadcast).Node, dump[4].transmits)
	}
}
