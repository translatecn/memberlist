package memberlist

import (
	"Compress/lzw"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

const PushPullScaleThreshold = 32

const (
	// Constant litWidth 2-8
	lzwLitWidth = 8
)

// Decode 解码
func Decode(buf []byte, out interface{}) error {
	r := bytes.NewReader(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode 编码
func Encode(msgType MessageType, in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(msgType))
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// 放回0~n之间的随机值
func RandomOffset(n int) int {
	if n == 0 {
		return 0
	}
	return int(rand.Uint32() % uint32(n))
}

// SuspicionTimeout computes the timeout that should be used when
// a node is Suspected
func SuspicionTimeout(suspicionMult, n int, interval time.Duration) time.Duration {
	nodeScale := math.Max(1.0, math.Log10(math.Max(1.0, float64(n))))
	// multiply by 1000 to keep some precision because time.Duration is an int64 type
	timeout := time.Duration(suspicionMult) * time.Duration(nodeScale*1000) * interval / 1000
	return timeout
}

// ShuffleNodes randomly shuffles the input Nodes using the Fisher-Yates shuffle
func ShuffleNodes(nodes []*NodeState) {
	n := len(nodes)
	rand.Shuffle(n, func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
}

// push\pull 间隔，需要随着集群规模变化。避免集群增大，网络阻塞
func PushPullScale(interval time.Duration, n int) time.Duration {
	// 节点数小于32个,时间不变
	if n <= PushPullScaleThreshold {
		return interval
	}

	multiplier := math.Ceil(math.Log2(float64(n))-math.Log2(PushPullScaleThreshold)) + 1.0
	return time.Duration(multiplier) * interval
}

// MoveDeadNodes 移除Dead\left节点 超过一个gossipToTheDeadTime间隔的;并返回当前依然存活的节点个数
func MoveDeadNodes(nodes []*NodeState, gossipToTheDeadTime time.Duration) int {
	numDead := 0
	n := len(nodes)
	// 【a,b,c,d,e,f,g,h,j,k,l】
	for i := 0; i < n-numDead; i++ {
		if !nodes[i].DeadOrLeft() {
			continue
		}

		// 判断节点的Dead超时有没有到
		if time.Since(nodes[i].StateChange) <= gossipToTheDeadTime {
			continue
		}

		// 将节点移至最后
		nodes[i], nodes[n-numDead-1] = nodes[n-numDead-1], nodes[i] // 存活节点、当前节点
		numDead++
		i--
	}
	return n - numDead
}

// KRandomNodes is used to select up to k random Nodes, excluding any Nodes where
// the exclude function returns true. It is possible that less than k Nodes are
// returned.
func KRandomNodes(k int, nodes []*NodeState, exclude func(*NodeState) bool) []Node {
	n := len(nodes)
	kNodes := make([]Node, 0, k)
OUTER:
	// Probe up to 3*n times, with large n this is not necessary
	// since k << n, but with small n we want search to be
	// exhaustive
	for i := 0; i < 3*n && len(kNodes) < k; i++ {
		// Get random NodeState
		idx := RandomOffset(n)
		state := nodes[idx]

		// Give the filter a shot at it.
		if exclude != nil && exclude(state) {
			continue OUTER
		}

		// Check if we have this node already
		for j := 0; j < len(kNodes); j++ {
			if state.Node.Name == kNodes[j].Name {
				continue OUTER
			}
		}

		// Append the node
		kNodes = append(kNodes, state.Node)
	}
	return kNodes
}

// MakeCompoundMessages takes a list of messages and packs
// them into one or multiple messages based on the limitations
// of compound messages (255 messages each).
func MakeCompoundMessages(msgs [][]byte) []*bytes.Buffer {
	const maxMsgs = 255
	bufs := make([]*bytes.Buffer, 0, (len(msgs)+(maxMsgs-1))/maxMsgs)

	for ; len(msgs) > maxMsgs; msgs = msgs[maxMsgs:] {
		bufs = append(bufs, MakeCompoundMessage(msgs[:maxMsgs]))
	}
	if len(msgs) > 0 {
		bufs = append(bufs, MakeCompoundMessage(msgs))
	}

	return bufs
}

// MakeCompoundMessage takes a list of messages and generates
// a single compound message containing all of them
func MakeCompoundMessage(msgs [][]byte) *bytes.Buffer {
	// Create a local buffer
	buf := bytes.NewBuffer(nil)

	// Write out the type
	buf.WriteByte(uint8(CompoundMsg))

	// Write out the number of message
	buf.WriteByte(uint8(len(msgs)))

	// Add the message lengths
	for _, m := range msgs {
		binary.Write(buf, binary.BigEndian, uint16(len(m)))
	}

	// Append the messages
	for _, m := range msgs {
		buf.Write(m)
	}

	return buf
}

// DecodeCompoundMessage splits a compound message and returns
// the slices of individual messages. Also returns the number
// of truncated messages and any potential error
func DecodeCompoundMessage(buf []byte) (trunc int, parts [][]byte, err error) {
	if len(buf) < 1 {
		err = fmt.Errorf("missing compound length byte")
		return
	}
	numParts := int(buf[0])
	buf = buf[1:]

	// Check we have enough bytes
	if len(buf) < numParts*2 {
		err = fmt.Errorf("truncated len slice")
		return
	}

	// Decode the lengths
	lengths := make([]uint16, numParts)
	for i := 0; i < numParts; i++ {
		lengths[i] = binary.BigEndian.Uint16(buf[i*2 : i*2+2])
	}
	buf = buf[numParts*2:]

	// Split each message
	for idx, msgLen := range lengths {
		if len(buf) < int(msgLen) {
			trunc = numParts - idx
			return
		}

		// Extract the slice, seek past on the buffer
		slice := buf[:msgLen]
		buf = buf[msgLen:]
		parts = append(parts, slice)
	}
	return
}

// 压缩
func CompressPayload(inp []byte) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	Compressor := lzw.NewWriter(&buf, lzw.LSB, lzwLitWidth)

	_, err := Compressor.Write(inp)
	if err != nil {
		return nil, err
	}

	if err := Compressor.Close(); err != nil {
		return nil, err
	}

	c := Compress{
		Algo: lzwAlgo,
		Buf:  buf.Bytes(),
	}
	return Encode(CompressMsg, &c)
}

// DeCompressPayload 解压缩
func DeCompressPayload(msg []byte) ([]byte, error) {
	var c Compress
	if err := Decode(msg, &c); err != nil {
		return nil, err
	}
	return DeCompressBuffer(&c)
}

// DeCompressBuffer is used to deCompress the buffer of
// a single Compress message, handling multiple algorithms
func DeCompressBuffer(c *Compress) ([]byte, error) {
	// Verify the algorithm
	if c.Algo != lzwAlgo {
		return nil, fmt.Errorf("Cannot deCompress unknown algorithm %d", c.Algo)
	}

	// Create a unCompressor
	uncomp := lzw.NewReader(bytes.NewReader(c.Buf), lzw.LSB, lzwLitWidth)
	defer uncomp.Close()

	// Read all the data
	var b bytes.Buffer
	_, err := io.Copy(&b, uncomp)
	if err != nil {
		return nil, err
	}

	// Return the unCompressed bytes
	return b.Bytes(), nil
}
