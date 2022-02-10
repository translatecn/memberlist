package memberlist

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

// General approach is to prefix all packets and streams with the same structure:
//
// magic type byte (244): uint8
// length of Label name:  uint8 (because Labels can't be longer than 255 bytes)
// Label name:            []uint8

// LabelMaxSize 包、流  标签的最大长度
const LabelMaxSize = 255

// AddLabelHeaderToPacket prefixes outgoing packets with the correct header if
// the Label is not empty.
func AddLabelHeaderToPacket(buf []byte, Label string) ([]byte, error) {
	if Label == "" {
		return buf, nil
	}
	if len(Label) > LabelMaxSize {
		return nil, fmt.Errorf("Label %q is too long", Label)
	}

	return makeLabelHeader(Label, buf), nil
}

// RemoveLabelHeaderFromPacket 从提供的包中删除任何标签头，并将其与剩余的包内容一起返回。
func RemoveLabelHeaderFromPacket(buf []byte) (newBuf []byte, Label string, err error) {
	if len(buf) == 0 {
		return buf, "", nil // 不可能有标签
	}

	// [type:byte] [size:byte] [size bytes]
	msgType := MessageType(buf[0])
	if msgType != HasLabelMsg {
		return buf, "", nil
	}

	if len(buf) < 2 {
		return nil, "", fmt.Errorf("cannot Decode Label; packet has been truncated")
	}

	size := int(buf[1])
	if size < 1 {
		return nil, "", fmt.Errorf("Label header cannot be empty when present")
	}

	if len(buf) < 2+size {
		return nil, "", fmt.Errorf("cannot Decode Label; packet has been truncated")
	}

	Label = string(buf[2 : 2+size])
	newBuf = buf[2+size:]

	return newBuf, Label, nil
}

// AddLabelHeaderToStream prefixes outgoing streams with the correct header if
// the Label is not empty.
func AddLabelHeaderToStream(conn net.Conn, Label string) error {
	if Label == "" {
		return nil
	}
	if len(Label) > LabelMaxSize {
		return fmt.Errorf("Label %q is too long", Label)
	}

	header := makeLabelHeader(Label, nil)

	_, err := conn.Write(header)
	return err
}

// RemoveLabelHeaderFromStream
// 如果存在的话，从流的开头删除任何标签头，并将其与删除了该头的最新conn一起返回。
// 请注意，当出现错误时，关闭连接是调用者的责任。
func RemoveLabelHeaderFromStream(conn net.Conn) (net.Conn, string, error) {
	br := bufio.NewReader(conn)

	peeked, err := br.Peek(1)
	if err != nil {
		if err == io.EOF {
			// 此时返回原始的 net.Conn 是安全的，因为它一开始就没有包含任何数据，
			// 所以我们不需要把缓冲区拼接到 conn 中，因为两者都是空的。
			return conn, "", nil
		}
		return nil, "", err
	}

	msgType := MessageType(peeked[0])
	if msgType != HasLabelMsg {
		conn, err = newPeekedConnFromBufferedReader(conn, br, 0)
		return conn, "", err
	}
	peeked, err = br.Peek(2)
	if err != nil {
		if err == io.EOF {
			return nil, "", fmt.Errorf("无法解码标签；流被截断了")
		}
		return nil, "", err
	}

	size := int(peeked[1])
	if size < 1 {
		return nil, "", fmt.Errorf("标签头存在时不能为空")
	}
	peeked, err = br.Peek(2 + size)
	if err != nil {
		if err == io.EOF {
			return nil, "", fmt.Errorf("无法解码标签；流被截断了")
		}
		return nil, "", err
	}

	Label := string(peeked[2 : 2+size])

	conn, err = newPeekedConnFromBufferedReader(conn, br, 2+size)
	if err != nil {
		return nil, "", err
	}

	return conn, Label, nil
}

// newPeekedConnFromBufferedReader 将读取到的数据拼接回conn
// 先从Peeked读取,再从Conn读
func newPeekedConnFromBufferedReader(conn net.Conn, br *bufio.Reader, offset int) (*peekedConn, error) {
	peeked, err := br.Peek(br.Buffered()) // 将所有的数据读取出来
	if err != nil {
		return nil, err
	}

	return &peekedConn{
		Peeked: peeked[offset:],
		Conn:   conn,
	}, nil
}

func makeLabelHeader(Label string, rest []byte) []byte {
	newBuf := make([]byte, 2, 2+len(Label)+len(rest))
	newBuf[0] = byte(HasLabelMsg)
	newBuf[1] = byte(len(Label))
	newBuf = append(newBuf, []byte(Label)...)
	if len(rest) > 0 {
		newBuf = append(newBuf, []byte(rest)...)
	}
	return newBuf
}

func LabelOverhead(Label string) int {
	if Label == "" {
		return 0
	}
	return 2 + len(Label)
}
