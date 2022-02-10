package broadcast_tree

import (
	"reflect"
	"testing"
)

func TestMemberlistBroadcast_Invalidates(t *testing.T) {
	m1 := &MemberlistBroadcast{"test", nil, nil}
	m2 := &MemberlistBroadcast{"foo", nil, nil}

	if m1.Invalidates(m2) || m2.Invalidates(m1) {
		t.Fatalf("unexpected invalidation")
	}

	if !m1.Invalidates(m1) {
		t.Fatalf("expected invalidation")
	}
}

func TestMemberlistBroadcast_Message(t *testing.T) {
	m1 := &MemberlistBroadcast{"test", []byte("test"), nil}
	msg := m1.Message()
	if !reflect.DeepEqual(msg, []byte("test")) {
		t.Fatalf("messages do not match")
	}
}
