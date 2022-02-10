package queue_broadcast

type UniqueBroadcast interface {
	Broadcast
	UniqueBroadcast()
}
