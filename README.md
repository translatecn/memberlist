# memberlist [![GoDoc](https://godoc.org/github.com/hashicorp/memberlist?status.png)](https://godoc.org/github.com/hashicorp/memberlist) [![CircleCI](https://circleci.com/gh/hashicorp/memberlist.svg?style=svg)](https://circleci.com/gh/hashicorp/memberlist)

memberlist 是一个 Go 库，它使用基于 gossip 的协议来管理集群成员和成员故障检测。 这种库的用例影响深远：所有分布式系统都需要成员资格，而 memberlist 是管理集群成员资格和节点故障检测的可重用解决方案。
memberlist 最终是一致的，但平均而言收敛速度很快。它的收敛速度可以通过协议上的各种旋钮进行大量调整。通过尝试通过多条路由与潜在的死节点进行通信，检测到节点故障并部分容忍网络分区。

## Usage

Memberlist 使用起来非常简单。一个例子如下所示：

```go
/* Create the initial memberlist from a safe configuration.
   Please reference the godoc for other default config types.
   http://godoc.org/github.com/hashicorp/memberlist#Config
*/
list, err := memberlist.Create(memberlist.DefaultLocalConfig())
if err != nil {
panic("Failed to create memberlist: " + err.Error())
}

// Join an existing cluster by specifying at least one known member.
n, err := list.Join([]string{"1.2.3.4"})
if err != nil {
panic("Failed to join cluster: " + err.Error())
}

// Ask for members of the cluster
for _, member := range list.Members() {
fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
}

// Continue doing whatever you need, memberlist will maintain membership
// information in the background. Delegates can be used for receiving
// events when members join or leave.
```

memberlist 最困难的部分是配置它，因为它有许多可用的旋钮来调整状态传播延迟和收敛时间。 Memberlist
提供了一个默认配置，它提供了一个良好的起点，但在谨慎方面会犯错误，选择为更高的收敛性而优化的值，但会以更高的带宽使用为代价。 For complete documentation, see the
associated [Godoc](http://godoc.org/github.com/hashicorp/memberlist).

``` 
UDP gossip 
TCP push/pull
refute gossip
```

### Node Change

```

StateAlive 
    m.AliveNode         某节点存活
StateLeft
    m.DeadNode          节点死亡
StateDead
    m.SuspectNode       质疑节点是否真的Dead
StateSuspect
    m.SuspectNode

```

```
m.gossip
m.PushPull
m.probeNode
```

### Join

```
sendAndReceiveState()
---> 
    buf = PushPullMsg (1 byte) + PushPullHeader + localNodes + userData
    
    sendBuf = CompressMsg (1 byte) + buf
    
    sendBuf = EncryptMsg (1 byte) + messageLength (4 byte) + sendBuf + stream_label (optional)
    
    
    
    
另外:如果有label
    HasLabelMsg (1 byte) + LabelSize (1 byte) +  LabelData (LabelSize byte) + sendBuf
    
    
```

```
streamLabel 只有在 HasLabelMsg 类型下有





PingMsg
PushPullMsg
    -   join 时触发
UserMsg
```

### issue

1、StateDead与StateLeft的区别


2、广播出去的包，server怎么处理






