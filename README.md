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
Refute gossip
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
    
    sendBuf = EncryptMsg (1 byte) + messageLength (4 byte) + sendBuf + stream_Label (optional)
    
            
    
    
另外:如果有Label
    HasLabelMsg (1 byte) + LabelSize (1 byte) +  LabelData (LabelSize byte) + sendBuf
    
UDP 
    HasLabelMsg (1 byte) + LabelSize (1 byte) +  LabelData (LabelSize byte) + sendBuf
    ----->  sendBuf 通过 私钥、label 解密
    buf = HasCrcMsg (1 byte) + crcSum(4 byte) + msgType(1 byte) + msg
    
    
    
    msg:
        CompoundMsg + len(msgs) uint8 + 每个消息的长度uint16 + 每个消息
    
    
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

BTree

```
tq.Max
tq.Len
tq.Delete
tq.AscendRange
tq.Ascend   按照升序遍历
tq.Descend  按照将序遍历

```

``` Broadcast
c:
    AliveNode                           |<--   Gossip()      定时广播到随机的机器
    DeadNode          ------> Btree  ---|      在主动发包到某个节点的过程中，填充额外的信息
    EncodeBroadcast                     |<--   encodeAndSendMsg  <--  Ping 、Gossip、ProbeNode、Probe 、ProbeNodeByAddr
    Refute

s:
    <------
        //_ = m.Transport.(*NetTransport).RecIngestPacket //往通道发消息
        //_ = m.Transport.(*NetTransport).UdpListen // 往通道发消息
			
    PacketListen   <-----
        HandleIngestPacket
            HandleCommand
                handlePing
                handleIndirectPing
                
    

    
    
    
    
AliveMsg:
    AliveNode  
    Refute    都是将自己还活着的消息存入到BTree中
    等待handlePing、handleIndirectPing、ProbeNode调用; 将BTree中的数据发送到一台指定的机器
    
Gossip
    会定时同步消息
    类似,多条消息会合并
    // a 1,2,3,4
	// 1 --> b
	// 2 --> c
	// 3 --> d
    

Join是建立的TCP链接


以AliveMsg为例
当某节点Create以后,就调用了 AliveNode ,将alive消息放入到了Btree等待广播
加入集群
等待Gossip超时,将信息，随机发往某个节点

也就是某个节点的alive信息会一直在集群中传输,最早将这个信息放进来的是这个节点本身



UserMsg:
c:
    SendUserMsg
    SendToAddress
    SendBestEffort
    SendToUDP
    以及配置了 m.Config.Delegate.GetBroadcasts   任何一个可以广播的行为
    
    程序本身没有发送UserMsg


周期性的向每个节点发送pingMsg
    a (ProbeNode)--->1、PingMsg---> b(handlePing) --->3、IndirectPingReq ---> c(handleIndirectPing)
        ↑                               ↓          |->3、IndirectPingReq ---> d(handleIndirectPing)
        ↑                               ↓          |->3、IndirectPingReq ---> d(handleIndirectPing)
        ↑<----     2、AckRespMsg     <---↓          |->3、IndirectPingReq ---> e(handleIndirectPing)  --->3.1、PingMsg---> b(handlePing) 
                                                                         ↓---->1、收到ack               ↑<-    AckRespMsg    <-↓
                                    a(handleNack) <-----------------     ↓---->2、没有收到ack,向A返回NACK消息      
                                             
       --->SendPingAndWaitForAck---> true return  \ false awarenessDelta+?    
       --->SuspectNode

```