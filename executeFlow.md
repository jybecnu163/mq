#  1. 客户端：生产者发送消息
   用户在业务代码中调用 Producer.send()，通过 Netty 自定义协议将消息异步发送给 Broker。

Producer.java

```java
public void send(Message msg) throws InterruptedException {
// ...
   Message response = client.send(msg).get();  // 同步等待响应
// ...
}
```

这里 msg 是 com.minmq.common.protocol.Message，里面包含命令 SEND、主题、消息体、标签等信息。client.send(msg) 返回
CompletableFuture<Message>，调用 get() 会阻塞直到收到 Broker 的响应。

MQClient.java

```java
public CompletableFuture<Message> send(Message msg) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    Channel ch = this.channel;
// ...
    pendingRequests.put(msg.getRequestId(), future);
    ch.writeAndFlush(msg).addListener((ChannelFutureListener) writeFuture -> {
        if (!writeFuture.isSuccess()) {
            pendingRequests.remove(msg.getRequestId());
            future.completeExceptionally(writeFuture.cause());
        }
    });
    return future;
}
```

每个消息都会生成一个唯一的 requestId，用来匹配响应。

pendingRequests 是一个 ConcurrentHashMap，存放 requestId -> Future 的映射。

消息通过 Netty 的 writeAndFlush 发送出去，如果发送失败则立即让 Future 失败。

#  2. 网络传输与协议编解码
   Netty 管道中配置了以下处理器：

BrokerServer.java (客户端同理)

```java
    ch.pipeline()
         .addLast(new LengthFieldBasedFrameDecoder(1024*1024, 0,4,0,4))
         .addLast(new LengthFieldPrepender(4))
        .addLast(new MessageCodec.Encoder())
        .addLast(new MessageCodec.Decoder())
        .addLast(new MessageHandler(store));
```

LengthFieldBasedFrameDecoder 和
LengthFieldPrepender 处理
TCP 粘包/拆包，我们使用的是 4字节长度头。

MessageCodec.Encoder/
Decoder 负责
Java 对象与字节数组的互相转换，
当前使用 Jackson
JSON 序列化。

#  3. Broker 接收并处理 SEND 命令
   当消息到达 Broker 时，MessageHandler.channelRead0 根据命令类型分派到 handleSend。

MessageHandler.java

```java

@Override
protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
    Command cmd = msg.getCommand();
    switch (cmd) {
        case SEND:
            if (msg.getDelayMs() > 0) {
                handleDelaySend(ctx, msg);
            } else {
                handleSend(ctx, msg);
            }
            break;
// ...
    }
}
```

handleSend 是处理普通消息（非延时）的核心方法：

MessageHandler.java (handleSend 部分代码)

```java
private void handleSend(ChannelHandlerContext ctx, Message msg) {
    try {
        String topic = msg.getTopic();
        String body = msg.getBody();
        String tags = msg.getTags();
        long offset = store.appendMessage(topic, body, tags);
        System.out.println("Message stored: topic=" + topic + ", offset=" + offset);

        // 追加索引到所有已注册的消费者组
        for (Map.Entry<String, ConsumeIndexManager> entry : store.getAllGroupIndexes().entrySet()) {
            if (entry.getKey().startsWith(topic + "-")) {
                entry.getValue().appendOffset(offset, System.currentTimeMillis());
            }
        }

        // 唤醒长轮询消费者
        wakeupPendingPulls(topic);

        // 回复生产者 ACK
        Message ack = new Message(Command.RESPONSE, topic, "OK");
        ack.setRequestId(msg.getRequestId());
        ctx.writeAndFlush(ack);
    } catch (Exception e) {
        // 错误处理...
    }
}
```

整个处理流程可以分为四步：

3.1 消息持久化（写入 MessageLog）
store.appendMessage(topic, body, tags) 最终调用 MessageLog.append() 将消息序列化后追加到磁盘文件。

MessageLog.java (append)

```java
public synchronized long append(String topic, String body, String tags, long timestamp) throws Exception {
    if (activeSegment == null || activeSegment.size() >= maxSegmentSize) {
        rollNewSegment();
    }
    MessageEntry entry = new MessageEntry(topic, body, tags, timestamp);
    byte[] data = MAPPER.writeValueAsBytes(entry);
    int length = data.length;

    ByteBuffer buf = ByteBuffer.allocate(4 + length);
    buf.putInt(length);
    buf.put(data);
    buf.flip();

    long offset = activeSegment.append(buf);
    return offset;
}
```

首先检查当前活动段是否达到大小上限，若超过则创建新段。

构造 MessageEntry 对象，包含主题、体、标签和时间戳。

序列化为 JSON 字节数组，前面加上 4 字节长度头，然后调用 Segment.append 写入。

返回的 offset 是全局物理偏移量，即该消息在日志文件中的起始位置。

Segment.java (append)

```java
synchronized long append(ByteBuffer buf) throws IOException {
    long offset = getEndOffset();   // 全局偏移量
    channel.write(buf);
    wrotePosition += buf.limit();
    return offset;
}
```

这里使用 FileChannel.write 实现顺序追加，性能极高。

3.2 追加消费者索引
消息持久化后，需要让订阅了该 Topic 的消费者组能够拉取到它。ConsumeIndexManager 为每个 (topic, consumerGroup)
维护一个索引文件，记录每条消息的物理偏移量。

BrokerStore.java (部分代码)

```java
public long appendMessage(String topic, String body, String tags) throws Exception {
    long now = System.currentTimeMillis();
    long offset = messageLog.append(topic, body, tags, now);
    for (Map.Entry<String, ConsumeIndexManager> entry : groupIndexes.entrySet()) {
        if (entry.getKey().startsWith(topic + "-")) {
            entry.getValue().appendOffset(offset, now);
        }
    }
    return offset;
}
```

ConsumeIndexManager.java (appendOffset)

```java
public synchronized void appendOffset(long physicalOffset, long timestamp) throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(physicalOffset);
    buf.flip();
    indexChannel.write(buf, indexCount * 8);
    timeIndex.add(new TimeOffsetEntry(timestamp, physicalOffset, indexCount));
    indexCount++;
}
```

将物理偏移量写入索引文件（按 8 字节顺序存储）。

同时在内存中的 timeIndex 列表记录时间戳与偏移量的对应关系，支持按时间定位。

3.3 唤醒挂起的长轮询消费者
如果此时有消费者正在等待新消息（长轮询），需要立即通知它们。

MessageHandler.java (wakeupPendingPulls)

```java
private void wakeupPendingPulls(String topic) {
    List<PendingPull> list = pendingPulls.get(topic);
    if (list == null || list.isEmpty()) return;

    List<PendingPull> snapshot = new ArrayList<>(list);
    for (PendingPull pending : snapshot) {
        if (list.remove(pending)) {
            if (pending.timeoutFuture != null && !pending.timeoutFuture.isDone()) {
                pending.timeoutFuture.cancel(false);
            }
            // 读取最新消息并响应
            try {
                ConsumeIndexManager indexMgr = store.getOrCreateIndex(pending.topic, pending.group);
                long offset = indexMgr.peekNextOffset();
                if (offset >= 0) {
                    String body = store.readMessage(offset);
                    Message resp = new Message(Command.RESPONSE, pending.topic, body);
                    resp.setRequestId(pending.requestId);
                    resp.setPullOffset(indexMgr.getConsumerOffset());
                    pending.ctx.writeAndFlush(resp);
                }
            } catch (Exception e) { /* ... */ }
        }
    }
}
```

从 pendingPulls 中取出该 Topic 的所有挂起请求。

取消超时任务，立即为它们拉取最新消息并写回响应。

3.4 返回 ACK 给生产者
最后，Broker 向生产者发送一个 RESPONSE 确认，携带 "OK" 表示消息已可靠存储。生产者侧的 Future.get() 解除阻塞，整个发送过程结束。

#  4. 消费者拉取消息的过程（简要）
   消费者通过 Consumer.pull() 发送 PULL 命令。Broker 在 handlePull 中：

获取或创建 ConsumeIndexManager。

检查下一条消息的物理偏移量。

通过 MessageLog.readMessage(offset) 读取消息内容。

根据消费者订阅的 Tag 进行过滤，不匹配则跳过并自动 ACK 该偏移量。

将消息返回给消费者，消费者处理完后调用 ack() 推进消费进度。

整体数据流总结

```text
Producer.send()
└─> MQClient.send() [Netty writeAndFlush]
│
v
Broker MessageHandler.handleSend()
├─> BrokerStore.appendMessage()
│     └─> MessageLog.append()  ───> 磁盘分段顺序写
├─> ConsumeIndexManager.appendOffset()  ───> 索引文件 + 时间索引
├─> wakeupPendingPulls()  ───> 唤醒等待的消费者
└─> ctx.writeAndFlush(ACK)  ───> 回复生产者
```

这就是一条消息从诞生到被存储、并准备投递的全过程。整个设计核心是 顺序写磁盘 + 物理偏移量索引 +
多消费者组独立进度，既保证了高性能，又提供了灵活的消费管理。