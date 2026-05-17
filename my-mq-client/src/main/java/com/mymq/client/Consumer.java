package com.mymq.client;

import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;

import java.util.concurrent.ExecutionException;

public class Consumer {
    private final MQClient client;
    private final String topic;
    private final String group;
    private long lastPullOffset = -1;   // 记录最后拉取消息的偏移，用于 ACK
    private String subscribeTag;  // 新增订阅标签

    public Consumer(MQClient client, String topic, String group) {
        this(client, topic, group, null); // 默认不过滤
    }

    public Consumer(MQClient client, String topic, String group, String subscribeTag) {
        this.client = client;
        this.topic = topic;
        this.group = group;
        this.subscribeTag = subscribeTag;
    }

    /**
     * 拉取一条消息，如果没有新消息返回 null
     */
    public Message pull() throws ExecutionException, InterruptedException {
        Message request = new Message(Command.PULL, topic, "");
        request.setGroup(group);                // 设置消费者组
        request.setSubscribeTag(subscribeTag);  // 设置订阅标签
        Message response = client.send(request).get();

        if (response.getBody() == null) {
            return null;
        }

        // 记录服务端返回的逻辑偏移量，用于后续 ACK
        lastPullOffset = response.getPullOffset();

        // 构造一条纯业务消息返回给调用方
        Message result = new Message();
        result.setBody(response.getBody());
        result.setTopic(topic);
        result.setPullOffset(lastPullOffset);
        return result;
    }

    /**
     * 确认消息已处理，推动消费进度
     */
    public void ack() throws ExecutionException, InterruptedException {
        if (lastPullOffset < 0) return;

        Message ackMsg = new Message(Command.ACK, topic, "");
        ackMsg.setGroup(group);
        ackMsg.setPullOffset(lastPullOffset);
        client.send(ackMsg).get();               // ACK 不需要关心响应
        lastPullOffset = -1;
    }

    public String getSubscribeTag() {
        return subscribeTag;
    }

    public void setSubscribeTag(String subscribeTag) {
        this.subscribeTag = subscribeTag;
    }
}