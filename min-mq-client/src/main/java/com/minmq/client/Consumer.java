package com.minmq.client;

import com.minmq.common.protocol.Command;
import com.minmq.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final MQClient client;
    private final String topic;
    private final String group;
    /**
     * lastPullOffset 是 Consumer 实例的成员变量，而每个 Consumer 对象对应一个特定的 topic + group 组合。因此不同 topic 或不同 group 的消费者拥有不同的 lastPullOffset，天然隔离，不会相互影响。无需全局处理。
     */
    private long lastPullOffset = -1;   // 记录最后拉取消息的偏移，用于 ACK
    private String subscribeTag;  // 新增订阅标签
    private long startTime = 0;   // 毫秒时间戳，0 表示从最早开始
    // 默认每次拉取1条
    private int maxMessages = 1;


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
     * 拉取消息，可能返回多条
     *
     * @return 消息列表（可能为空）
     */
    public List<Message.MessagePayload> pullBatch() throws InterruptedException {
        Message request = new Message(Command.PULL, topic, "");
        request.setGroup(group);
        request.setSubscribeTag(subscribeTag);
        request.setStartTime(startTime);
        request.setMaxMessages(maxMessages);     // 传递上限
        try {
            Message response = client.send(request).get();
            if (response.getCommand() == Command.RESPONSE && response.getMessages() != null) {
                lastPullOffset = response.getPullOffset();   // 记录最后一条的偏移量，用于 ACK
                return response.getMessages();               // 批量消息列表
            }
            return Collections.emptyList();
        } catch (ExecutionException e) {
            throw new RuntimeException("Pull failed", e.getCause());
        }
    }

    /**
     * 确认本次批量拉取的所有消息（提交最后一条的偏移量）
     */
    public void ackLast() throws ExecutionException, InterruptedException {
        ack();   // 无参 ack() 内部使用 lastPullOffset 发送确认
    }

    /**
     * 拉取一条消息，如果没有新消息返回 null
     */
    public Message pull() throws InterruptedException {
        Message request = new Message(Command.PULL, topic, "");
        request.setGroup(group);                // 设置消费者组
        request.setSubscribeTag(subscribeTag);  // 设置订阅标签
        request.setStartTime(startTime);

        Message response = null;
        int retries = 0;
        final int maxRetries = 20;
        while (retries < maxRetries) {
            try {
                response = client.send(request).get();
                retries = maxRetries + 999;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException && "Connection not available".equals(cause.getMessage())) {
                    log.warn("Connection not available, retrying in 3s...");
                    Thread.sleep(3000L * retries++);
                } else {
                    throw new RuntimeException("Pull failed", e);
                }
            }
        }
        if (retries == maxRetries) {
            throw new RuntimeException("Could not pull after " + maxRetries + " retries");
        }

        // 记录服务端返回的逻辑偏移量，用于后续 ACK
        assert response != null;
        lastPullOffset = response.getPullOffset();

        // 构造一条纯业务消息返回给调用方
        Message result = new Message();
        result.setTopic(topic);

        // ---- 新增：传递二进制字段 ----
        byte[] bodyBytes = response.getBodyBytes();
        if (bodyBytes != null) {
            result.setBodyBytes(bodyBytes);
            result.setBodyCodec(response.getBodyCodec());
            result.setBody(null);  // 二进制优先，字符串 body 置空
        } else {
            result.setBody(response.getBody());
        }
        result.setPullOffset(lastPullOffset);
        result.setTags(response.getTags());  // 如果需要的话，也可以透传 tags

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

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setMaxMessages(int max) {
        this.maxMessages = max;
    }
}