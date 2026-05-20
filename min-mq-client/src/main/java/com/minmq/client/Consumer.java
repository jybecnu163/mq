package com.minmq.client;

import com.minmq.common.protocol.AckMode;
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
    // 新增：确认模式，默认手动
    private AckMode ackMode = AckMode.MANUAL;

    public Consumer(MQClient client, String topic, String group) {
        this(client, topic, group, null); // 默认不过滤
    }

    public Consumer(MQClient client, String topic, String group, String subscribeTag) {
        if (topic.contains("-") || group.contains("-")) {
            throw new RuntimeException("!!! topic and group can't contain -");
        }
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
        Message request = new Message(Command.PULL, topic, null);
        request.setGroup(group);
        request.setSubscribeTag(subscribeTag);
        request.setStartTime(startTime);
        request.setMaxMessages(maxMessages);     // 传递上限
        try {
            // 将确认模式告知服务端（服务器即时确认模式需要）
            if (ackMode == AckMode.SERVER_IMMEDIATE) {
                request.getHeaders().put("X-Ack-Mode", String.valueOf(ackMode.getValue()));
            }

            Message response = client.send(request).get();
            if (response.getCommand() == Command.RESPONSE && response.getMessages() != null) {
                lastPullOffset = response.getPullOffset();   // 记录最后一条的偏移量，用于 ACK
                // 客户端自动确认模式
                if (ackMode == AckMode.CLIENT_AUTO && lastPullOffset >= 0) {
                    autoAck();
                }
                return response.getMessages();               // 批量消息列表
            }
            return Collections.emptyList();
        } catch (ExecutionException e) {
            throw new RuntimeException("Pull failed", e.getCause());
        }
    }

    /**
     * 拉取一条消息，如果没有新消息返回 null
     */
    public Message pull() throws InterruptedException {
        Message request = new Message(Command.PULL, topic, null);
        request.setGroup(group);                // 设置消费者组
        request.setSubscribeTag(subscribeTag);  // 设置订阅标签
        request.setStartTime(startTime);

        Message response = null;
        int retries = 0;
        final int maxRetries = 20;
        while (retries < maxRetries) {
            try {
                // 将确认模式告知服务端（服务器即时确认模式需要）
                if (ackMode == AckMode.SERVER_IMMEDIATE) {
                    request.getHeaders().put("X-Ack-Mode", String.valueOf(ackMode.getValue()));
                }

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

        result.setBodyCodec(response.getBodyCodec());
        result.setBody(response.getBody());

        result.setPullOffset(lastPullOffset);
        result.setTags(response.getTags());  // 如果需要的话，也可以透传 tags

        // 客户端自动确认模式
        if (ackMode == AckMode.CLIENT_AUTO) {
            autoAck();
        }

        return result;
    }

    private void autoAck() {
        try {
            ack();
        } catch (Exception e) {
            log.error("Auto ack failed", e);
        }
    }

    /**
     * 确认本次批量拉取的所有消息（提交最后一条的偏移量）
     */
    public void ackLast() throws ExecutionException, InterruptedException {
        ack();   // 无参 ack() 内部使用 lastPullOffset 发送确认
    }

    /**
     * 手动确认（如果启用了自动确认，该方法仍可安全调用，会忽略重复确认）
     */
    public void ack() throws ExecutionException, InterruptedException {
        if (lastPullOffset < 0) {
            log.debug("No valid offset to ack");
            return;
        }
        // 服务器即时确认模式下无需发送ACK，因为服务端已经提交了
        if (ackMode == AckMode.SERVER_IMMEDIATE) {
            log.debug("Server immediate mode, skipping client ack");
            lastPullOffset = -1; // 清空，防止重复调用
            return;
        }
        Message ackMsg = new Message(Command.ACK, topic, null);
        ackMsg.setGroup(group);
        ackMsg.setPullOffset(lastPullOffset);
        client.send(ackMsg).get();
        lastPullOffset = -1; // 确认后清空
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

    public void setAckMode(AckMode ackMode) {
        this.ackMode = ackMode;
    }

}