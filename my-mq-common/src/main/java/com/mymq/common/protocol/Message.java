package com.mymq.common.protocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Message {
    private String command;
    private String requestId;
    private String topic;
    private String body;
    // 消费者组，用于区分不同消费组进度
    private String group;
    // 消费进度偏移量，PULL 时服务端返回，ACK 时客户端回传
    private long pullOffset = -1;
    private Map<String, String> headers = new HashMap<>();
    // 延时消息专用字段（单位：毫秒）
    private long delayMs = 0;

    // 新增：消息标签（生产者设置）
    private String tags;
    // 新增：消费者订阅的标签（消费者拉取时使用）
    private String subscribeTag;
    private long startTime = 0;   // 起始消费时间（毫秒时间戳），0 表示从最早开始

    // 批量发送相关
    private List<MessagePayload> payloads;   // 批量发送时使用
    // 批量拉取相关
    private int maxMessages = 1;             // 消费者最大拉取条数
    private List<MessagePayload> messages;   // 服务端返回的批量消息

    public Message() {
    }

    public Message(Command command, String topic, String body) {
        this.command = command.name();
        this.topic = topic;
        this.body = body;
        this.requestId = UUID.randomUUID().toString();
    }

    // 新增构造方法，方便创建延时消息
    public static Message createDelay(String topic, String body, long delayMs) {
        Message msg = new Message(Command.SEND, topic, body);
        msg.delayMs = delayMs;
        return msg;
    }

    public Command getCommand() {
        return Command.valueOf(command);
    }

    public void setCommand(Command command) {
        this.command = command.name();
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public long getPullOffset() {
        return pullOffset;
    }

    public void setPullOffset(long pullOffset) {
        this.pullOffset = pullOffset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
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

    public void setCommand(String command) {
        this.command = command;
    }

    public List<MessagePayload> getPayloads() {
        return payloads;
    }

    public void setPayloads(List<MessagePayload> payloads) {
        this.payloads = payloads;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    public List<MessagePayload> getMessages() {
        return messages;
    }

    public void setMessages(List<MessagePayload> messages) {
        this.messages = messages;
    }

    // 内部类：单条消息负载
    public static class MessagePayload {
        private String topic;
        private String body;
        private String tags;

        public MessagePayload() {
        }

        public MessagePayload(String topic, String body, String tags) {
            this.topic = topic;
            this.body = body;
            this.tags = tags;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
        }

        @Override
        public String toString() {
            return "MessagePayload{" +
                    "topic='" + topic + '\'' +
                    ", body='" + body + '\'' +
                    ", tags='" + tags + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Message{" +
                "command='" + command + '\'' +
                ", requestId='" + requestId + '\'' +
                ", topic='" + topic + '\'' +
                ", body='" + body + '\'' +
                ", group='" + group + '\'' +
                ", pullOffset=" + pullOffset +
                ", headers=" + headers +
                ", delayMs=" + delayMs +
                ", tags='" + tags + '\'' +
                ", subscribeTag='" + subscribeTag + '\'' +
                ", startTime=" + startTime +
                ", payloads=" + payloads +
                ", maxMessages=" + maxMessages +
                ", messages=" + messages +
                '}';
    }
}