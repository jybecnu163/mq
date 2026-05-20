package com.minmq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {
    private Command command;
    private String requestId;
    private String topic;
    private String group;
    private long pullOffset = -1;
    private Map<String, String> headers = new HashMap<>();
    private long delayMs = 0;

    // body 改为 byte[]，统一承载消息体
    private byte[] body;
    // 编码类型
    private BodyCodec bodyCodec = BodyCodec.TEXT;  // 默认为文本，兼容旧数据
    // 标签保留字符串
    private String tags;

    // 消费者相关
    private String subscribeTag;
    private long startTime = 0;
    private int maxMessages = 1;
    private List<MessagePayload> messages;
    private List<MessagePayload> payloads;

    // response返回的信息字段
    private String info;

    // 构造方法
    public Message() {
    }

    public Message(Command command, String topic, byte[] body) {
        if (topic.contains("-")) {
            throw new RuntimeException("!!! topic can't contain -");
        }
        this.command = command;
        this.topic = topic;
        this.body = body;
        this.requestId = UUID.randomUUID().toString();
    }

    public Message(Command command, String topic, byte[] body, BodyCodec bodyCodec) {
        if (topic.contains("-")) {
            throw new RuntimeException("!!! topic can't contain -");
        }
        this.command = command;
        this.topic = topic;
        this.body = body;
        this.bodyCodec = bodyCodec;
        this.requestId = UUID.randomUUID().toString();
    }

    public Message(String topic, byte[] body) {
        this.command = Command.RESPONSE;

        this.topic = topic;
        this.body = body;
    }

    public Message(String topic, String info) {
        this.command = Command.RESPONSE;

        this.topic = topic;
        this.info = info;
    }

    // 新增构造方法，方便创建延时消息
    public static Message createDelay(String topic, byte[] body, long delayMs) {
        if (topic.contains("-")) {
            throw new RuntimeException("!!! topic can't contain -");
        }
        Message msg = new Message(Command.SEND, topic, body);
        msg.delayMs = delayMs;
        return msg;
    }

    public static Message createDelay(String topic, byte[] body, BodyCodec bodyCodec, long delayMs) {
        if (topic.contains("-")) {
            throw new RuntimeException("!!! topic can't contain -");
        }
        Message msg = new Message(Command.SEND, topic, body, bodyCodec);
        msg.delayMs = delayMs;
        return msg;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
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

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public long getPullOffset() {
        return pullOffset;
    }

    public void setPullOffset(long pullOffset) {
        this.pullOffset = pullOffset;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public BodyCodec getBodyCodec() {
        return bodyCodec;
    }

    public void setBodyCodec(BodyCodec bodyCodec) {
        this.bodyCodec = bodyCodec;
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

    public List<MessagePayload> getPayloads() {
        return payloads;
    }

    public void setPayloads(List<MessagePayload> payloads) {
        this.payloads = payloads;
    }

    // 内部类：单条消息负载
    public static class MessagePayload {
        private String topic;
        private byte[] body;         // 改为 byte[]
        private BodyCodec bodyCodec = BodyCodec.TEXT;
        private String tags;


        public MessagePayload() {
        }

        public MessagePayload(String topic, byte[] body, BodyCodec codec, String tags) {
            this.topic = topic;
            this.body = body;
            this.bodyCodec = codec;
            this.tags = tags;
        }

        // 兼容旧构造（String body）
        public MessagePayload(String topic, byte[] bodyStr, String tags) {
            this(topic, bodyStr, BodyCodec.TEXT, tags);
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public byte[] getBody() {
            return body;
        }

        public void setBody(byte[] body) {
            this.body = body;
        }

        public BodyCodec getBodyCodec() {
            return bodyCodec;
        }

        public void setBodyCodec(BodyCodec bodyCodec) {
            this.bodyCodec = bodyCodec;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
        }
    }
}