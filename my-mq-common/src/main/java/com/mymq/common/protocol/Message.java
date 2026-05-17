package com.mymq.common.protocol;

import java.util.HashMap;
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
}