package com.mymq.common.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Message {
    private String command;
    private String requestId;
    private String topic;
    private String body;
    private Map<String, String> headers = new HashMap<>();

    public Message() {}

    public Message(Command command, String topic, String body) {
        this.command = command.name();
        this.topic = topic;
        this.body = body;
        this.requestId = UUID.randomUUID().toString();
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
}