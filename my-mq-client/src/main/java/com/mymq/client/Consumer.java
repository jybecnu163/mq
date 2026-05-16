package com.mymq.client;

import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;

import java.util.concurrent.ExecutionException;

public class Consumer {
    private final MQClient client;
    private final String topic;

    public Consumer(MQClient client, String topic) {
        this.client = client;
        this.topic = topic;
    }

    public Message pull() throws ExecutionException, InterruptedException {
        Message request = new Message(Command.PULL, topic, "");
        Message response = client.send(request).get();
        if (response.getBody() == null) {
            return null;
        }
        // 构造一个包含实际消息体的Message返回给调用方
        Message result = new Message();
        result.setBody(response.getBody());
        result.setTopic(topic);
        return result;
    }
}