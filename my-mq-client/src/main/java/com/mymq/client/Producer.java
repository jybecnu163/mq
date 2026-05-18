package com.mymq.client;

import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    private final MQClient client;

    public Producer(MQClient client) {
        this.client = client;
    }

    public void send(Message msg) throws ExecutionException, InterruptedException {
        Message response = client.send(msg).get();
        if (!(response.getCommand() == Command.RESPONSE && "OK".equals(response.getBody()))) {
            log.warn("Producer: send failed:{}", msg.getRequestId());
        }
    }

    public void send(Message msg, String tags) throws ExecutionException, InterruptedException {
//        Message msg = new Message(Command.SEND, topic, body);
        msg.setTags(tags);
        Message response = client.send(msg).get();
        if (!(response.getCommand() == Command.RESPONSE && "OK".equals(response.getBody()))) {
            log.warn("Producer: send failed:{},{}", msg, tags);
        }
    }
}