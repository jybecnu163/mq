package com.mymq.client;

import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;

import java.util.concurrent.ExecutionException;

public class Producer {
    private final MQClient client;

    public Producer(MQClient client) {
        this.client = client;
    }

    public void send(Message msg) throws ExecutionException, InterruptedException {
        Message response = client.send(msg).get();
        if (response.getCommand() == Command.RESPONSE && "OK".equals(response.getBody())) {
            System.out.println("Producer: message sent successfully");
        } else {
            System.out.println("Producer: send failed");
        }
    }

    public void send(Message msg, String tags) throws ExecutionException, InterruptedException {
//        Message msg = new Message(Command.SEND, topic, body);
        msg.setTags(tags);
        Message response = client.send(msg).get();
        if (response.getCommand() == Command.RESPONSE && "OK".equals(response.getBody())) {
            System.out.println("Producer: message sent successfully with tags=" + tags);
        } else {
            System.out.println("Producer: send failed");
        }
    }
}