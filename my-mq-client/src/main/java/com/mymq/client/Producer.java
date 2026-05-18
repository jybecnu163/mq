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

    public Message send(Message msg) throws InterruptedException {
        Message response = null;
        int retries = 0;
        final int maxRetries = 20;
        while (retries < maxRetries) {
            try {
                response = client.send(msg).get();
                if (response.getCommand() == Command.RESPONSE && "OK".equals(response.getBody())) {
                    log.debug("Producer: message sent successfully");
                    return response;
                } else {
                    log.warn("Producer: send failed with response {}", response.getBody());
                    throw new RuntimeException("Send failed: " + response.getBody());
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException && "Connection not available".equals(cause.getMessage())) {
                    log.warn("Connection not available, retrying in 3s...");
                    Thread.sleep(3000L *  retries++);
                } else {
                    throw new RuntimeException("Send failed", e);
                }
            }
        }
        throw new RuntimeException("Could not send after " + maxRetries + " retries");
    }

    public Message send(Message msg, String tags) throws InterruptedException {
        msg.setTags(tags);

        return send(msg);
    }
}