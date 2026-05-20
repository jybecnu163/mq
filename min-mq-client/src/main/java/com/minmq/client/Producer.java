package com.minmq.client;

import com.minmq.common.protocol.Command;
import com.minmq.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    private final MQClient client;

    public Producer(MQClient client) {
        this.client = client;
    }

    /**
     * 批量发送多条消息（可混合不同主题）。
     *
     * @param defaultTopic 当 payload 中没有指定 topic 时使用的默认主题
     * @param payloads     消息负载列表，每条可携带自己的 topic（为 null 或空则使用 defaultTopic）
     * @return 每条消息的物理偏移量列表（顺序与 payloads 一一对应）
     */
    public List<Long> sendBatch(String defaultTopic, List<Message.MessagePayload> payloads)
            throws ExecutionException, InterruptedException {
        if (null == payloads || payloads.isEmpty()) return Collections.emptyList();
        payloads.forEach(k -> {
            if (null == k.getBody() || k.getBody().length == 0) {
                throw new RuntimeException("Batch send body is null or empty");
            }
        });
        Message msg = new Message(Command.SEND, defaultTopic, null);
        msg.setPayloads(payloads);   // 使用批量字段
        Message response = client.send(msg).get();
        if (response.getCommand() == Command.RESPONSE
                && "OK".equals(response.getInfo())) {
            // 偏移量列表通过 headers 返回（逗号分隔）
            String offsetsStr = response.getHeaders().get("offsets");
            if (offsetsStr != null) {
                return Arrays.stream(offsetsStr.split(","))
                        .map(Long::parseLong)
                        .collect(Collectors.toList());
            }
            return Collections.emptyList();
        } else {
            throw new RuntimeException("Batch send failed: "
                    + response.getInfo());
        }
    }

    public Message send(Message msg) throws InterruptedException {
        Message response = null;
        int retries = 0;
        final int maxRetries = 20;
        while (retries < maxRetries) {
            try {
                response = client.send(msg).get();
                if (response.getCommand() == Command.RESPONSE && "OK".equals(response.getInfo())) {
                    log.debug("Producer: message sent successfully");
                    return response;
                } else {
                    log.warn("Producer: send failed with response {}", response.getInfo());
                    throw new RuntimeException("Send failed: " + response.getInfo());
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException && "Connection not available".equals(cause.getMessage())) {
                    log.warn("Connection not available, retrying in 3s...");
                    Thread.sleep(3000L * retries++);
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