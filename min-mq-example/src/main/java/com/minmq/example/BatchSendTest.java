package com.minmq.example;

import com.minmq.client.MQClient;
import com.minmq.client.Producer;
import com.minmq.common.protocol.BodyCodec;
import com.minmq.common.protocol.Message;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendTest {
    public static void main(String[] args) {
        try {
            MQClient prodClient = new MQClient("localhost", 8080);
            prodClient.connect();
            Producer producer = new Producer(prodClient);

            List<Message.MessagePayload> payloads = Arrays.asList(
                    // 指定了自己的 topic
                    new Message.MessagePayload("order_topic", "msg1".getBytes(), BodyCodec.TEXT, "paid"),
                    new Message.MessagePayload("test_topic", "msg2".getBytes(), BodyCodec.TEXT, "paid"),
                    new Message.MessagePayload("order_topic", "msg3".getBytes(), BodyCodec.TEXT, "shipped"),
                    // topic 为空，使用默认
                    new Message.MessagePayload("", "msg4".getBytes(), BodyCodec.TEXT, "paid"),
                    new Message.MessagePayload("", "msg5".getBytes(), BodyCodec.TEXT, "paid"),
                    new Message.MessagePayload("order_topic", "msg6".getBytes(), BodyCodec.TEXT, "shipped")
            );

            producer.sendBatch("order_topic", payloads);
            System.out.println("batchMsg: " + payloads);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
