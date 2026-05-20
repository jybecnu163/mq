package com.minmq.example;

import com.google.protobuf.InvalidProtocolBufferException;
import com.minmq.client.Consumer;
import com.minmq.client.MQClient;
import com.minmq.client.Producer;
import com.minmq.common.protocol.AckMode;
import com.minmq.common.protocol.Command;
import com.minmq.common.protocol.Message;
import com.mymq.example.proto.OrderOuterClass;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class ProtoBufTest {
    public static void main(String[] args) {
        try {


            MQClient consClient = new MQClient("localhost", 8080);
            consClient.connect();
            Consumer consumer = new Consumer(consClient, "order_topic", "group_1", "*");
            System.out.println(consumer.pull());
            consumer.setAckMode(AckMode.MANUAL);

//            TimeUnit.SECONDS.sleep(3);

            MQClient prodClient = new MQClient("localhost", 8080);
            prodClient.connect();
            Producer producer = new Producer(prodClient);
            productMessages(producer);

            System.out.println("*****************************************************");
            for (int i = 0; i < 20; i++) {
                Message msg = consumer.pull();
                if (msg.getBodyBytes() != null) {
                    // Protobuf 消息
                    if ("protobuf".equals(msg.getBodyCodec())) {
                        try {
                            OrderOuterClass.Order order = OrderOuterClass.Order.parseFrom(msg.getBodyBytes());
                            System.out.println("Protobuf 订单: " + order.getOrderId());
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else if (msg.getBody() != null) {
                    // JSON 消息
                    System.out.println("JSON 消息: " + msg.getBody());
                } else {
                    // 可能是长轮询超时的空响应
                    System.out.println("无消息，继续等待...");
                    continue;
                }
                consumer.ack();
            }

            System.out.println("===========================================");

//            batchPull(consumer);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static void batchPull(Consumer consumer) throws InterruptedException, ExecutionException {
        consumer.setMaxMessages(5);  // 一次最多拉 10 条
        while (true) {
            List<Message.MessagePayload> messages = consumer.pullBatch();
            System.out.println(messages);
            for (Message.MessagePayload payload : messages) {
                if (payload.getBodyBytes() != null) {
                    // 根据编码类型选择反序列化方式
                    if ("protobuf".equals(payload.getBodyCodec())) {
                        try {
                            OrderOuterClass.Order order = OrderOuterClass.Order.parseFrom(payload.getBodyBytes());
                            System.out.println("订单ID: " + order.getOrderId());
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else {
                    // 回退到旧版 JSON 字符串
                    String jsonBody = payload.getBody();
                    System.out.println("JSON body: " + jsonBody);
                }
                consumer.ack();
            }
//                consumer.ackLast();  // 批量确认
        }
    }

    private static void productMessages(Producer producer) throws InterruptedException {

        // 构造 Protobuf 消息
        OrderOuterClass.Order order1 = OrderOuterClass.Order.newBuilder()
                .setOrderId(12345L)
                .setUserName("张三")
                .setAmount(99.9)
                .setStatus("paid")
                .build();

        for (int i = 0; i < 5; i++) {
            // message中的requestId在构造函数中自动生成
            Message msg = new Message(Command.SEND, "order_topic", null);
            msg.setBodyBytes(order1.toByteArray());   // 序列化为字节数组
            msg.setBodyCodec("protobuf");            // 标记编码类型
            msg.setTags("paid");                     // 标签过滤仍可用

            System.out.println(producer.send(msg));
//            System.out.println(producer.send(new Message(Command.SEND, "order_topic", "json message:" + i)));
        }
    }
}
