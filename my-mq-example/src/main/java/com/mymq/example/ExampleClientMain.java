package com.mymq.example;

import com.mymq.client.Consumer;
import com.mymq.client.MQClient;
import com.mymq.client.Producer;
import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ExampleClientMain {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:DDD");

    public static void main(String[] args) throws Exception {
        MQClient prodClient = new MQClient("localhost", 8080);
        MQClient consClient1 = new MQClient("localhost", 8080);
        MQClient consClient2 = new MQClient("localhost", 8080);
        MQClient consClient3 = new MQClient("localhost", 8080);
        prodClient.connect();
        consClient1.connect();
        consClient2.connect();
        consClient3.connect();
        String topic = "test-topic";
        Producer producer = new Producer(prodClient);
        Consumer consumer1 = new Consumer(consClient1, topic, "group-1", "paid");
        Consumer consumer2 = new Consumer(consClient2, topic, "group-2", "order");
        Consumer consumer3 = new Consumer(consClient3, "order-topic", "group-1", "*");

        // 1. 预注册两个消费者组（通过一次 PULL 触发服务端注册）
        consumer1.pull();
        consumer2.pull();
        consumer3.pull();

        // 创建消费者后，设置从 1 分钟前开始消费
        consumer1.setStartTime(System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(1));

        Random rand = new Random();
        String[] tagsArr = new String[]{"paid", "order", "paid,order"};
//        让出 CPU 保证注册请求到达服务端
        Thread.sleep(200);

        // // 混合主题批量发送
        List<Message.MessagePayload> payloads = Arrays.asList(
                // 指定了自己的 topic
                new Message.MessagePayload("order-topic", "msg1", "paid"),
                new Message.MessagePayload("test-topic", "msg2", "paid"),
                new Message.MessagePayload("order-topic", "msg3", "shipped"),
                // topic 为空，使用默认
                new Message.MessagePayload("", "msg4", "paid"),
                new Message.MessagePayload(null, "msg5", "paid"),
                new Message.MessagePayload("order-topic", "msg6", "shipped")
        );

        // 2. 启动 Producer 线程
        for (int n = 0; n < 1; n++) {
            // 单线程生产10W，大概10.5s，
            // 10线程分别生产10W，大概30s，sdf.format 和 rand 可能时单线程

            // 去除 sdf.format 和 rand =》 28.8s，那么大概tps=3.5W

            new Thread(() -> {
                String[] tags = new String[]{"paid", "order", "paid,order"};
                int i = 0;
                long start = System.currentTimeMillis();
                while (i < 3) {
                    try {
//                    producer.send(new Message(Command.SEND, topic,
//                                    sdf.format(System.currentTimeMillis()) + " Hello QMQ!" + i++),
//                            tagsArr[rand.nextInt(tagsArr.length)]);
                        Message msg = new Message(Command.SEND, topic, " Hello QMQ!" + i++);
                        String tag = tags[i % 3];
                        producer.send(msg, tag);
                        System.out.println("msg with tag: " + msg + " " + tag);

                        // 发送一条 0-60 分钟后投递的延时消息
                        Message delayMsg = Message.createDelay(topic,
                                sdf.format(System.currentTimeMillis()) + " This is a delayed message",
                                1000L * 60 * (i % 61));
                        delayMsg.setTags(tagsArr[rand.nextInt(tagsArr.length)]);
                        producer.send(delayMsg); // 或直接 producer.sendDelay(...)
                        System.out.println("delayMsg: " + delayMsg);

                        producer.sendBatch("order-topic", payloads);
                        System.out.println("batchMsg: " + payloads);
                        TimeUnit.MILLISECONDS.sleep(1500);
                    } catch (Exception e) {
                        System.err.println("Send error: " + e.getMessage());
                    }
                }
                System.err.println(System.currentTimeMillis() - start);
            }).start();
        }

        // 3. 消费者线程（使用独立连接，逻辑不变）
        new Thread(() -> {
            try {
                // 尝试拉取几次
                for (int i = 0; i < 5000; ) {
                    Message msg = consumer1.pull();
                    if (msg != null && msg.getBody() != null) {
                        // 消费者需要处理 pull() 返回 null 的情况（长轮询超时）
                        i++;

                        System.out.println("Consumer1 received: " + msg.getBody() +
                                " (offset=" + msg.getPullOffset() + ")");
                        // 确认消息处理完成
                        consumer1.ack();
                    } else {
                        System.out.println("No message available, Consumer1 retrying in 1s...");
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                // 尝试拉取几次
                for (int i = 0; i < 50000; ) {
                    Message msg = consumer2.pull();
                    if (msg != null && msg.getBody() != null) {
                        i++;

                        System.out.println("Consumer2 received: " + msg.getBody() +
                                " (offset=" + msg.getPullOffset() + ")");
                        // 确认消息处理完成
                        consumer2.ack();
                    } else {
                        System.out.println("No message available, Consumer2 retrying in 1s...");
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                // 设置批次拉取最大数量
                consumer3.setMaxMessages(5);
                // 尝试拉取几次
                for (int i = 0; i < 50000; ) {
                    List<Message.MessagePayload> messages = consumer3.pullBatch();
                    for (Message.MessagePayload p : messages) {
                        System.out.println("Consumer3 received: " + p.getBody() +
                                " (offset=" + messages.size() + ")");
                    }
                    // 全部处理完后，确认最后一条的偏移量
                    if (!messages.isEmpty()) {
                        consumer3.ackLast(); // 需要新增 ackLast 方法，发送最后一条 offset 进行批量确认
                    } else {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        TimeUnit.SECONDS.sleep(36000);

        prodClient.close();
        consClient1.close();
        consClient2.close();
        consClient3.close();

        System.exit(0);
    }
}