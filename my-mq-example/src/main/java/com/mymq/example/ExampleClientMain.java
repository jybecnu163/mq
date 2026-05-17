package com.mymq.example;

import com.mymq.client.Consumer;
import com.mymq.client.MQClient;
import com.mymq.client.Producer;
import com.mymq.common.protocol.Message;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ExampleClientMain {
    public static void main(String[] args) throws Exception {
        MQClient prodClient = new MQClient("localhost", 8080);
        MQClient consClient1 = new MQClient("localhost", 8080);
        MQClient consClient2 = new MQClient("localhost", 8080);
        prodClient.connect();
        consClient1.connect();
        consClient2.connect();

        Producer producer = new Producer(prodClient);
        Consumer consumer1 = new Consumer(consClient1, "test-topic", "group-1");
        Consumer consumer2 = new Consumer(consClient2, "test-topic", "group-2");

        // 1. 预注册两个消费者组（通过一次 PULL 触发服务端注册）
        consumer1.pull();
        consumer2.pull();

//        让出 CPU 保证注册请求到达服务端
        Thread.sleep(200);
        // 2. 启动 Producer 线程
        new Thread(() -> {
            int i = 0;
            while (true) {
                try {
                    producer.send("test-topic", System.currentTimeMillis() + " Hello QMQ!" + i++);
                    TimeUnit.MILLISECONDS.sleep(1500);
                } catch (Exception e) {
                    System.err.println("Send error: " + e.getMessage());
                }
            }
        }).start();

        // 3. 消费者线程（使用独立连接，逻辑不变）
        new Thread(() -> {
            try {
                // 尝试拉取几次
                for (int i = 0; i < 50; ) {
                    Message msg = consumer1.pull();
                    if (msg != null && msg.getBody() != null) {
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
                for (int i = 0; i < 50; ) {
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

        TimeUnit.SECONDS.sleep(3600);

        prodClient.close();
        consClient1.close();
        consClient2.close();

        System.exit(0);
    }
}