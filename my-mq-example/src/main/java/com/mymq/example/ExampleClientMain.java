package com.mymq.example;

import com.mymq.client.Consumer;
import com.mymq.client.MQClient;
import com.mymq.client.Producer;
import com.mymq.common.protocol.Message;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ExampleClientMain {
    public static void main(String[] args) throws Exception {
        MQClient client = new MQClient("localhost", 8080);
        client.connect();

        Producer producer = new Producer(client);
        Consumer consumer = new Consumer(client, "test-topic");

        // 发送一条消息
        new Thread(() -> {
            while (true) {
                try {
                    producer.send("test-topic", System.currentTimeMillis() + " Hello QMQ!");

                    TimeUnit.MILLISECONDS.sleep(1500);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();


        // 尝试拉取几次
        for (int i = 0; i < 5; ) {
            Message msg = consumer.pull();
            if (msg != null && msg.getBody() != null) {
                i++;
                System.out.println("Consumer received: " + msg.getBody());
            } else {
                System.out.println("No message available, retrying in 1s...");
                TimeUnit.MILLISECONDS.sleep(1000);
            }
        }

        client.close();
        System.exit(0);
    }
}