package com.minmq.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.minmq.client.Consumer;
import com.minmq.client.MQClient;
import com.minmq.client.Producer;
import com.minmq.common.protocol.Command;
import com.minmq.common.protocol.Message;
import com.mymq.example.proto.OrderOuterClass.Order;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ExampleClientMain {
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:DDD");

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

        MQClient prodClient = new MQClient("localhost", 8080);
        MQClient consClient1 = new MQClient("localhost", 8080);
        MQClient consClient2 = new MQClient("localhost", 8080);
        MQClient consClient3 = new MQClient("localhost", 8080);
        prodClient.connect();
        consClient1.connect();
        consClient2.connect();
        consClient3.connect();
        String topic = "test_topic";
        Producer producer = new Producer(prodClient);
        Consumer consumer1 = new Consumer(consClient1, topic, "group_1", "paid");
        Consumer consumer2 = new Consumer(consClient2, topic, "group_2", "order");
        Consumer consumer3 = new Consumer(consClient3, "order_topic", "group_1", "*");

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
                new Message.MessagePayload("order_topic", "msg1", "paid"),
                new Message.MessagePayload("test_topic", "msg2", "paid"),
                new Message.MessagePayload("order_topic", "msg3", "shipped"),
                // topic 为空，使用默认
                new Message.MessagePayload("", "msg4", "paid"),
                new Message.MessagePayload(null, "msg5", "paid"),
                new Message.MessagePayload("order_topic", "msg6", "shipped")
        );

        String[] tags = new String[]{"paid", "order", "paid,order"};

        String[] names = new String[]{"zhangsan", "lisi", "wangwu", "xiongliu", "maoqi", "zhuba", "liujiu"};
        int namelen = names.length;
        // 2. 启动 Producer 线程
        for (int n = 0; n < 1; n++) {
            // 单线程生产10W，大概10.5s，
            // 10线程分别生产10W，大概30s，sdf.format 和 rand 可能时单线程

            // 去除 sdf.format 和 rand =》 28.8s，那么大概tps=3.5W

            new Thread(() -> {
                int i = 0;
                long start = System.currentTimeMillis();
                while (i < 10) {
                    try {
//                    producer.send(new Message(Command.SEND, topic,
//                                    sdf.format(System.currentTimeMillis()) + " Hello QMQ!" + i++),
//                            tagsArr[rand.nextInt(tagsArr.length)]);
                        String name = names[i % namelen];
                        Message msg = new Message(Command.SEND, topic,
                                mapper.writeValueAsString(new Test(i++, name, name + "@email.com")));
                        String tag = tags[i % 3];
                        producer.send(msg, tag);
                        System.out.println("msg with tag: " + msg + " " + tag);

                        // 发送一条 0-60 分钟后投递的延时消息
                        Message delayMsg = Message.createDelay(topic,
                                mapper.writeValueAsString(new DelayMessage(System.currentTimeMillis(),
                                        "This is a delayed message:" + i)),
                                1000L * 60 * (i % 61));
                        delayMsg.setTags(tagsArr[rand.nextInt(tagsArr.length)]);
                        producer.send(delayMsg); // 或直接 producer.sendDelay(...)
                        System.out.println("delayMsg: " + delayMsg);

                        List<Message.MessagePayload> batchMessages = Arrays.asList(
                                // 指定了自己的 topic
                                new Message.MessagePayload("order_topic",
                                        mapper.writeValueAsString(new BatchMessage(i, "a", true, null)), "paid"),
                                new Message.MessagePayload("test_topic",
                                        mapper.writeValueAsString(new BatchMessage(i, "b", true, new ArrayList<>())), "paid"),
                                new Message.MessagePayload("order_topic",
                                        mapper.writeValueAsString(new BatchMessage(i, "c", true, new ArrayList<>() {{
                                            add(null);
                                        }})), "shipped"),
                                // topic 为空，使用默认
                                new Message.MessagePayload("",
                                        mapper.writeValueAsString(new BatchMessage(i, "c", true, new ArrayList<>() {{
                                            add(new BatchMessage(100, "a", false, null));
                                        }})), "paid"),
                                new Message.MessagePayload(null,
                                        mapper.writeValueAsString(new BatchMessage(i, "c", true, new ArrayList<>() {{
                                            add(new BatchMessage(101, "a", false, new ArrayList<>()));
                                        }})), "paid"),
                                new Message.MessagePayload("order_topic",
                                        mapper.writeValueAsString(new BatchMessage(i, "b", true, new ArrayList<>() {{
                                            add(new BatchMessage(102, "c", true, new ArrayList<>()));
                                            add(new BatchMessage(103, "a", false, new ArrayList<>()));
                                        }})), "shipped")
                        );

                        producer.sendBatch("order_topic", payloads);
                        System.out.println("batchMsg: " + payloads);

                        // 构造 Protobuf 消息
                        Order order = Order.newBuilder()
                                .setOrderId(1001L)
                                .setUserName("张三")
                                .setAmount(99.99)
                                .setStatus("PAID")
                                .build();

                        Message msg2 = new Message(Command.SEND, "order_topic", null);
                        msg2.setBodyBytes(order.toByteArray());
                        msg2.setBodyCodec("protobuf");
                        msg2.setTags("paid");
                        producer.send(msg);
                        producer.send(msg2);

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
                        TimeUnit.MILLISECONDS.sleep(5000);
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
                        TimeUnit.MILLISECONDS.sleep(5000);
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
                        if ("protobuf".equals(p.getBodyCodec())) {
                            try {
                                Order order = Order.parseFrom(p.getBodyBytes()); // 反序列化
                                System.err.println("收到订单: " + order.getOrderId() + ", " + order.getUserName());
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException(e);
                            }
                        }else {
                            System.out.println("Consumer3 received: " + p.getBody() +
                                    " (offset=" + messages.size() + ")");
                        }

                        i++;
                    }
                    // 全部处理完后，确认最后一条的偏移量
                    if (!messages.isEmpty()) {
                        consumer3.ackLast(); // 需要新增 ackLast 方法，发送最后一条 offset 进行批量确认
                    } else {
                        System.out.println("No message available, Consumer3 retrying in 5s...");
                        TimeUnit.MILLISECONDS.sleep(5000);
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

class Test {
    private long id;
    private String name;
    private String email;

    public Test(long id, String name, String email) {
        this.id = id;
        this.name = name;
        this.email = email;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}

class DelayMessage {
    private String mBody;
    private long offset;

    public DelayMessage(long offset, String mBody) {
        this.mBody = mBody;
        this.offset = offset;
    }

    public String getBody() {
        return mBody;
    }

    public void setBody(String mBody) {
        this.mBody = mBody;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}

class BatchMessage {
    private int id;
    private String type;
    private boolean active;
    private List<BatchMessage> subMessages;

    public BatchMessage(int id, String type, boolean active, List<BatchMessage> subMessages) {
        this.id = id;
        this.type = type;
        this.active = active;
        this.subMessages = subMessages;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public List<BatchMessage> getSubMessages() {
        return subMessages;
    }

    public void setSubMessages(List<BatchMessage> subMessages) {
        this.subMessages = subMessages;
    }


}