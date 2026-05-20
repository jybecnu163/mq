package com.minmq.broker.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minmq.broker.store.BrokerStore;
import com.minmq.broker.store.ConsumeIndexManager;
import com.minmq.broker.store.MessageLog;
import com.minmq.common.protocol.Command;
import com.minmq.common.protocol.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MessageHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private final BrokerStore store;
    // 主题 -> 挂起的拉取请求列表（线程安全）
    private final ConcurrentMap<String, List<PendingPull>> pendingPulls = new ConcurrentHashMap<>();
    // 定时任务调度器，用于超时控制
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final int MAX_RETRY_COUNT = 3;   // 最大重试次数，可配置
    ObjectMapper mapper = new ObjectMapper();

    public MessageHandler(BrokerStore store) {
        this.store = store;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        Command cmd = msg.getCommand();
        System.err.println(mapper.writeValueAsString(msg));
        switch (cmd) {
            case SEND:
                if (msg.getDelayMs() > 0) {
                    // 处理延时消息
                    handleDelaySend(ctx, msg);
                } else {
                    // 即时消息
                    handleSend(ctx, msg);
                }
                break;
            case PULL:
                handlePull(ctx, msg);
                break;
            case ACK:
                handleAck(ctx, msg);
                break;
            default:
                Message error = new Message(Command.RESPONSE, null, "Unknown command");
                error.setRequestId(msg.getRequestId());
                ctx.writeAndFlush(error);
        }
    }

    private void handleDelaySend(ChannelHandlerContext ctx, Message msg) {
        try {
            String topic = msg.getTopic();
            String body = msg.getBody();
            long delayMs = msg.getDelayMs();
            long expireTime = System.currentTimeMillis() + delayMs;

            // 委托给 BrokerStore 的延时调度
            store.scheduleDelayMessage(expireTime, topic, body, msg.getTags());

            log.info("Delay message scheduled: topic={}, body={}, expireAt={}", topic, body, expireTime);

            Message ack = new Message(Command.RESPONSE, topic, "OK");
            ack.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(ack);
        } catch (Exception e) {
            e.printStackTrace();
            Message error = new Message(Command.RESPONSE, msg.getTopic(), "ERROR");
            error.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(error);
        }
    }

    private void handleSend(ChannelHandlerContext ctx, Message msg) {
        String topic = msg.getTopic();
        String body = msg.getBody();
        String tags = msg.getTags();  // 获取 tags
        List<Message.MessagePayload> payloads = msg.getPayloads();
        byte[] bodyBytes = msg.getBodyBytes();
        String bodyCodec = msg.getBodyCodec();

        try {
            if (payloads != null && !payloads.isEmpty()) {
                // ---- 批量发送 ----
                List<Long> offsets = new ArrayList<>(payloads.size());
                Set<String> affectedTopics = new HashSet<>();   // 实际写入过的 topic 集合
                for (Message.MessagePayload p : payloads) {
                    // 决定本条消息的实际 topic：优先使用 payload 中的，为空则用总 topic
                    String effectiveTopic = (p.getTopic() == null || p.getTopic().isEmpty())
                            ? topic : p.getTopic();

                    // 写入消息日志，返回物理偏移量
                    byte[] pBodyBytes = p.getBodyBytes();
                    String pBodyCodec = p.getBodyCodec();
                    long offset = store.appendMessage(effectiveTopic, p.getBody(),
                            p.getTags(), pBodyBytes, pBodyCodec);
                    offsets.add(offset);
                    affectedTopics.add(effectiveTopic);

                    // 为所有订阅了 effectiveTopic 的消费者组追加索引
                    for (Map.Entry<String, ConsumeIndexManager> entry : store.getAllGroupIndexes().entrySet()) {
                        if (entry.getKey().startsWith(effectiveTopic + "-")) {
                            entry.getValue().appendOffset(offset, System.currentTimeMillis());
                        }
                    }
                }

                // 唤醒所有受影响 topic 的长轮询消费者
                for (String tp : affectedTopics) {
                    wakeupPendingPulls(tp);
                }

                // 将偏移量列表编码返回
                String offsetsStr = offsets.stream().map(String::valueOf)
                        .collect(Collectors.joining(","));
                Message ack = new Message(Command.RESPONSE, topic, "OK");
                ack.setRequestId(msg.getRequestId());
                ack.getHeaders().put("offsets", offsetsStr);
                ctx.writeAndFlush(ack);
            } else {
                // ---- 单条发送（原有逻辑） ----
                long offset = store.appendMessage(topic, body, tags, bodyBytes, bodyCodec);

                log.info("Message stored: topic={}, offset={}, body={}", topic, offset, body);

                appendIndexForAllGroups(topic, offset);

                // 唤醒该 topic 下所有挂起的拉取请求
                wakeupPendingPulls(topic);

                // 回复生产者
                Message ack = new Message(Command.RESPONSE, topic, "OK");
                ack.setRequestId(msg.getRequestId());
                ctx.writeAndFlush(ack);
            }
        } catch (Exception e) {
            Message error = new Message(Command.RESPONSE, msg.getTopic(), "ERROR_SEND");
            error.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(error);
        }

        for (int i = 0; i < (payloads != null && !payloads.isEmpty()
                ? payloads.size() : 1); i++) {
            // 动态增加带 topic 标签的计数器
            store.getMeterRegistry().counter("mq_messages_produced_total",
                    "topic", topic).increment();
            store.getProducedThisMinute().incrementAndGet();
        }
    }

    // 提取公共索引追加
    private void appendIndexForAllGroups(String topic, long offset) throws Exception {
        // 遍历所有已注册组，自动追加索引   // 追加索引到所有组（同前）
        for (Map.Entry<String, ConsumeIndexManager> entry : store.getAllGroupIndexes().entrySet()) {
            if (entry.getKey().startsWith(topic + "-")) {
                entry.getValue()
                        .appendOffset(offset, System.currentTimeMillis());
            }
        }
    }

    /**
     * 唤醒所有挂起的拉取请求（在消息到达后调用）
     */
    private void wakeupPendingPulls(String topic) {
        List<PendingPull> list = pendingPulls.get(topic);
        if (list == null || list.isEmpty()) return;

        // 创建一个临时列表，避免遍历时集合被修改
        List<PendingPull> snapshot = new ArrayList<>(list);
        for (PendingPull pending : snapshot) {
            // 只有确实从列表中移除才算唤醒成功（避免重复响应）
            if (list.remove(pending)) {
                // 取消超时任务
                if (pending.timeoutFuture != null && !pending.timeoutFuture.isDone()) {
                    pending.timeoutFuture.cancel(false);
                }
                // 立即为该消费者组拉取消息并响应
                try {
                    ConsumeIndexManager indexMgr
                            = store.getOrCreateIndex(pending.topic, pending.group);
                    long offset = indexMgr.peekNextOffset();
                    if (offset >= 0) {
                        String body = store.readMessage(offset);
                        Message resp = new Message(Command.RESPONSE, pending.topic, body);
                        resp.setRequestId(pending.requestId);
                        resp.setPullOffset(indexMgr.getConsumerOffset());
                        pending.ctx.writeAndFlush(resp);

                        log.info("PULL woken: topic={}, group={}", pending.topic, pending.group);
                    } else {
                        // 极端情况：消息刚被其他消费者取走，仍无新消息，则挂起（此处简单处理，重新挂起）
                        // 但为了避免复杂逻辑，可直接返回空响应
                        Message emptyResp = new Message(Command.RESPONSE, pending.topic, null);
                        emptyResp.setRequestId(pending.requestId);
                        emptyResp.setPullOffset(-1);
                        pending.ctx.writeAndFlush(emptyResp);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Message error = new Message(Command.RESPONSE, pending.topic, "ERROR");
                    error.setRequestId(pending.requestId);
                    pending.ctx.writeAndFlush(error);
                }
            }
        }
    }

    private void handlePull(ChannelHandlerContext ctx, Message msg) {
        try {
            String topic = msg.getTopic();
            String group = msg.getGroup() != null ? msg.getGroup() : "default";
            String subscribeTag = msg.getSubscribeTag(); // 消费者订阅的 tag
            int maxMessages = Math.max(1, msg.getMaxMessages()); // 至少拉取 1 条

            // 获取或创建消费者组索引（注册动作内置于 getOrCreateIndex）
            ConsumeIndexManager indexMgr = store.getOrCreateIndex(topic, group);
            long startTime = msg.getStartTime();
            if (startTime > 0 && indexMgr.getConsumerOffset() == 0) {
                long startOffset = indexMgr.findStartOffset(startTime);
                if (startOffset >= 0) {
                    indexMgr.resetConsumerOffset(startOffset); // 新增方法，直接设置进度
                    log.info("Consumer {} set start offset to {} based on time {}", group, startOffset, startTime);
                }
            }

//            log.info("Group active: topic={}, group={}", topic, group);

            List<Message.MessagePayload> collected = new ArrayList<>();
            long lastAckOffset = -1; // 用于客户端确认的最后一条偏移量
            // 循环查找匹配的消息
            while (collected.size() < maxMessages) {
                long physicalOffset = indexMgr.peekNextOffset();

                if (physicalOffset < 0) {
                    // 无消息：若已有收集，立即返回；否则挂起
                    if (!collected.isEmpty()) {
                        sendBatchResponse(ctx, msg, collected, lastAckOffset);
                        return;
                    }
                    // 无消息，挂起等待，并设置超时
                    PendingPull pending = new PendingPull(ctx, msg.getRequestId(), group, topic);
                    // 将请求加入该 topic 的挂起列表
                    List<PendingPull> list = pendingPulls.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>());
                    list.add(pending);
                    // 启动超时任务：5秒后自动返回空响应
                    pending.timeoutFuture = scheduler.schedule(() -> {
                        // 检查请求是否仍在列表中（可能已被唤醒移除）
                        if (list.remove(pending)) {
                            // 发送空响应
                            Message emptyResp = new Message(Command.RESPONSE, topic, null);
                            emptyResp.setRequestId(msg.getRequestId());
                            emptyResp.setPullOffset(-1);
                            ctx.writeAndFlush(emptyResp);
                        }
                    }, 5000, TimeUnit.MILLISECONDS);

                    return;
                }

                long consumerOffset = indexMgr.getConsumerOffset();
                int attempts = indexMgr.getAttempts(consumerOffset);

                // --- 死信检查（增加二进制字段传递，防止死信丢失二进制负载） ---
                if (attempts >= MAX_RETRY_COUNT) {
                    // 已达最大重试次数，转入死信队列
                    MessageLog.MessageEntry entry = store.readMessageData(physicalOffset);
                    String dlqTopic = "DLQ-" + topic;

                    // 【修改点】死信转移需同时传递 bodyBytes 和 bodyCodec
                    store.appendMessage(dlqTopic, entry.getBody(), entry.getTags(),
                            entry.bodyBytes, entry.bodyCodec);

                    // 自动 ACK 跳过该消息
                    indexMgr.commitOffset(consumerOffset); // 内部已清理投递计数
                    // indexMgr.clearDeliveryAttempt(); // 移除该行

                    // 死信计数 +1
                    store.getMeterRegistry().counter("mq_dead_letter_total",
                            "originalTopic", topic, "group", group).increment();
                    log.info("Message moved to DLQ [{}]: body={}, codec={}",
                            dlqTopic, entry.getBody(), entry.bodyCodec);

                    // 继续尝试拉取下一条消息
                    continue;
                }

                // 读取完整消息（含 tags）
                MessageLog.MessageEntry entry = store.readMessageData(physicalOffset);
                if (!matchTag(entry.getTags(), subscribeTag)) {
                    indexMgr.commitOffset(consumerOffset); // 跳过不匹配的消息
                    continue;
                }

                // --- 收集消息，填充二进制字段 ---
                Message.MessagePayload payload = new Message.MessagePayload(topic, entry.getBody(), entry.getTags());
                // 【修改点】如果存在二进制 body，则设置到 payload 中
                if (entry.bodyBytes != null) {
                    payload.setBodyBytes(entry.bodyBytes);
                    payload.setBodyCodec(entry.bodyCodec);
                }
                collected.add(payload);
                // 记录此条偏移量用于 ACK
                lastAckOffset = consumerOffset;
                // 记录投递次数
                indexMgr.recordDelivery(consumerOffset);
                // 临时推进偏移，以便获取下一条
                indexMgr.advanceOffset();
            }

            // 发送响应（根据单条/批量选择不同格式）
            if (collected.size() == 1 && maxMessages == 1) {
                Message.MessagePayload payload = collected.get(0);
                Message resp = new Message(Command.RESPONSE, topic, payload.getBody());
                resp.setRequestId(msg.getRequestId());
                resp.setPullOffset(lastAckOffset);
                // 【修改点】单条响应也要携带二进制字段
                resp.setBodyBytes(payload.getBodyBytes());
                resp.setBodyCodec(payload.getBodyCodec());
                ctx.writeAndFlush(resp);
            } else {
                // 达到批量上限，立即返回
                sendBatchResponse(ctx, msg, collected, lastAckOffset);
            }
        } catch (Exception e) {
            log.error("Error in handlePull", e);
            // 即使出错也返回错误
            Message error = new Message(Command.RESPONSE, msg.getTopic(), "PULL_ERROR");
            error.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(error);
        }
    }

    /**
     * 发送批量拉取响应
     */
    private void sendBatchResponse(ChannelHandlerContext ctx, Message request,
                                   List<Message.MessagePayload> messages, long ackOffset) {
        Message resp = new Message(Command.RESPONSE, request.getTopic(), null);
        resp.setRequestId(request.getRequestId());
        resp.setMessages(messages);
        resp.setPullOffset(ackOffset);   // 客户端 ACK 时需使用此偏移量
        ctx.writeAndFlush(resp);
    }

    // Tag 匹配逻辑：支持 '*' 或 null 匹配所有，否则检查逗号分隔列表中是否包含订阅标签
    private boolean matchTag(String messageTags, String subscribeTag) {
        if (subscribeTag == null || subscribeTag.equals("*")) {
            return true; // 不过滤
        }
        if (messageTags == null || messageTags.isEmpty()) {
            return false; // 消息没有标签，但消费者指定了过滤条件，则不匹配
        }
        String[] tagsArray = messageTags.split(",");
        for (String tag : tagsArray) {
            if (tag.trim().equals(subscribeTag.trim())) {
                return true;
            }
        }
        return false;
    }

    private void handleAck(ChannelHandlerContext ctx, Message msg) {
        String topic = msg.getTopic();
        String group = msg.getGroup() != null ? msg.getGroup() : "default";
        long offset = msg.getPullOffset();
        try {
            ConsumeIndexManager indexMgr = store.getOrCreateIndex(topic, group);
            if (indexMgr != null) {
                indexMgr.commitOffset(offset);// 内部已清理投递计数
                // 移除该行// ACK 成功，清除重试记录
                // indexMgr.clearDeliveryAttempt();
                log.debug("ACK: topic={}, group={}, offset={}", topic, group, offset);
            }

            // 回复确认，防止客户端阻塞
            Message resp = new Message(Command.RESPONSE, topic, "ACK_OK");
            resp.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(resp);
        } catch (Exception e) {
            log.error("Error in handleAck", e);
            // 即使出错也返回错误
            Message error = new Message(Command.RESPONSE, msg.getTopic(), "ACK_ERROR");
            error.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(error);
        }

        store.getMeterRegistry().counter("mq_messages_consumed_total", "topic", topic, "group", group).increment();
        store.getConsumedThisMinute().incrementAndGet();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        store.onClientConnected();
        super.channelActive(ctx);
    }

    /**
     * 连接断开时清理该连接的所有挂起请求
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        store.onClientDisconnected();
        // 遍历所有 topic 的挂起列表，移除属于该 ctx 的请求
        for (List<PendingPull> list : pendingPulls.values()) {
            list.removeIf(pending -> pending.ctx.equals(ctx) && pending.timeoutFuture != null && pending.timeoutFuture.cancel(false));
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.io.IOException) {
            log.info("Client disconnected: {}", ctx.channel().remoteAddress());
        } else {
            cause.printStackTrace();
        }
        ctx.close();
    }

    // 内部类：挂起的拉取请求
    private static class PendingPull {
        final ChannelHandlerContext ctx;
        final String requestId;
        final String group;
        final String topic;
        ScheduledFuture<?> timeoutFuture;

        PendingPull(ChannelHandlerContext ctx, String requestId, String group, String topic) {
            this.ctx = ctx;
            this.requestId = requestId;
            this.group = group;
            this.topic = topic;
        }
    }


}