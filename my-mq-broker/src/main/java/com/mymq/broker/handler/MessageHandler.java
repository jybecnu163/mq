package com.mymq.broker.handler;

import com.mymq.broker.store.BrokerStore;
import com.mymq.broker.store.ConsumeIndexManager;
import com.mymq.broker.store.MessageLog;
import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class MessageHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private final BrokerStore store;
    // 主题 -> 挂起的拉取请求列表（线程安全）
    private final ConcurrentMap<String, List<PendingPull>> pendingPulls = new ConcurrentHashMap<>();
    // 定时任务调度器，用于超时控制
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public MessageHandler(BrokerStore store) {
        this.store = store;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        Command cmd = msg.getCommand();
        switch (cmd) {
            case SEND:
                if (msg.getDelayMs() > 0) {
                    handleDelaySend(ctx, msg);
                } else {
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
        try {
            String topic = msg.getTopic();
            String body = msg.getBody();
            String tags = msg.getTags();  // 获取 tags
            long offset = store.appendMessage(topic, body, tags);

            log.info("Message stored: topic={}, offset={}, body={}", topic, offset, body);

            // 遍历所有已注册组，自动追加索引   // 追加索引到所有组（同前）
            for (Map.Entry<String, ConsumeIndexManager> entry : store.getAllGroupIndexes().entrySet()) {
                if (entry.getKey().startsWith(topic + "-")) {
//                    String group = entry.getKey().substring(topic.length() + 1);
                    entry.getValue().appendOffset(offset, System.currentTimeMillis());
//                    System.out.println("Index appended: topic=" + topic + ", group=" + group + ", offset=" + offset);
                }
            }

            // 唤醒该 topic 下所有挂起的拉取请求
            wakeupPendingPulls(topic);

            // 回复生产者
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

            log.info("Group active: topic={}, group={}", topic, group);
            // 循环查找匹配的消息
            while (true) {
                long physicalOffset = indexMgr.peekNextOffset();

                if (physicalOffset < 0) {
                    // 无消息可消费，挂起长轮询  ... 挂起逻辑不变 ...

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

                // 读取完整消息（含 tags）
                MessageLog.MessageEntry entry = store.readMessageData(physicalOffset);
                String messageTags = entry.getTags();

                // 检查 Tag 是否匹配
                if (matchTag(messageTags, subscribeTag)) {
                    // 匹配，返回给消费者
                    Message resp = new Message(Command.RESPONSE, topic, entry.getBody());
                    resp.setRequestId(msg.getRequestId());
                    resp.setPullOffset(indexMgr.getConsumerOffset());
                    ctx.writeAndFlush(resp);
                    return;
                } else {
                    // 不匹配，自动跳过（ACK 推进偏移）
                    indexMgr.commitOffset(indexMgr.getConsumerOffset());
                    // 继续循环，尝试下一条消息
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private void handleAck(ChannelHandlerContext ctx, Message msg) throws Exception {
        try {
            String topic = msg.getTopic();
            String group = msg.getGroup() != null ? msg.getGroup() : "default";
            long offset = msg.getPullOffset();
            ConsumeIndexManager indexMgr = store.getOrCreateIndex(topic, group);
            if (indexMgr != null) {
                indexMgr.commitOffset(offset);
            }

            // 回复确认，防止客户端阻塞
            Message resp = new Message(Command.RESPONSE, topic, "ACK_OK");
            resp.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(resp);
        } catch (Exception e) {
            e.printStackTrace();
            // 即使出错也返回错误
            Message error = new Message(Command.RESPONSE, msg.getTopic(), "ACK_ERROR");
            error.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(error);
        }
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