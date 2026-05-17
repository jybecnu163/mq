package com.mymq.broker.handler;

import com.mymq.broker.store.BrokerStore;
import com.mymq.broker.store.ConsumeIndexManager;
import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class MessageHandler extends SimpleChannelInboundHandler<Message> {

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
                handleSend(ctx, msg);
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

    private void handleSend(ChannelHandlerContext ctx, Message msg) {
        try {
            String topic = msg.getTopic();
            String body = msg.getBody();
            long offset = store.appendMessage(topic, body);
            System.out.println("Message stored: topic=" + topic + ", offset=" + offset + ", body=" + body);

            // 遍历所有已注册组，自动追加索引
            for (Map.Entry<String, ConsumeIndexManager> entry : store.getAllGroupIndexes().entrySet()) {
                if (entry.getKey().startsWith(topic + "-")) {
//                    String group = entry.getKey().substring(topic.length() + 1);
                    entry.getValue().appendOffset(offset);
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
                        System.out.println("PULL woken: topic=" + pending.topic + ", group=" + pending.group);
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

            // 获取或创建消费者组索引（注册动作内置于 getOrCreateIndex）
            ConsumeIndexManager indexMgr = store.getOrCreateIndex(topic, group);
            System.out.println("Group active: topic=" + topic + ", group=" + group);

            long physicalOffset = indexMgr.peekNextOffset();
            System.out.println("PULL debug: topic=" + topic + ", group=" + group +
                    ", consumerOffset=" + indexMgr.getConsumerOffset() +
                    ", physicalOffset=" + physicalOffset);

            if (physicalOffset >= 0) {
                // 有消息，立即返回
                String body = store.readMessage(physicalOffset);
                Message resp = new Message(Command.RESPONSE, topic, body);
                resp.setRequestId(msg.getRequestId());
                resp.setPullOffset(indexMgr.getConsumerOffset());
                ctx.writeAndFlush(resp);
            } else {
                // 无消息，挂起等待，并设置超时
                PendingPull pending = new PendingPull(ctx, msg.getRequestId(), group, topic);
                // 将请求加入该 topic 的挂起列表
                List<PendingPull> list = pendingPulls.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>());
                list.add(pending);
                System.out.println("PULL suspended: topic=" + topic + ", group=" + group);

                // 启动超时任务：5秒后自动返回空响应
                pending.timeoutFuture = scheduler.schedule(() -> {
                    // 检查请求是否仍在列表中（可能已被唤醒移除）
                    if (list.remove(pending)) {
                        // 发送空响应
                        Message emptyResp = new Message(Command.RESPONSE, topic, null);
                        emptyResp.setRequestId(msg.getRequestId());
                        emptyResp.setPullOffset(-1);
                        ctx.writeAndFlush(emptyResp);
                        System.out.println("PULL timeout: topic=" + topic + ", group=" + group);
                    }
                }, 5000, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleAck(ChannelHandlerContext ctx, Message msg) throws Exception {
        try {
            String topic = msg.getTopic();
            String group = msg.getGroup() != null ? msg.getGroup() : "default";
            long offset = msg.getPullOffset();
            ConsumeIndexManager indexMgr = store.getOrCreateIndex(topic, group);
            if (indexMgr != null) {
                indexMgr.commitOffset(offset);
                System.out.println("ACK: topic=" + topic + ", group=" + group + ", offset=" + offset);
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

    /**
     * 连接断开时清理该连接的所有挂起请求
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 遍历所有 topic 的挂起列表，移除属于该 ctx 的请求
        for (List<PendingPull> list : pendingPulls.values()) {
            list.removeIf(pending -> pending.ctx.equals(ctx) && pending.timeoutFuture != null && pending.timeoutFuture.cancel(false));
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.io.IOException) {
            System.out.println("Client disconnected: " + ctx.channel().remoteAddress());
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