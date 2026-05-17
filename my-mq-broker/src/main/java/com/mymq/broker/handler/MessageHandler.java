package com.mymq.broker.handler;

import com.mymq.broker.store.BrokerStore;
import com.mymq.broker.store.ConsumeIndexManager;
import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;

public class MessageHandler extends SimpleChannelInboundHandler<Message> {

    private final BrokerStore store;

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
                    String group = entry.getKey().substring(topic.length() + 1);
                    entry.getValue().appendOffset(offset);
                    System.out.println("Index appended: topic=" + topic + ", group=" + group + ", offset=" + offset);
                }
            }

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

            Message resp = new Message(Command.RESPONSE, topic, null);
            resp.setRequestId(msg.getRequestId());
            if (physicalOffset >= 0) {
                String body = store.readMessage(physicalOffset);
                resp.setBody(body);
                resp.setPullOffset(indexMgr.getConsumerOffset());
            } else {
                resp.setBody(null);
                resp.setPullOffset(-1);
            }
            ctx.writeAndFlush(resp);
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
                System.out.println("ACK received: topic=" + topic + ", group=" + group + ", offset=" + offset);
            }

            // 回复 ACK 确认
            Message resp = new Message(Command.RESPONSE, topic, "ACK_OK");
            resp.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(resp);
        } catch (Exception e) {
            // 即使出错也返回错误
            Message error = new Message(Command.RESPONSE, msg.getTopic(), "ACK_ERROR");
            error.setRequestId(msg.getRequestId());
            ctx.writeAndFlush(error);
        }
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
}