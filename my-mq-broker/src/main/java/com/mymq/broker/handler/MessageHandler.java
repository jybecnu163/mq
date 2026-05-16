package com.mymq.broker.handler;

import com.mymq.common.protocol.Command;
import com.mymq.common.protocol.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageHandler extends SimpleChannelInboundHandler<Message> {

    private final Map<String, Queue<Message>> messageStore = new ConcurrentHashMap<>();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        Command cmd = msg.getCommand();
        switch (cmd) {
            case SEND:
                System.out.println("Received message: topic=" + msg.getTopic() + ", body=" + msg.getBody());
                messageStore.computeIfAbsent(msg.getTopic(), k -> new ConcurrentLinkedQueue<>()).add(msg);
                Message ack = new Message(Command.RESPONSE, msg.getTopic(), "OK");
                ack.setRequestId(msg.getRequestId());
                ctx.writeAndFlush(ack);
                break;
            case PULL:
                Queue<Message> queue = messageStore.get(msg.getTopic());
                Message storedMsg = (queue != null) ? queue.poll() : null;
                String body = (storedMsg != null) ? storedMsg.getBody() : null;
                Message resp = new Message(Command.RESPONSE, msg.getTopic(), body);
                resp.setRequestId(msg.getRequestId());
                ctx.writeAndFlush(resp);
                break;
            default:
                Message error = new Message(Command.RESPONSE, null, "Unknown command");
                error.setRequestId(msg.getRequestId());
                ctx.writeAndFlush(error);
        }
    }
}