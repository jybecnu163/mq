package com.mymq.client;

import com.mymq.common.protocol.Message;
import com.mymq.common.protocol.MessageCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MQClient {
    private static final Logger log = LoggerFactory.getLogger(MQClient.class);
    private final String host;
    private final int port;
    private Channel channel;
    private EventLoopGroup group;
    private final ConcurrentHashMap<String, CompletableFuture<Message>> pendingRequests = new ConcurrentHashMap<>();

    public MQClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new MessageCodec.Encoder())
                                .addLast(new MessageCodec.Decoder())
                                .addLast(new SimpleChannelInboundHandler<Message>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
                                        CompletableFuture<Message> future = pendingRequests.remove(msg.getRequestId());
                                        if (future != null) {
                                            future.complete(msg);
                                        }
                                    }
                                });
                    }
                });
        ChannelFuture f = b.connect(host, port).sync();
        channel = f.channel();
        log.info("Connected to broker at {}:{}", host, port);
    }

    public CompletableFuture<Message> send(Message msg) {
        CompletableFuture<Message> future = new CompletableFuture<>();
        pendingRequests.put(msg.getRequestId(), future);
        channel.writeAndFlush(msg);
        return future;
    }

    public void close() {
        if (channel != null) {
            channel.flush();
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}