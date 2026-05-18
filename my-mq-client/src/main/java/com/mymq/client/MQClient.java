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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MQClient {
    private static final Logger log = LoggerFactory.getLogger(MQClient.class);
    private final String host;
    private final int port;
    private Channel channel;
    private final EventLoopGroup group;
    private final ConcurrentHashMap<String, CompletableFuture<Message>> pendingRequests = new ConcurrentHashMap<>();

    // 重连配置
    private static final int MAX_RETRY = 20;            // 最大重试次数
    private static final long RETRY_DELAY_MS = 3000;    // 重试间隔
    private final AtomicBoolean retrying = new AtomicBoolean(false);
    private volatile boolean closed = false;             // 是否主动关闭

    public MQClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.group = new NioEventLoopGroup(1);           // 单线程即可
    }

    /**
     * 同步连接，首次调用使用
     */
    public void connect() throws InterruptedException {
        ChannelFuture future = doConnect();
        future.sync();
        channel = future.channel();
        log.info("Connected to broker at {}:{}", host, port);
    }

    /**
     * 异步建立连接，返回 ChannelFuture
     */
    private ChannelFuture doConnect() {
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
                                        } else {
                                            log.warn("Received unexpected response: {}", msg.getRequestId());
                                        }
                                    }

                                    @Override
                                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                        log.warn("Disconnected from broker");
                                        // 如果不是主动关闭，触发重连
                                        if (!closed) {
                                            scheduleReconnect();
                                        }
                                        super.channelInactive(ctx);
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        if (cause instanceof java.io.IOException) {
                                            log.warn("Connection reset, will reconnect");
                                        } else {
                                            log.error("Unexpected error", cause);
                                        }
                                        ctx.close();
                                    }
                                });
                    }
                });
        return b.connect(host, port);
    }

    /**
     * 调度重连（避免在事件循环中阻塞）
     */
    private void scheduleReconnect() {
        if (!retrying.compareAndSet(false, true)) {
            return; // 已有重连任务在进行
        }
        group.execute(() -> attemptReconnect(1));
    }

    private void attemptReconnect(int attempt) {
        if (closed) {
            retrying.set(false);
            return;
        }
        log.info("Attempting reconnect {}/{}", attempt, MAX_RETRY);
        ChannelFuture future = doConnect();
        future.addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                channel = f.channel();
                log.info("Reconnected successfully");
                retrying.set(false);
                // 可以在这里通知上层重连成功（可选）
            } else {
                if (attempt < MAX_RETRY) {
                    log.warn("Reconnect failed, retry in {}ms",
                            RETRY_DELAY_MS);

                    group.schedule(() ->
                                    attemptReconnect(attempt + 1),
                            RETRY_DELAY_MS,
                            TimeUnit.MILLISECONDS);
                } else {
                    retrying.set(false);
                    // 超过最大次数，可回调给应用层
                    log.error("Max reconnect attempts reached, giving up");
                }
            }
        });
    }

    /**
     * 发送消息（异步），返回 CompletableFuture
     * 如果连接未建立，会等待重连或立即失败
     */
    public CompletableFuture<Message> send(Message msg) {
        CompletableFuture<Message> future = new CompletableFuture<>();
        Channel ch = this.channel;
        if (ch == null || !ch.isActive()) {
            // 连接不可用，立即失败（也可以选择等待重连，但会增加复杂度）
            future.completeExceptionally(new RuntimeException("Connection not available"));
            return future;
        }
        pendingRequests.put(msg.getRequestId(), future);
        ch.writeAndFlush(msg).addListener((ChannelFutureListener) writeFuture -> {
            if (!writeFuture.isSuccess()) {
                pendingRequests.remove(msg.getRequestId());
                future.completeExceptionally(writeFuture.cause());
            }
        });
        return future;
    }

    /**
     * 主动关闭客户端
     */
    public void close() {
        closed = true;
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
        log.info("MQClient closed");
    }

    public boolean isConnected() {
        return channel != null && channel.isActive();
    }
}