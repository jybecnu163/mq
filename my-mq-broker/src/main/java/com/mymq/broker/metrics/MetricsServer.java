package com.mymq.broker.metrics;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 内嵌 HTTP 服务器，暴露 /metrics 给 Prometheus 抓取
 */
public class MetricsServer {
    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);
    private final int port;
    private final PrometheusMeterRegistry registry;

    public MetricsServer(int port, PrometheusMeterRegistry registry) {
        this.port = port;
        this.registry = registry;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new HttpServerCodec())
                                    .addLast(new HttpObjectAggregator(65536))
                                    .addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
                                            // 在 channelRead0 中增加判断
                                            if ("/health".equals(request.uri())) {
                                                FullHttpResponse health = new DefaultFullHttpResponse(
                                                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                        Unpooled.copiedBuffer("UP", StandardCharsets.UTF_8));
                                                ctx.writeAndFlush(health);
                                            } else if ("/metrics".equals(request.uri())) {
                                                String metrics = registry.scrape();
                                                FullHttpResponse response = new DefaultFullHttpResponse(
                                                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                        Unpooled.copiedBuffer(metrics, StandardCharsets.UTF_8));
                                                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                                                ctx.writeAndFlush(response);
                                            } else {
                                                FullHttpResponse response = new DefaultFullHttpResponse(
                                                        HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
                                                ctx.writeAndFlush(response);
                                            }
                                        }
                                    }).addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                            if (cause instanceof java.io.IOException) {
                                                // 静默处理连接重置，不打印堆栈
                                                log.info(cause.getMessage());
                                            } else {
                                                // 其他异常记录日志（可选）
                                                log.error(cause.getMessage(), cause);
                                            }
                                            ctx.close();
                                        }
                                    });
                        }
                    });
            ChannelFuture f = b.bind(port).sync();

            log.info("Metrics server started on port {}", port);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}