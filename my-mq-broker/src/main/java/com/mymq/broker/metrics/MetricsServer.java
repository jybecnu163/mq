package com.mymq.broker.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mymq.broker.store.BrokerStore;
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
import java.util.HashMap;
import java.util.Map;

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

    public void start(BrokerStore store) throws InterruptedException {
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
                                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws JsonProcessingException {
                                            // 在 channelRead0 中增加判断
                                            if ("/metrics".equals(request.uri())) {
                                                String metrics = registry.scrape();
                                                FullHttpResponse response = new DefaultFullHttpResponse(
                                                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                        Unpooled.copiedBuffer(metrics, StandardCharsets.UTF_8));
                                                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                                                ctx.writeAndFlush(response);
                                            } else {
                                                String uri = request.uri();
                                                if ("/hello".equals(uri)) {
                                                    String html = "<html><body><h1>Hello World</h1></body></html>";
                                                    FullHttpResponse response = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1,
                                                            HttpResponseStatus.OK,
                                                            Unpooled.copiedBuffer(html, StandardCharsets.UTF_8));
                                                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                                                    ctx.writeAndFlush(response);
                                                    return;
                                                }

                                                if ("/health".equals(uri)) {
                                                    FullHttpResponse health = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                            Unpooled.copiedBuffer("<html><body><h1>UP</h1></body></html>", StandardCharsets.UTF_8));
                                                    health.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                                                    ctx.writeAndFlush(health);
                                                    return;
                                                }

                                                // 新增：获取所有 topic
                                                if ("/admin/topics".equals(uri)) {
                                                    String json = new ObjectMapper().writeValueAsString(store.getAllTopics());
                                                    String html = "<html><body>" + json + "</body></html>";
                                                    FullHttpResponse response = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                            Unpooled.copiedBuffer(html, StandardCharsets.UTF_8));
                                                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                                                    ctx.writeAndFlush(response);
                                                    return;
                                                }

                                                // 新增：获取消费者组
                                                if ("/admin/consumers".equals(uri)) {
                                                    String json = new ObjectMapper().writeValueAsString(store.getConsumerGroups());
                                                    String html = "<html><body>" + json + "</body></html>";
                                                    FullHttpResponse response = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                            Unpooled.copiedBuffer(html, StandardCharsets.UTF_8));
                                                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                                                    ctx.writeAndFlush(response);
                                                    return;
                                                }

                                                // 新增：获取统计数据
                                                if ("/admin/stats".equals(uri)) {
                                                    Map<String, Object> stats = new HashMap<>();
                                                    stats.put("produced", store.getMinuteProductionRates());
                                                    stats.put("consumed", store.getMinuteConsumptionRates());

                                                    String html = "<html><body>" + new ObjectMapper().writeValueAsString(stats) + "</body></html>";
                                                    FullHttpResponse response = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                            Unpooled.copiedBuffer(html, StandardCharsets.UTF_8));

                                                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                                                    ctx.writeAndFlush(response);
                                                    return;
                                                }

                                                // 可选：内置管理页面
                                                if ("/admin".equals(uri) || "/admin/".equals(uri)) {
                                                    String html = "<html><body>...</body></html>";
                                                    FullHttpResponse response = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                                                            Unpooled.copiedBuffer(html, StandardCharsets.UTF_8));
                                                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                                                    ctx.writeAndFlush(response);
                                                    return;
                                                }

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