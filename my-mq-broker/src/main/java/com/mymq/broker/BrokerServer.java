package com.mymq.broker;

import com.mymq.broker.handler.MessageHandler;
import com.mymq.broker.metrics.MetricsServer;
import com.mymq.broker.store.BrokerStore;
import com.mymq.common.protocol.MessageCodec;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerServer {
    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);
    private final int port;
    private final BrokerStore store;

    private final int metricsPort;
    private final PrometheusMeterRegistry meterRegistry;

    public BrokerServer(int port, int metricsPort, String dataDir) throws Exception {
        this.port = port;
        this.metricsPort = metricsPort;
        // 初始化 Prometheus 注册表
        this.meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // 传入注册表，供业务埋点使用
        this.store = new BrokerStore(dataDir, meterRegistry);
    }

    public void start() throws InterruptedException {
        // 启动指标 HTTP 服务（独立线程）
        Thread metricsThread = new Thread(() -> {
            try {
                new MetricsServer(metricsPort, meterRegistry).start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        metricsThread.setDaemon(true);
        metricsThread.start();
        log.info("metrics started on port {}", metricsPort);

        // 启动 MQ 核心服务
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new MessageCodec.Encoder())
                                    .addLast(new MessageCodec.Decoder())
                                    .addLast(new MessageHandler(store));  // 传入共享的 store
                        }
                    });
            ChannelFuture f = b.bind(port).sync();

            log.info("Broker started on port {}", port);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new BrokerServer(8080, 8081, "./data").start();
    }
}