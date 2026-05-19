package com.mymq.broker;

import com.mymq.broker.handler.MessageHandler;
import com.mymq.broker.metrics.MetricsServer;
import com.mymq.broker.store.BrokerStore;
import com.mymq.common.protocol.MessageCodec;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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

import java.util.concurrent.TimeUnit;

public class BrokerServer {
    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);
    private final int port;
    private final BrokerStore store;

    private final int metricsPort;
    private final PrometheusMeterRegistry meterRegistry;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public BrokerServer(int port, int metricsPort, String dataDir) throws Exception {
        this.port = port;
        this.metricsPort = metricsPort;
        // 初始化 Prometheus 注册表
        this.meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // 传入注册表，供业务埋点使用
        this.store = new BrokerStore(dataDir, meterRegistry);
    }

    public void start() throws InterruptedException {
        // 1. 启动 Metrics HTTP 服务
        Thread metricsThread = new Thread(() -> {
            try {
                new MetricsServer(metricsPort, meterRegistry).start(store);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        metricsThread.setDaemon(true);
        metricsThread.start();
        log.info("metrics started on port {}", metricsPort);

        // 2. 启动核心 MQ 服务
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        /**
                         * LengthFieldBasedFrameDecoder 和 LengthFieldPrepender 处理 TCP 粘包/拆包，
                         * 我们使用的是 4 字节长度头。
                         *
                         * MessageCodec.Encoder/Decoder 负责 Java 对象与字节数组的互相转换，
                         * 当前使用 Jackson JSON 序列化
                         */
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new MessageCodec.Encoder())
                                .addLast(new MessageCodec.Decoder())
                                // 数据进入后，传入 含store的MessageHandler进行处理
                                .addLast(new MessageHandler(store));
                    }
                });

        ChannelFuture future = b.bind(port).sync();
        serverChannel = future.channel();
        log.info("Broker started on port {}", port);

        // 3. 注册 JVM shutdown hook（优雅关闭）
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, gracefully stopping...");
            // 3.1 关闭服务端 socket，不再接受新连接
            if (serverChannel != null) {
                serverChannel.close();
            }
            // 3.2 优雅关闭 worker 和 boss 线程池，等待已接收的请求处理完
            if (workerGroup != null) {
                workerGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS);
            }
            if (bossGroup != null) {
                bossGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS);
            }
            // 3.3 关闭存储层（关闭所有文件通道，正常关闭活跃段）
            try {
                store.close();
            } catch (Exception e) {
                log.error("Error closing store", e);
            }
            log.info("Broker stopped gracefully");
        }));

        // 4. 阻塞直到服务端 channel 关闭（可通过 shutdown hook 关闭）
        future.channel().closeFuture().sync();
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv()
                .getOrDefault("MQ_PORT", "8080"));
        int metricsPort = Integer.parseInt(System.getenv()
                .getOrDefault("METRICS_PORT", "8081"));
        String dataDir = System.getenv()
                .getOrDefault("MQ_DATA_DIR", "./data");

        new BrokerServer(port, metricsPort, dataDir).start();
    }
}