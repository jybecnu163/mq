package com.mymq.broker;

import com.mymq.broker.handler.MessageHandler;
import com.mymq.broker.store.BrokerStore;
import com.mymq.common.protocol.MessageCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class BrokerServer {
    private final int port;
    private final BrokerStore store;

    public BrokerServer(int port, String dataDir) throws Exception {
        this.port = port;
        this.store = new BrokerStore(dataDir);
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
            System.out.println("Broker started on port " + port);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new BrokerServer(8080, "./data").start();
    }
}