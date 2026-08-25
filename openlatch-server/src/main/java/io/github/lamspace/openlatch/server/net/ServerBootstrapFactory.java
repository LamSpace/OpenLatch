package io.github.lamspace.openlatch.server.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Netty ServerBootstrap 构建（设计说明书 §5.2）：boss 1 线程、worker 可配置。
 */
public final class ServerBootstrapFactory {

    private ServerBootstrapFactory() {
    }

    public static ServerBootstrap create(EventLoopGroup bossGroup,
                                         EventLoopGroup workerGroup,
                                         ServerChannelInitializer initializer) {
        return new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(initializer);
    }
}
