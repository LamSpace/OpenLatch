/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.server.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Netty ServerBootstrap 构建（设计说明书 §5.2）：boss 1 线程、worker 可配置。
 */
public final class ServerBootstrapFactory {

    /** 工具类禁止实例化。 */
    private ServerBootstrapFactory() {
    }

    /**
     * 创建配置好的 ServerBootstrap：SO_BACKLOG 1024、TCP_NODELAY 开启。
     *
     * @param bossGroup   accept 线程组（1 线程）
     * @param workerGroup IO 线程组
     * @param initializer pipeline 装配器
     * @return 未绑定端口的 ServerBootstrap
     */
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
