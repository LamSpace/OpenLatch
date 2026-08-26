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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.protocol.Envelope;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * 客户端入站唯一收口（详设 §6.1）：解码后的 {@link Envelope} 全部经
 * 信封下沉点分发（由 {@link ConnectionManager#dispatch} 实现：通知与响应分流）；
 * 通道失效事件转交断连下沉点。
 *
 * <p>每个连接装配一个实例，无跨连接共享状态。
 */
final class ClientChannelHandler extends SimpleChannelInboundHandler<Envelope> {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(ClientChannelHandler.class);

    /** 信封下沉点：入站信封的统一分发入口。 */
    private final Consumer<Envelope> envelopeSink;
    /** 断连下沉点：通道失效时调用。 */
    private final Runnable inactiveSink;

    /**
     * 创建入站处理器。
     *
     * @param envelopeSink 信封分发入口
     * @param inactiveSink 断连通知入口
     */
    ClientChannelHandler(Consumer<Envelope> envelopeSink, Runnable inactiveSink) {
        this.envelopeSink = envelopeSink;
        this.inactiveSink = inactiveSink;
    }

    /**
     * 入站信封处理：交给信封下沉点分发。
     *
     * @param ctx 通道上下文
     * @param msg 已解码的信封
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Envelope msg) {
        envelopeSink.accept(msg);
    }

    /**
     * 通道失效：转交断连下沉点（重连状态机统一处理）。
     *
     * @param ctx 通道上下文
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        inactiveSink.run();
    }

    /**
     * 入站异常：记录日志并关闭通道，交由断连路径重建连接。
     *
     * @param ctx   通道上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("client channel exception, closing: {}", cause.toString());
        ctx.close();
    }
}
