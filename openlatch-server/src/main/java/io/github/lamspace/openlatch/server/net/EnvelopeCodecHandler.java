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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 编解码异常守卫：分帧/反序列化失败（如超帧长、非法 Protobuf 字节）时记录
 * 日志并断连；本站位紧邻两个解码器之后，接收其异常传播。凡传播至本站位的
 * 其他未处理入站异常同样按不可恢复处理（记 WARN 后断连）——业务分发层
 * 自有 INTERNAL_ERROR 兜底，其正常路径不会有异常漏到此处。可解码但语义
 * 非法的消息（类型与 payload 不匹配等）不属异常、不经本类，由分发层回
 * {@code INVALID_REQUEST}（规格"消息合法性校验"）。
 */
@ChannelHandler.Sharable
public final class EnvelopeCodecHandler extends ChannelInboundHandlerAdapter {

    /** 构造编解码异常守卫（共享实例，无可变状态）。 */
    public EnvelopeCodecHandler() {
    }

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(EnvelopeCodecHandler.class);

    /**
     * 入站异常兜底：记 WARN（远端地址 + 异常摘要）后关闭连接。触发源包括
     * 分帧/反序列化失败及一切传播至本站位的未处理异常（见类注释）；
     * 在所属连接 EventLoop 上执行。
     *
     * @param ctx   通道上下文
     * @param cause 入站异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("codec failure from {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
