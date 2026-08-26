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
 * 编解码异常守卫：分帧/反序列化失败（如超帧长、非法 Protobuf 字节）时记录日志并断连。
 * 可解码但语义非法的消息（类型与 payload 不匹配等）不在此处理，由分发层回
 * {@code INVALID_REQUEST}（规格"消息合法性校验"）。
 */
@ChannelHandler.Sharable
public final class EnvelopeCodecHandler extends ChannelInboundHandlerAdapter {

    /** 构造编解码异常守卫（共享实例，无可变状态）。 */
    public EnvelopeCodecHandler() {
    }

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(EnvelopeCodecHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("codec failure from {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
