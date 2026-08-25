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

    private static final Logger log = LoggerFactory.getLogger(EnvelopeCodecHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("codec failure from {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
