package io.github.lamspace.openlatch.server.net;

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.junit.jupiter.api.Test;

import static io.github.lamspace.openlatch.server.net.ServerChannelInitializer.MAX_FRAME_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 分帧行为（设计说明书 §3.1）：半包、粘包、超帧长断连。
 */
class FramingTest {

    private static EmbeddedChannel newFramingChannel() {
        return new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4),
                new ProtobufDecoder(Envelope.getDefaultInstance()),
                new EnvelopeCodecHandler());
    }

    private static ByteBuf frame(Envelope envelope) {
        byte[] payload = envelope.toByteArray();
        ByteBuf buf = Unpooled.buffer(4 + payload.length);
        buf.writeInt(payload.length);
        buf.writeBytes(payload);
        return buf;
    }

    private static Envelope ping(long requestId) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.PING)
                .setRequestId(requestId)
                .build();
    }

    @Test
    void half_packet_is_buffered_until_complete() {
        EmbeddedChannel ch = newFramingChannel();
        ByteBuf frame = frame(ping(7));
        int split = frame.readableBytes() / 2;

        ch.writeInbound(frame.readRetainedSlice(split));
        assertThat((Object) ch.readInbound()).isNull();

        ch.writeInbound(frame);
        Envelope decoded = ch.readInbound();
        assertThat(decoded.getType()).isEqualTo(MessageType.PING);
        assertThat(decoded.getRequestId()).isEqualTo(7);
        ch.finish();
    }

    @Test
    void sticky_packets_are_split_into_envelopes() {
        EmbeddedChannel ch = newFramingChannel();
        ByteBuf combined = Unpooled.wrappedBuffer(frame(ping(1)), frame(ping(2)), frame(ping(3)));

        ch.writeInbound(combined);

        Envelope first = ch.readInbound();
        Envelope second = ch.readInbound();
        Envelope third = ch.readInbound();
        assertThat(first.getRequestId()).isEqualTo(1);
        assertThat(second.getRequestId()).isEqualTo(2);
        assertThat(third.getRequestId()).isEqualTo(3);
        assertThat((Object) ch.readInbound()).isNull();
        ch.finish();
    }

    @Test
    void outbound_write_is_encoded_and_length_prepended() {
        // 出站序：业务 → ProtobufEncoder → LengthFieldPrepender（prepender 更靠近 head）。
        EmbeddedChannel ch = new EmbeddedChannel(
                new LengthFieldPrepender(4),
                new ProtobufEncoder());

        Envelope envelope = ping(9);
        ch.writeOutbound(envelope);

        // 新版 Netty 的 LengthFieldPrepender 将长度头与载荷作为两条消息零拷贝写出，
        // TCP 层按序传输无影响；此处合并后断言帧结构。
        ByteBuf framed = Unpooled.wrappedBuffer((ByteBuf) ch.readOutbound(), (ByteBuf) ch.readOutbound());
        assertThat(framed.readableBytes()).isEqualTo(4 + envelope.getSerializedSize());
        assertThat(framed.readInt()).isEqualTo(envelope.getSerializedSize());
        byte[] payload = new byte[framed.readableBytes()];
        framed.readBytes(payload);
        assertThat(payload).isEqualTo(envelope.toByteArray());
        framed.release();
        assertThat((Object) ch.readOutbound()).isNull();
        ch.finish();
    }

    @Test
    void frame_over_max_length_disconnects() {
        EmbeddedChannel ch = newFramingChannel();
        ByteBuf oversizedHeader = Unpooled.buffer(4);
        oversizedHeader.writeInt(MAX_FRAME_LENGTH + 1);

        ch.writeInbound(oversizedHeader);

        assertThat(ch.isOpen()).isFalse();
    }
}
