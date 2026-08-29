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

package io.github.lamspace.openlatch.protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §10.2 属 M1 的两类协议测试：全消息类型 round-trip 与未知字段容忍。
 */
class ProtocolCodecTest {

    /**
     * 序列化→反序列化回环：{@code toByteArray()} 后 {@code parseFrom} 还原。
     * 解码失败（{@link InvalidProtocolBufferException}）转为 {@link AssertionError}，
     * 使调用方直接对返回值做等值断言而无需处理受检异常。
     *
     * @param envelope 待回环的原信封
     * @return 回环后的信封（应与入参等值）
     */
    private static Envelope roundTrip(Envelope envelope) {
        try {
            return Envelope.parseFrom(envelope.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("decode failed", e);
        }
    }

    /** 场景：HELLO 请求信封回环——协议版本、请求标识与客户端字段整体等值，还原后 payload 分支仍为 HELLO_REQUEST。 */
    @Test
    void helloRequestRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.HELLO)
                .setRequestId(42L)
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(1)
                        .setClientName("app-1")
                        .setAuthToken("")
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getType()).isEqualTo(MessageType.HELLO);
        assertThat(parsed.getPayloadCase()).isEqualTo(Envelope.PayloadCase.HELLO_REQUEST);
    }

    /** 场景：HELLO 响应信封回环——会话 ID、服务端协议版本、默认租约与 leader hint 字段等值保留。 */
    @Test
    void helloResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.HELLO)
                .setRequestId(42L)
                .setHelloResponse(HelloResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setSessionId(99L)
                        .setServerProtocolVersion(1)
                        .setDefaultLeaseMs(30_000L)
                        .setLeaderHint(0L)
                        .build())
                .build();

        assertThat(roundTrip(envelope)).isEqualTo(envelope);
    }

    /** 场景：LOCK_ACQUIRE 请求信封回环（含 wait_ms=-1 排队语义）——整体等值，key 与锁类型还原后逐字段可读。 */
    @Test
    void acquireRequestRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(7L)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("my-lock")
                        .setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1234L)
                        .setLeaseMs(30_000L)
                        .setWaitMs(-1L)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getAcquireRequest().getKey()).isEqualTo("my-lock");
        assertThat(parsed.getAcquireRequest().getLockType()).isEqualTo(LockType.LOCK_TYPE_REENTRANT);
    }

    /** 场景：LOCK_ACQUIRE 的 QUEUED 响应信封回环——位次 3 与凭证/到期字段取 0 的口径等值保留。 */
    @Test
    void acquireResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(7L)
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(StatusCode.QUEUED)
                        .setLeaseToken(0L)
                        .setLeaseExpiresAtMs(0L)
                        .setQueuePosition(3)
                        .setGrantedLeaseMs(0L)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(parsed.getAcquireResponse().getQueuePosition()).isEqualTo(3);
    }

    /** 场景：LOCK_RELEASE 请求信封回环——key、lease token 与 thread_id 等值保留。 */
    @Test
    void releaseRequestRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(8L)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey("my-lock")
                        .setLeaseToken(555L)
                        .setThreadId(1234L)
                        .build())
                .build();

        assertThat(roundTrip(envelope)).isEqualTo(envelope);
    }

    /** 场景：LOCK_RELEASE 的 OK 响应信封回环——整体等值且 fullyReleased=true 布尔字段保留。 */
    @Test
    void releaseResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(8L)
                .setReleaseResponse(ReleaseResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setFullyReleased(true)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getReleaseResponse().getFullyReleased()).isTrue();
    }

    /** 场景：LEASE_RENEW 请求与响应各自回环——含 leaseExpiresAtMs 毫秒纪元大值不截断，均保持等值。 */
    @Test
    void leaseRenewRoundTrip() {
        Envelope request = Envelope.newBuilder()
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(9L)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder()
                        .setKey("my-lock")
                        .setLeaseToken(555L)
                        .setLeaseMs(30_000L)
                        .build())
                .build();
        assertThat(roundTrip(request)).isEqualTo(request);

        Envelope response = Envelope.newBuilder()
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(9L)
                .setLeaseRenewResponse(LeaseRenewResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setLeaseExpiresAtMs(1_700_000_000_000L)
                        .build())
                .build();
        assertThat(roundTrip(response)).isEqualTo(response);
    }

    /** 场景：无 payload 的 PING 信封回环——仅 type 与 request_id 承载信息，还原后 payload 分支为 PAYLOAD_NOT_SET。 */
    @Test
    void pingRoundTrip() {
        // PING 无 payload：Envelope 仅有 type 与 request_id。
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.PING)
                .setRequestId(10L)
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed.getType()).isEqualTo(MessageType.PING);
        assertThat(parsed.getRequestId()).isEqualTo(10L);
        assertThat(parsed.getPayloadCase()).isEqualTo(Envelope.PayloadCase.PAYLOAD_NOT_SET);
    }

    /** 场景：服务端推送 AWAIT_NOTIFY 信封回环——request_id=0、经 request_id_ref 关联原请求，两语义还原后不变。 */
    @Test
    void awaitNotifyPushRoundTrip() {
        // 服务端推送：Envelope.request_id 为 0，通过 request_id_ref 关联原请求。
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.AWAIT_NOTIFY)
                .setRequestId(0L)
                .setAwaitNotify(AwaitNotify.newBuilder()
                        .setKey("my-lock")
                        .setRequestIdRef(7L)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getRequestId()).isZero();
        assertThat(parsed.getAwaitNotify().getRequestIdRef()).isEqualTo(7L);
    }

    /** 场景：未知字段前向兼容——带未知字段（field 999, varint）的字节流可解析、已知字段不受损，且未知字段跨再序列化仍保留。 */
    @Test
    void unknownFieldsAreToleratedAndPreserved() throws InvalidProtocolBufferException {
        Envelope original = Envelope.newBuilder()
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(7L)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("my-lock")
                        .setThreadId(1234L)
                        .build())
                .build();

        // 附加一个模式未知的字段（field 999, varint 12345）。
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(999, UnknownFieldSet.Field.newBuilder()
                        .addVarint(12345L)
                        .build())
                .build();

        byte[] withUnknown = Envelope.parseFrom(original.toByteArray())
                .toBuilder()
                .setUnknownFields(unknown)
                .build()
                .toByteArray();

        Envelope parsed = Envelope.parseFrom(withUnknown);
        assertThat(parsed.getRequestId()).isEqualTo(7L);
        assertThat(parsed.getAcquireRequest().getKey()).isEqualTo("my-lock");
        // 未知字段被保留。
        assertThat(parsed.getUnknownFields().hasField(999)).isTrue();

        // 再次序列化后未知字段仍保留。
        Envelope reparsed = Envelope.parseFrom(parsed.toByteArray());
        assertThat(reparsed.getUnknownFields().hasField(999)).isTrue();
    }

    /** 场景：MessageType/LockType/StatusCode 枚举编号与设计说明书 §3.2 逐项核对（抽查关键取值）。 */
    @Test
    void enumValuesMatchDesignSpecification() {
        // 字段/枚举取值与设计说明书 §3.2 逐项一致（抽查关键取值）。
        assertThat(MessageType.HELLO.getNumber()).isEqualTo(1);
        assertThat(MessageType.LOCK_ACQUIRE.getNumber()).isEqualTo(2);
        assertThat(MessageType.LOCK_RELEASE.getNumber()).isEqualTo(3);
        assertThat(MessageType.LEASE_RENEW.getNumber()).isEqualTo(4);
        assertThat(MessageType.PING.getNumber()).isEqualTo(5);
        assertThat(MessageType.AWAIT_NOTIFY.getNumber()).isEqualTo(6);

        assertThat(LockType.LOCK_TYPE_REENTRANT.getNumber()).isZero();
        assertThat(LockType.LOCK_TYPE_SIMPLE.getNumber()).isEqualTo(1);
        assertThat(LockType.LOCK_TYPE_READ.getNumber()).isEqualTo(2);
        assertThat(LockType.LOCK_TYPE_WRITE.getNumber()).isEqualTo(3);

        assertThat(StatusCode.OK.getNumber()).isZero();
        assertThat(StatusCode.INVALID_TOKEN.getNumber()).isEqualTo(3);
        assertThat(StatusCode.OVERLOADED.getNumber()).isEqualTo(6);
        assertThat(StatusCode.KEY_EMPTY.getNumber()).isEqualTo(8);
        assertThat(StatusCode.INVALID_REQUEST.getNumber()).isEqualTo(9);
    }
}
