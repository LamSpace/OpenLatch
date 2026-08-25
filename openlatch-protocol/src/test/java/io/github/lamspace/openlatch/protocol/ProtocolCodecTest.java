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

    private static Envelope roundTrip(Envelope envelope) {
        try {
            return Envelope.parseFrom(envelope.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("decode failed", e);
        }
    }

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
