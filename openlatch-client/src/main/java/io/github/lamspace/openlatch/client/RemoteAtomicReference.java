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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.protocol.AtomicOp;

import java.nio.charset.StandardCharsets;

/**
 * {@link OAtomicReference} 的远程实现——每个方法一次往返（写含同序号自动
 * 重发，车道经 {@link RemoteAtomicBase} 的载荷通道与标量形态同一裁决表），
 * 语义降级/增强声明见 {@link OAtomicReference} 接口注释。
 *
 * <p><b>字符串折叠</b>：{@code String} 便利形态仅在客户端做 UTF-8 编解码，
 * 与服务端视角的字节通道逐项等价；{@code null} 字符串与字节 {@code null}
 * 形态等价（清空/期望 null 态）。
 */
final class RemoteAtomicReference extends RemoteAtomicBase implements OAtomicReference {

    /** 初值主张载荷（{@code null}=不主张；随每条写请求携带作一致性断言）。 */
    private final byte[] claim;

    /**
     * 构造远程有值引用句柄（仅由 {@link OpenLatchClient} 工厂调用）。
     *
     * @param client       所属客户端
     * @param key          有值引用 key
     * @param initialClaim 初值主张载荷（{@code null}=不主张，条目初值即
     *                     null 态；零长度数组=空字节串主张）
     */
    RemoteAtomicReference(OpenLatchClient client, String key, byte[] initialClaim) {
        super(client, key, io.github.lamspace.openlatch.protocol.LockType
                .LOCK_TYPE_ATOMIC_REFERENCE, 0);
        this.claim = initialClaim;
    }

    /**
     * UTF-8 编码（null 透传为 null 态）。
     *
     * @param s 字符串载荷（可为 {@code null}）
     * @return 字节载荷或 {@code null}
     */
    private static byte[] enc(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * UTF-8 解码（null 透传为 null 态）。
     *
     * @param b 字节载荷（可为 {@code null}）
     * @return 字符串载荷或 {@code null}
     */
    private static String dec(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] get() throws InterruptedException {
        return readRef(claim).value();
    }

    @Override
    public String getAsString() throws InterruptedException {
        return dec(get());
    }

    @Override
    public Stamped getStamped() throws InterruptedException {
        RefQuad q = readRef(claim);
        return new Stamped(q.value(), q.version());
    }

    @Override
    public long getVersion() throws InterruptedException {
        return readRef(claim).version();
    }

    @Override
    public void set(byte[] newValue) throws InterruptedException {
        writeRef(AtomicOp.ATOMIC_SET, newValue, null, 0, claim);
    }

    @Override
    public void setString(String newValue) throws InterruptedException {
        set(enc(newValue));
    }

    @Override
    public byte[] getAndSet(byte[] newValue) throws InterruptedException {
        return writeRef(AtomicOp.ATOMIC_GET_AND_SET, newValue, null, 0, claim).oldValue();
    }

    @Override
    public String getAndSetAsString(String newValue) throws InterruptedException {
        return dec(getAndSet(enc(newValue)));
    }

    @Override
    public boolean compareAndSet(byte[] expected, byte[] update) throws InterruptedException {
        return writeRef(AtomicOp.ATOMIC_CAS, update, expected, 0, claim).applied();
    }

    @Override
    public boolean compareAndSetString(String expected, String update) throws InterruptedException {
        return compareAndSet(enc(expected), enc(update));
    }

    @Override
    public boolean compareAndSetStamped(byte[] expectedValue, long expectedVersion,
            byte[] update) throws InterruptedException {
        return writeRef(AtomicOp.ATOMIC_CAS_STAMPED, update, expectedValue,
                expectedVersion, claim).applied();
    }

    @Override
    public boolean compareAndSetStampedString(String expectedValue, long expectedVersion,
            String update) throws InterruptedException {
        return compareAndSetStamped(enc(expectedValue), expectedVersion, enc(update));
    }
}
