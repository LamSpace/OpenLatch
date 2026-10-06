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

/**
 * 跨进程有值引用（对应 JDK {@link java.util.concurrent.atomic.AtomicReference}
 * 的协调面形态，协议 v6 起）：条目装一段<b>不透明字节载荷</b>，附每 key
 * 单调版本戳，提供 get/set/版本 CAS 族操作。
 *
 * <p><b>定位与载荷边界</b>：本原语面向小载荷协调数据（默认上限 4KB，服务端
 * 以 {@code openlatch.server.limit.max-value-bytes} 权威钳制）——载荷全程按
 * 字节传输与比较，服务端 MUST NOT 解释或反序列化内容；应用需要对象形态时
 * 自行编解码（UTF-8 字符串直接走 {@code String} 便利形态）。超限写入以
 * {@link OpenLatchException}（拒绝语义）失败且零生效，SDK MUST NOT 静默
 * 截断或重试（重试只会反复撞同一判定）。
 *
 * <p><b>值域两态</b>：{@code null} 态与零长度空字节串是两个可区分的合法值
 * （线路按显式 presence 表达）——{@code set(null)} 为清空、
 * {@code compareAndSet(null, x)} 可建立空态、空串条目上期望 null 的 CAS
 * 不命中。{@code String} 便利族（{@link #setString}/{@link #compareAndSetString}/
 * {@link #compareAndSetStampedString}/{@link #getAsString}/{@link #getAndSetAsString}）按 UTF-8 编解码，{@code null} 字符串字面量
 * 与字节 {@code null} 形态等价。
 *
 * <p><b>相对 JDK {@code AtomicReference} 的语义降级/增强清单</b>：
 * <ul>
 *   <li>每次操作一次网络往返（本地引用读写纳秒级 vs 跨进程毫秒级），
 *       高争用下吞吐受 RTT 支配；</li>
 *   <li>超时抛出构成<b>效果不确定窗口</b>：写操作抛
 *       {@link OpenLatchTimeoutException} 时值可能已变也可能未变，须以
 *       {@link #getStamped()} 复核；SDK 对可重试失败（NOT_LEADER、断连、
 *       单次超时）以同一 {@code op_seq} 自动重发，服务端去重槽保证重发
 *       不重复生效——同 key 写在本客户端经在途监视器串行；</li>
 *   <li>值 CAS（{@link #compareAndSet}）的 ABA 风险与 JDK 同形；
 *       {@link #compareAndSetStamped} 以版本戳消除 ABA（版本每成功写恰 +1，
 *       任何路径不可回退）；</li>
 *   <li>无 {@code getAndSet} 之外的算术面、无 {@code updateAndGet}/
 *       {@code compareAndExchange} 族（弱 CAS 的"偶然失败"语义在 RTT 面
 *       失去意义，故不提供对应物）；</li>
 *   <li>值不绑定 {@code (sessionId, threadId)} 归属：任何会话的创建、持有、
 *       死亡均不回滚本条目（进程被杀后值纹丝不动是承诺而非巧合）；</li>
 *   <li>条目一经创建常驻不回收（null 值亦存续）——载荷驻留即内存成本，
 *       key 基数治理由部署方承担，无删除语义；</li>
 *   <li>需 v6 握手：对握版本上限低于 6 的服务端，引用形态请求以
 *       {@link OpenLatchException}（{@code INVALID_REQUEST}）显式失败，
 *       SDK 不重发不降级。</li>
 * </ul>
 * 全部方法声明 {@code InterruptedException}（本地等待可中断，中断标志被
 * 复位）；本接口无租约、无看门狗、不触发 {@link LockLostListener}，操作
 * MUST NOT 进入等待-通知-重发闭环（全部即时线性化应答）。
 *
 * @see OpenLatchClient#newAtomicReference(String)
 */
public interface OAtomicReference {

    /**
     * 载荷与版本戳的不可变读数对（{@link #getStamped()} 产物）。
     *
     * <p>载荷分量为字节数组：record 的 {@code equals} 对数组按引用比较，
     * 内容判等请用 {@link java.util.Arrays#equals}（或经
     * {@link #valueAsString()} 折叠后比较字符串）。
     *
     * @param value   当前载荷（{@code null}=null 态、零长度=空字节串）
     * @param version 版本戳（服务端每成功写恰 +1）
     */
    record Stamped(byte[] value, long version) {

        /**
         * 载荷的 UTF-8 字符串读数（null 态回 {@code null}）。
         *
         * @return 字符串载荷或 {@code null}
         */
        public String valueAsString() {
            return value == null ? null : new String(value, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /**
     * 本引用的 key（服务端条目定位，与工厂入参一致）。
     *
     * @return key
     */
    String key();

    /**
     * 读当前载荷（一次网络往返；不推进版本戳，key 不存在时回 {@code null}
     * 且不建条目）。
     *
     * @return 当前载荷字节（可为 {@code null}）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    byte[] get() throws InterruptedException;

    /**
     * 读当前载荷的 UTF-8 字符串形态。
     *
     * @return 字符串载荷（可为 {@code null}）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    String getAsString() throws InterruptedException;

    /**
     * 读载荷与版本戳（单往返原子读数）。
     *
     * @return (value, version) 读数对
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    Stamped getStamped() throws InterruptedException;

    /**
     * 读版本戳（等价 {@code getStamped().version()}，单往返）。
     *
     * @return 当前版本戳
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    long getVersion() throws InterruptedException;

    /**
     * 置新载荷（一次网络往返；{@code newValue} 为 {@code null} 即清空态，
     * 零长度数组为空字节串态——两态可区分）。
     *
     * @param newValue 新载荷字节（可为 {@code null}）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    void set(byte[] newValue) throws InterruptedException;

    /**
     * 置新载荷的 UTF-8 字符串形态（{@code null} 即清空）。
     *
     * @param newValue 新载荷字符串（可为 {@code null}）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    void setString(String newValue) throws InterruptedException;

    /**
     * 置新载荷并返回旧载荷。
     *
     * @param newValue 新载荷字节（可为 {@code null}）
     * @return 置位前的旧载荷（可为 {@code null}）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    byte[] getAndSet(byte[] newValue) throws InterruptedException;

    /**
     * 置新载荷（UTF-8 字符串形态）并返回旧载荷的字符串形态。
     *
     * @param newValue 新载荷字符串（可为 {@code null}）
     * @return 置位前的旧载荷字符串（可为 {@code null}）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    String getAndSetAsString(String newValue) throws InterruptedException;

    /**
     * 值 CAS：当前载荷与 {@code expected} 字节内容相等（含
     * {@code null==null}、空串≠null）时落 {@code update}。ABA 风险由契约
     * 声明，需消除用 {@link #compareAndSetStamped}。
     *
     * @param expected 期望载荷（可为 {@code null}=期望 null 态）
     * @param update   更新载荷（可为 {@code null}=更新为 null 态）
     * @return 是否落值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    boolean compareAndSet(byte[] expected, byte[] update) throws InterruptedException;

    /**
     * 值 CAS 的 UTF-8 字符串形态。
     *
     * @param expected 期望字符串（可为 {@code null}）
     * @param update   更新字符串（可为 {@code null}）
     * @return 是否落值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    boolean compareAndSetString(String expected, String update) throws InterruptedException;

    /**
     * 值 + 版本双 CAS（ABA-free）：载荷与 {@code expectedValue} 字节相等
     * 且版本戳恰为 {@code expectedVersion} 才落 {@code update}。
     *
     * @param expectedValue   期望载荷（可为 {@code null}）
     * @param expectedVersion 期望版本戳（须为 {@code getStamped()} 读数）
     * @param update          更新载荷（可为 {@code null}）
     * @return 是否落值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    boolean compareAndSetStamped(byte[] expectedValue, long expectedVersion, byte[] update)
            throws InterruptedException;

    /**
     * 值 + 版本双 CAS 的 UTF-8 字符串形态。
     *
     * @param expectedValue   期望字符串（可为 {@code null}）
     * @param expectedVersion 期望版本戳
     * @param update          更新字符串（可为 {@code null}）
     * @return 是否落值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝（含超限）或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    boolean compareAndSetStampedString(String expectedValue, long expectedVersion, String update)
            throws InterruptedException;
}
