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
 * 跨进程广播发布/订阅句柄（{@code OTopic}，协议 v8）——OpenLatch 对 JDK
 * {@link java.util.concurrent.Flow.Publisher}/{@link java.util.concurrent.SubmissionPublisher}
 * 协调面形态的映射：一个 key 即一个广播通道，发布者的每条消息尽力交付给
 * 当时在册的全部订阅者。
 *
 * <p><b>交付语义：至多一次（at-most-once）</b>。这是相对 JDK Flow 最重要的
 * 语义降级，逐项显式声明：
 * <ul>
 *   <li>{@code publish} 返回 {@code OK} 仅表示服务端<b>已受理并入扇出</b>，
 *       不承诺任何订阅者收到；订阅者可能因缓冲满、连接中断、换主窗口而丢
 *       消息，服务端不重投、不逐条通知丢失；</li>
 *   <li>无请求门控强背压：不提供 {@code Flow.Subscription.request(n)} 对偶，
 *       慢消费者不反压发布者（见下"弱背压"）；</li>
 *   <li>顺序承诺仅限<b>单订阅内、同一 Leader 任期</b>：同一订阅收到的
 *       {@code topicSeq} 严格升序；跨发布者、跨订阅无全局序；Leader 更替后
 *       序号重新起算（{@code topicSeq} 不是持久游标，跨任期不可比）；</li>
 *   <li>发布重试去重<b>仅同 Leader 内</b>：同一句柄的同序号自动重发命中
 *       服务端每会话去重槽、不双扇出；跨换主的失败重试（含应用层手工重试）
 *       <b>可能双投</b>——消费侧幂等是应用义务；</li>
 *   <li>换主窗口的已发布消息<b>不补投</b>：订阅关系由 SDK 在重连后自动
 *       重挂（对应用透明），但断档期消息不可追回。</li>
 * </ul>
 *
 * <p><b>弱背压为显式契约（drop-newest 两级缓冲）</b>：每订阅服务端在途
 * 缓冲（深度为服务端 {@code max-subscription-buffer}，默认 256 条）满时
 * <b>丢弃最新一条</b>并计数——已入队消息照常交付，发布者不被阻塞、慢订阅者
 * 不被断开（相对 JDK {@code SubmissionPublisher} 的 onOverflow-close 判例
 * 刻意不同：本拓扑订阅者复用会话连接，断开即会话死亡、连带该会话全部
 * 持锁释放，代价不对等）。SDK 本地另有二级缓冲（256 条）同策略。丢弃不逐条
 * 通知，仅经 {@link OTopicSubscription#droppedCount()} 观察（同任期
 * {@code topicSeq} 缺口推断 + 本地溢出计数；跨任期基线重置不累计）。
 *
 * <p><b>订阅生命周期绑定会话</b>：订阅者进程/连接死亡即退订（服务端三路
 * 回收登记、缓冲与去重槽）。这与 {@link OBlockingQueue} 的"死亡不吞元素"
 * <b>看似相仿实则相反</b>——队列承载驻留数据、topic 承载交付事件，数据
 * 需要幸存而事件不必。需要"死亡不丢消息"的场景应使用队列而非 topic。
 * 订阅存续有服务端登记表与缓冲的常驻成本，长驻订阅方应显式
 * {@link #unsubscribe()}。
 *
 * <p><b>载荷</b>：消息体为不透明字节（服务端不解释、无反序列化，对象需
 * 应用层自行编码）；{@code publish} 必携非 null 消息体，零长度空字节串为
 * 合法空消息（与 {@link OAtomicReference} 的 null 语义刻意不同，判例
 * {@link OBlockingQueue} 元素纪律）；逐条尺寸由服务端 {@code max-value-bytes}
 * 入口权威钳制（默认 4KB），超限抛 {@link OpenLatchException}
 * （{@code INVALID_REQUEST}）且零扇出。
 *
 * <p><b>撞 key</b>：topic 键与锁/信号量/Latch/屏障/原子/队列键同名时，
 * 受理节点只读探测拒绝（{@code INVALID_REQUEST}）——尽力而为、无竞态保证。
 * "一 key 一形态"由应用侧维持（相对各复制家族的机制互斥属契约降级，防混读
 * 声明）。
 *
 * <p><b>线程模型</b>：句柄线程安全；{@code publish} 每次一通网络往返
 * （集群含改道开销）；同一 key 的发布经客户端级在途互斥与自动重发
 * （判例原子/队列写车道）。监听器在 SDK dispatcher 线程执行、<b>单订阅内
 * 串行回调</b>（对齐 {@code Flow.Subscriber.onNext} 不重入承诺），绝不占用
 * 网络 EventLoop；回调异常被吞并记录、不中断后续交付。
 *
 * <p><b>版本门</b>：需协议 v8（服务端与客户端双端升级，升级序先服务端后
 * 客户端，判例 v3–v7）；v≤7 会话发送 topic 消息被消息级拒绝。
 *
 * @see OTopicSubscription
 * @see OpenLatchClient#newTopic(String)
 */
public interface OTopic {

    /**
     * 本句柄的 topic 键。
     *
     * @return key（非空）
     */
    String key();

    /**
     * 发布一条消息（同步，阻塞至受理回执或总界限耗尽）。
     *
     * @param payload 消息体字节（非 null——与引用形态 null 语义相反；
     *                零长度合法；超服务端 {@code max-value-bytes} 被拒）
     * @return 服务端受理序号（Leader 任期内按 key 单调，可作本条消息的
     *         任期定位；跨任期不可比）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        不可重试失败（超限/形状拒绝/版本门/
     *                                   会话切换放弃——放弃后效果不确定，
     *                                   重试可能双投）
     * @throws OpenLatchTimeoutException 总界限耗尽（消息可能已受理并扇出——
     *                                   效果不确定，重试可能双投）
     */
    long publish(byte[] payload) throws InterruptedException;

    /**
     * 发布一条 UTF-8 文本消息（{@link #publish(byte[])} 的便利族）。
     *
     * @param text 消息文本（非 null）
     * @return 服务端受理序号
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        不可重试失败
     * @throws OpenLatchTimeoutException 总界限耗尽（效果不确定）
     */
    long publish(String text) throws InterruptedException;

    /**
     * 异步发布（单次发送，不进入自动重发环）：future 以服务端回执完成
     * （受理序号）或以对应失败异常终结。适用于无需受理定序确认的旁路发布；
     * 与 {@link #publish(byte[])} 共享服务端去重槽纪律（同
     * {@code opSeq}——由调用方经重试同句柄重发时——不双扇出）。
     *
     * @param payload 消息体字节（非 null）
     * @return 受理序号 future
     */
    java.util.concurrent.CompletableFuture<Long> publishAsync(byte[] payload);

    /**
     * 建立订阅：向服务端登记本会话对该 key 的兴趣，交付经
     * {@code TOPIC_MESSAGE} 推送、在 SDK dispatcher 线程串行回调。
     * 句柄同一时刻至多一个活跃订阅：重复调用替换前一个订阅（旧句柄
     * 失效并尽力退订，不静默双路由）。
     *
     * @param handler 消息处理器（非 null；单订阅内串行调用）
     * @return 订阅句柄（丢弃观察与显式退订入口）
     * @throws InterruptedException      登记等待被中断
     * @throws OpenLatchException        登记被拒（订阅数达上限
     *                                   {@code REJECT_SUBSCRIBERS}、撞 key、
     *                                   版本门等）
     * @throws OpenLatchTimeoutException 登记总界限耗尽
     */
    OTopicSubscription subscribe(OTopicMessageHandler handler) throws InterruptedException;

    /**
     * 退订本会话在本 key 上的登记（幂等：未订阅时为空操作）。服务端摘除
     * 登记、缓冲与去重槽；此后不再收到该 key 的任何交付（已在途的本地
     * 缓冲消息被丢弃）。连接断开后调用仅清理本地态（服务端登记随会话
     * 死亡自动回收）。
     */
    void unsubscribe();
}
