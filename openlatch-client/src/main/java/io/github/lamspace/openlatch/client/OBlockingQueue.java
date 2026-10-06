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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * 跨进程有界阻塞队列句柄（对应 JDK {@code java.util.concurrent.BlockingQueue} 的
 * 协调面形态，协议 v7）：元素为不透明字节（或 UTF-8 {@code String} 便利形态），
 * 经复制状态机跨进程共享——每次投递/消费都是多数派提交后的权威裁决。
 *
 * <p><b>定位与边界</b>：这是"协调 + 小载荷"的搬运原语，不是消息中间件。单元素
 * 尺寸由服务端入口权威钳制（默认 4KB，{@code openlatch.server.limit.max-value-bytes}）；
 * 容量在建条目时定型（{@code newBlockingQueue(key, capacity)} 的首次写入沉淀，
 * 服务端上限 {@code max-queue-capacity} 默认 1024）。服务端不解释元素字节、
 * MUST NOT 反序列化对象——复杂结构由应用层自行编解码。
 *
 * <p><b>相对 JDK {@code BlockingQueue} 的语义清单</b>（每处降级/增强均在此显式声明）：
 * <ul>
 *   <li>增强：元素绑定 key 而非会话——投递者进程死亡<b>不吞元素</b>（JDK 同进程
 *       堆消散语义在此不成立），元素随复制日志存续至被消费；条目常驻不回收，
 *       忘删 key 即永久驻留（驻留成本见控制台队列观察），服务端无自动清理；</li>
 *   <li>增强：容量无界形态不提供（JDK {@code LinkedBlockingQueue} 默认无界）——
 *       协调面驻留治理要求容量必定型；未消费元素永不因时限被服务端删除；</li>
 *   <li>差异：元素<b>不可为 null</b>（判例 JDK {@code BlockingQueue} rejectNull，
 *       与 {@link OAtomicReference} 的 null 一等公民语义刻意不同——后者 null 合法，
 *       本接口传 null 抛 {@link IllegalArgumentException}）；零长度空字节串是
 *       合法元素；</li>
 *   <li>差异：无 {@code iterator}/{@code contains}/{@code remove(Object)}/
 *       {@code spliterator} 族——O(n) 字节匹配与集合视图在 RTT 面失真，不入面；
 *       {@code add/remove/element} 的 JDK 别名族亦不另设（{@code offer/poll/peek}
 *       即权威形态）；</li>
 *   <li>差异：{@code put}/{@code take} 的挂起在 Leader 本地等待队列承载——
 *       服务端对挂起不设到期期限（判例 Latch await），客户端中断/超时即本地终态、
 *       服务端挂起项由后续事件与会话清理收敛；等待深度超服务端上限
 *       （{@code max-queue-depth-per-key}）时以 {@link OpenLatchException}
 *       （{@code OVERLOADED}）终结本次调用（双"满"判读：元素满立即式回
 *       {@code false}/{@code null}，等待者满抛错）；</li>
 *   <li>差异：{@code poll/offer(timeout)} 的超时为客户端本地计时（挂起-通知环
 *       在预算内自旋，与 JDK 语义等价但唤醒精度为服务端 tick 级）；</li>
 *   <li>降级：每操作至少一次网络往返（{@code drainTo} 一次摘多个是唯一摊薄
 *       通道；{@code remainingCapacity()} 为一次 SIZE 往返的派生读数）；
 *       阻塞方法在超时不确定窗口自动以同序号重发——服务端去重槽保证
 *       PUT 不双插、TAKE 重放交付同一份字节（效果可判，与 {@link OAtomicLong}
 *       族同一裁决）；</li>
 *   <li>差异：阻塞挂起经"等待-通知-重发"闭环（同 {@link OCountDownLatch#await()}
 *       机制），中断响应为本地立即返回；应用点竞态回弹（预检可满足但提交时
 *       被抢先）对调用者透明，表现为等待更久而非虚假失败。</li>
 * </ul>
 *
 * <p><b>版本门</b>：需 v7 握手；对握版本上限 &lt;7 的服务端，首操作以显式
 * {@link OpenLatchException}（{@code INVALID_REQUEST}）表达协议不支持，
 * MUST NOT 重试或降级。
 *
 * <p><b>线程模型</b>：句柄线程安全，可被多线程共享（每操作独立请求；同 key
 * 写侧在途互斥继承 {@link OAtomicLong} 族纪律以保证去重槽充分）。与 JDK 一致，
 * 本接口不提供"本句柄自身挂起者"的公平性承诺——多进程 FIFO 公平体现在
 * 服务端唤醒顺序（先到先唤醒），跨进程抢占顺序不受客户端控制。
 */
public interface OBlockingQueue {

    /**
     * 队列键（跨进程共享标识）。
     *
     * @return 建句柄时给定的 key，非空
     */
    String key();

    /**
     * 本句柄主张的定型容量（随每次写携带作一致性断言）。
     *
     * @return 容量（{@code >= 1}，服务端上限内）
     */
    long capacity();

    /**
     * 阻塞式投递：入队或挂起至有容量。元素随多数派提交跨进程可见，
     * 本会话死亡不回滚已提交投递。
     *
     * @param element 元素字节（非 {@code null}；零长度=空串元素）
     * @throws InterruptedException      挂起中被中断（服务端挂起项由事件/清理收敛）
     * @throws OpenLatchException        服务端显式拒绝（超限/形状/低版本/等待深度满）
     * @throws OpenLatchTimeoutException 请求总时限耗尽（效果不确定窗——同序号
     *                                   重发与去重槽保证不双插，界内裁决至最终应答）
     */
    void put(byte[] element) throws InterruptedException;

    /**
     * UTF-8 字符串便利形态的 {@link #put(byte[])}。
     *
     * @param element 元素字符串（非 {@code null}）
     * @throws InterruptedException      挂起中被中断
     * @throws OpenLatchException        服务端显式拒绝
     * @throws OpenLatchTimeoutException 请求总时限耗尽
     */
    void put(String element) throws InterruptedException;

    /**
     * 立即式投递：有空位即入队，队满立即回 {@code false}（不等候）。
     *
     * @param element 元素字节（非 {@code null}）
     * @return 入队成功为 {@code true}；队满为 {@code false}
     * @throws OpenLatchException 服务端显式拒绝（超限/形状/低版本）
     */
    boolean offer(byte[] element);

    /**
     * UTF-8 字符串便利形态的 {@link #offer(byte[])}。
     *
     * @param element 元素字符串（非 {@code null}）
     * @return 入队成功为 {@code true}；队满为 {@code false}
     * @throws OpenLatchException 服务端显式拒绝
     */
    boolean offer(String element);

    /**
     * 挂起式投递带客户端本地超时：预算内挂起等容量，超时回 {@code false}
     * （等待-通知-重发环在预算内自旋；超时为本地计时语义）。
     *
     * @param element 元素字节（非 {@code null}）
     * @param timeout 等待预算（毫秒，{@code >= 0}；0 等价 {@link #offer(byte[])}）
     * @param unit    时间单位
     * @return 入队成功为 {@code true}；预算耗尽为 {@code false}
     * @throws InterruptedException 等待被中断
     * @throws OpenLatchException   服务端显式拒绝
     */
    boolean offer(byte[] element, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * UTF-8 字符串便利形态的 {@link #offer(byte[], long, TimeUnit)}。
     *
     * @param element 元素字符串（非 {@code null}）
     * @param timeout 等待预算（毫秒，{@code >= 0}）
     * @param unit    时间单位
     * @return 入队成功为 {@code true}；预算耗尽为 {@code false}
     * @throws InterruptedException 等待被中断
     * @throws OpenLatchException   服务端显式拒绝
     */
    boolean offer(String element, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 阻塞式消费：取得可消费元素，空队则挂起至有元素。交付与去重槽绑定——
     * 应答丢失后的同序号重发交付<b>同一份字节</b>，不丢件不偷吃。
     *
     * @return 元素字节（可为零长度空串元素）
     * @throws InterruptedException      挂起中被中断
     * @throws OpenLatchException        服务端显式拒绝（形状/低版本/等待深度满）
     * @throws OpenLatchTimeoutException 请求总时限耗尽
     */
    byte[] take() throws InterruptedException;

    /**
     * UTF-8 字符串便利形态的 {@link #take()}。
     *
     * @return 元素字符串
     * @throws InterruptedException      挂起中被中断
     * @throws OpenLatchException        服务端显式拒绝
     * @throws OpenLatchTimeoutException 请求总时限耗尽
     */
    String takeAsString() throws InterruptedException;

    /**
     * 立即式消费：有可消费元素即摘取，空队立即回 {@code null}（不等候）。
     *
     * @return 元素字节；空队为 {@code null}
     * @throws OpenLatchException 服务端显式拒绝
     */
    byte[] poll();

    /**
     * UTF-8 字符串便利形态的 {@link #poll()}。
     *
     * @return 元素字符串；空队为 {@code null}
     * @throws OpenLatchException 服务端显式拒绝
     */
    String pollAsString();

    /**
     * 挂起式消费带客户端本地超时：预算内挂起等元素，超时回 {@code null}。
     *
     * @param timeout 等待预算（毫秒，{@code >= 0}；0 等价 {@link #poll()}）
     * @param unit    时间单位
     * @return 元素字节；预算耗尽为 {@code null}
     * @throws InterruptedException 等待被中断
     * @throws OpenLatchException   服务端显式拒绝
     */
    byte[] poll(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 批量摘取（一次性提交）：至多摘走当前全部可消费元素，追加进
     * {@code sink}——高吞吐场景的 RTT 摊薄通道。服务端应答预算
     * （{@code max-drain-bytes}）会钳制单次摘取数，实际摘取数以返回值为准。
     *
     * @param sink 收集容器（非空引用；元素按出队序追加）
     * @return 实际摘取数（0 为合法结果——空队不报错）
     * @throws OpenLatchException 服务端显式拒绝
     */
    int drainTo(Collection<? super byte[]> sink);

    /**
     * 批量摘取带上限（一次性提交）。
     *
     * @param sink        收集容器
     * @param maxElements 摘取上限（{@code > 0}；实际另受服务端预算钳制）
     * @return 实际摘取数
     * @throws OpenLatchException 服务端显式拒绝
     */
    int drainTo(Collection<? super byte[]> sink, int maxElements);

    /**
     * 队首读数（不移除；零迁移读条目，读写皆经提交保证线性一致）。
     *
     * @return 队首元素字节；空队为 {@code null}
     * @throws OpenLatchException 服务端显式拒绝
     */
    byte[] peek();

    /**
     * UTF-8 字符串便利形态的 {@link #peek()}。
     *
     * @return 队首字符串；空队为 {@code null}
     * @throws OpenLatchException 服务端显式拒绝
     */
    String peekAsString();

    /**
     * 驻留元素数（延时形态含未到期项——可见性口径以 {@link #peek()}/
     * {@link #take()} 为准）。
     *
     * @return 当前元素数
     * @throws OpenLatchException 服务端显式拒绝
     */
    int size();

    /**
     * 剩余容量（一次 SIZE 往返的派生读数：{@code capacity − size}，非原子快照）。
     *
     * @return 剩余容量（下限 0）
     * @throws OpenLatchException 服务端显式拒绝
     */
    int remainingCapacity();
}
