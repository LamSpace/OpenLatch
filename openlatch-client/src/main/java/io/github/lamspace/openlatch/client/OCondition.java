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

import java.util.concurrent.TimeUnit;

/**
 * 分布式条件变量句柄（协议 v9）：与 {@link OLock} 配套的等待-通知通道，
 * 对应 JDK {@link java.util.concurrent.locks.Condition}。
 *
 * <p><b>条件身份 = 命名寻址</b>：条件由 {@code (lock key, name)} 唯一确定——
 * 同 key 上同名重复 {@code newCondition} 得到的句柄、以及不同进程对同 key
 * 同名创建的句柄，均绑定服务端<b>同一个等待集</b>（相对 JDK 句柄身份的本质
 * 适配：跨进程可寻址是能力面，非缺陷）。不同 name 的等待集互相隔离：
 * 对 "x" 的 signal 不扰动 "y" 的等待项。句柄本身<b>无状态、无需关闭</b>——
 * 等待位由服务端三路回收（LEAVE 撤登、会话死亡/断连、换主灭失），
 * 不存在 SDK 侧泄漏面。
 *
 * <p><b>实现形态（等待-通知-重发闭环的折叠复用）</b>：{@code await} 以
 * 携带 {@code condition} 字段的获取请求（折叠形态）挂入既有等待-通知-重发
 * 闭环：受理即完成"全量释放 + 登记等待集"两半程（服务端单关键区原子或
 * 先登记后释放，登记先于释放可见），唤醒经既有队首通知送达后以
 * <b>清除 {@code condition} 字段的同请求标识信封</b>继续普通排队获取直至
 * 授予；{@code signal}/{@code signalAll}/{@code LEAVE} 走独立的即时回执
 * 请求-应答通道（signal 家族零日志、零挂起环）。
 *
 * <p><b>虚假唤醒允许且不承诺杜绝</b>（JDK 同契约）。来源清单：等待队列
 * 队首超时清扫后的推进促醒、换主重挂窗中已丢失 signal 对应的谓词变化、
 * LEAVE 与 SIGNAL 的竞态收敛。<b>守卫循环（guard loop）谓词复查是调用方
 * 义务</b>，标准惯用法：
 * <pre>{@code
 * lock.lock();
 * try {
 *     while (!ready) {        // 永远以谓词复查包裹 await，不裸 await
 *         condition.await();
 *     }
 *     // 此处以持有 1 级重入的状态执行临界区
 * } finally {
 *     lock.unlock();
 * }
 * }</pre>
 *
 * <p><b>返回时持锁保真与重入算术</b>：{@link #await()} 与
 * {@link #await(long, TimeUnit)} 无论被唤醒、超时还是中断，返回（或抛出）
 * 前均已<b>重新持有锁</b>，且重入计数从 <b>1</b> 起——与 await 前持有的
 * N 级无关（JDK 同款算术：await 受理即一步全量释放，重新获取授予 1 级）。
 * 外层配套的 {@code unlock()} 次数相应从 N 次降为 1 次：await 返回后按
 * "持有 1 级"书写解锁逻辑即可，多出的 N-1 次 unlock 会因未持有而抛
 * {@link IllegalMonitorStateException}。
 *
 * <p><b>权限双层（signal 权威在服务端、await 权威在本地）</b>：
 * {@code signal}/{@code signalAll} 本地先行检查——当前线程未持有本锁立即
 * 抛 {@link IllegalMonitorStateException} 且不发请求；服务端权威复查归属
 * （持有已丢的窗口，如锁丢失），线路拒绝码 {@code NOT_HELD} 同样映射为
 * {@link IllegalMonitorStateException}（JDK 异常类型保真，与既有
 * {@link OLock#unlock()} 误用同型）。空等待集、无此 name、key 不存在
 * 的 signal = 无操作正常返回（JDK 对齐）。{@code await} 的权限检查只在
 * 本地进行（误用即抛、零请求）；服务端<b>不查</b> await 登记权限——跨换主
 * 重挂必需（重挂时旧会话持有已随会话关闭释放，新会话非持有者，重挂是
 * 登记半程的重演）。误用/恶意 awaiter 的登记为 ghost：永无 signal 到达则
 * 随会话死亡 / LEAVE / 换主三路收口，人数受服务端合并深度护栏钳制，
 * 且与"谓词竞态"不可区分本就是 JDK 并发常识面。
 *
 * <p><b>租约交互</b>：await 受理即全量释放——该 key 的本地持有簿记消失，
 * 看门狗随之自动停摆；唤醒重新获取签发<b>新租约凭证</b>并自动重启续租，
 * 应用无感。持有者进程死亡或租约到期时，服务端 sweep 释放锁并唤醒
 * <b>等待队列入队者</b>，但<b>不代为唤醒条件等待者</b>——无人 signal 则
 * 条件等待者永睡（JDK 对齐；推荐 {@code await(timeout, unit)} 形态自救）。
 * 等待者自身无租约、无续租义务。
 *
 * <p><b>换主分层语义</b>：<b>等待是承诺</b>——折叠条目经日志重放使释放
 * 确定生效，等待项随获取车道迁移自动重挂（同请求标识幂等登记，对应用
 * 透明，新 Leader 上照常接收 signal 唤醒）；<b>signal 是事件</b>——不重放、
 * 不补偿，换主窗内发出的 signal 随旧等待集合灭失，等待项至多重挂后等待
 * 下一次 signal 或本地超时（有界窗，超时自救面）。
 *
 * <p><b>成本模型</b>：折叠 await 至少 1 次往返（提交受理）+ 唤醒后 1 次
 * 重发往返 + 被授予 1 次往返；{@code signal}/{@code signalAll}/{@code LEAVE}
 * 各 1 次往返即时回执。等待期间无长连接专属资源（服务端仅登记条目）。
 *
 * <p><b>不提供</b>：{@code awaitNanos}、{@code awaitUntil}、
 * {@code awaitUninterruptibly} 及其异步对偶（异步 await 为设计非目标）。
 *
 * <p><b>版本门</b>：需 v9 握手；连接 v8 及以下服务端时握手按既有区间纪律
 * 失败，条件类消息亦被消息级拒绝。升级序必须<b>先服务端后客户端</b>。
 *
 * <p><b>支持面</b>：仅可重入（{@code REENTRANT}）、公平（{@code FAIR}）、
 * 不可重入（{@code SIMPLE}）三互斥形态的 {@link OLock} 支持条件变量；
 * {@link OReadWriteLock} 所得读/写锁句柄调用
 * {@link OLock#newCondition(String)} 抛 {@link UnsupportedOperationException}
 * （本地裁决，不产生任何请求）。
 *
 * <p><b>线程安全</b>：句柄不可变（仅携带 key/name/锁形态），多线程可共用；
 * 每次 await/signal 操作的归属均为<b>调用线程</b>（以
 * {@code Thread.currentThread().threadId()} 为服务端归属键）。
 *
 * <p><b>降级面（相对 JDK 的额外终止原因）</b>：await 阻塞期间连接断开
 * （{@link ServerUnavailableException}）、服务端等待项超限
 * （{@code OVERLOADED}）、形状违例（{@code INVALID_REQUEST}）等以携带状态
 * 码的运行时异常收束——此时调用线程<b>不持有</b>锁（登记或已随会话死亡
 * 回收、或从未受理），按 {@link OLock#onLockLost(LockLostListener)} 与
 * 守卫循环惯例处理；超时/中断路径的"重新获取"本身失败时同样以运行时
 * 异常上抛。虚假唤醒之外的正常收束一律满足"返回时持锁"。
 */
public interface OCondition {

    /**
     * 挂起当前线程直至被唤醒（或被促醒）并重新持有锁。
     *
     * <p>调用前当前线程必须持有配套 {@link OLock}（本地先行检查，误用即抛
     * {@link IllegalMonitorStateException} 且零请求）。进入等待即全量释放
     * 锁（重入计数一步清零、租约清除、看门狗停摆），随后以折叠等待形态
     * 挂起；被唤醒（signal、促醒谓词变化或换主重挂后的下一次 signal）后
     * 重新获取锁，返回时持有 1 级重入。无超时预算——唤醒不到达则持续挂起
     * （{@code await(timeout, unit)} 形态自救）。
     *
     * <p>虚假唤醒允许：返回不代表条件谓词为真，调用方 MUST 以守卫循环复查。
     *
     * @throws InterruptedException 等待期间当前线程被中断——先撤登等待项
     *                              （LEAVE），再重新获取锁后方抛出（抛出时
     *                              持有 1 级重入，JDK 保真；重新获取本身
     *                              失败时以对应运行时异常上抛）
     * @throws IllegalMonitorStateException 当前线程未持有配套锁
     * @throws ServerUnavailableException  等待期间连接断开
     * @throws OpenLatchException          服务端拒绝或传输失败（如等待项
     *                                     超限 {@code OVERLOADED}）
     */
    void await() throws InterruptedException;

    /**
     * 限时等待唤醒；返回时当前线程持有锁。
     *
     * <p>本地计时：总时限内被唤醒（或被促醒）并重新获取锁 → 返回
     * {@code true}；总时限到期仍未收束 → 先撤登等待项（LEAVE，尽力而为），
     * 再转入常规阻塞获取重新入锁，成功入锁后返回 {@code false}——返回
     * {@code false} 时调用线程同样<b>持有锁</b>（重入 1 级），可安全复查
     * 谓词并决策。超时到期与唤醒恰好同时到达时，返回值按先收束者如实
     * 呈现（best-effort 语义）。
     *
     * @param timeout 等待总时限；{@code <= 0} 等价立即超时形态（仍完成
     *                "释放-重新获取"全程后返回 {@code false}）
     * @param unit    时限单位，非空
     * @return 以唤醒路径收束返回 {@code true}；以本地超时路径收束返回
     *         {@code false}
     * @throws NullPointerException unit 为 null
     * @throws InterruptedException 等待期间当前线程被中断——先撤登等待项
     *                              （LEAVE），再重新获取锁后方抛出（抛出时
     *                              持有 1 级重入，JDK 保真；重新获取本身
     *                              失败时以对应运行时异常上抛）
     * @throws IllegalMonitorStateException 当前线程未持有配套锁
     * @throws ServerUnavailableException  等待期间连接断开
     * @throws OpenLatchException          服务端拒绝或传输失败
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 唤醒该条件等待集中的队首一个等待项（按到达序）。
     *
     * <p>权限双层：本地先行——当前线程未持有配套锁立即抛
     * {@link IllegalMonitorStateException} 且不发请求；服务端权威复查
     * 归属，持有已丢窗口时线路码 {@code NOT_HELD} 同样映射为
     * {@link IllegalMonitorStateException}。空集、无此 name、key 不存在
     * 均为无操作正常返回（JDK 对齐，ghost 无害面）。
     *
     * <p>{@code OK} 回执仅表搬运已生效，<b>不承诺唤醒效果</b>：被搬运者
     * 的了结经等待-通知-重发闭环另行完成；signal 是事件不是状态，换主窗
     * 内发出的 signal 不补偿。
     *
     * @throws IllegalMonitorStateException 当前线程未持有配套锁（本地先行），
     *                                      或服务端裁决 {@code NOT_HELD}
     * @throws OpenLatchException 服务端其余错误码（形状违例、会话过期等）
     *                            或传输失败
     */
    void signal();

    /**
     * 唤醒该条件等待集中的全部等待项（按到达序搬运入等待队列）。
     *
     * <p>权限、空集语义与 {@code OK} 回执边界同 {@link #signal()}；
     * 被搬运者的授予经等待队列队首纪律<b>串行</b>兑现（互斥形态一次授予
     * 一人，各自 await 返回时持有 1 级重入）。
     *
     * @throws IllegalMonitorStateException 当前线程未持有配套锁（本地先行），
     *                                      或服务端裁决 {@code NOT_HELD}
     * @throws OpenLatchException 服务端其余错误码或传输失败
     */
    void signalAll();
}
