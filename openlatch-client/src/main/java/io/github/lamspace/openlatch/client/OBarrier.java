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
 * 跨进程循环屏障（对应 JDK {@code CyclicBarrier} 的协调面投影）。
 *
 * <p><b>职责</b>：多方会合——每世代恰好 {@code parties} 个到场使屏障合拢，
 * 合拢后自动回卷为新世代可反复复用。与本地 {@code CyclicBarrier} 的语义
 * 差异与增强 MUST 逐条知悉：
 * <ul>
 *   <li><b>每次到场/重发是一至多次网络往返</b>（等待为挂起-推送-重发闭环，
 *   等待者无租约、零续租流量）；</li>
 *   <li><b>离场即破障（增强）</b>：JDK 中线程死亡只是静默不再到场，屏障
 *   永远等不满；本原语中任一到场方的离场——超时、中断、进程死亡、显式
 *   {@link #breakBarrier()}——即时打破<em>当前世代</em>，同世代全体等待者
 *   以 {@link OBrokenBarrierException} 收场，新世代不受牵连、无粘滞；</li>
 *   <li><b>破障作用域为世代局部</b>（相对 JDK 的差异）：JDK 的 broken 状态
 *   持续到 {@code reset()}，本原语破障后下一批到场自然开启新世代，
 *   因此不提供 {@code reset()}；</li>
 *   <li><b>异常形态非受检</b>：{@link OBrokenBarrierException} 与
 *   {@link OpenLatchException} 系列替代 JDK 受检异常；</li>
 *   <li><b>在途到场不跨会话重放</b>：连接闪断致会话切换时本方 await 以
 *   {@link OpenLatchException} 终结（到场是有副作用的请求），旧世代因本会话
 *   消亡已被打破，其余到场方以破障裁决感知；</li>
 *   <li><b>{@code barrierAction} 由最后到场方在<em>其进程内</em>执行</b>：
 *   动作完成回报前其余到场方均不放行（忠实 JDK"动作完成前屏障关闭"）；
 *   动作抛异常则本方以该异常终结且同世代全体收破障（等价 JDK 动作异常
 *   破障路径）；执行者进程死亡同样破障。</li>
 * </ul>
 *
 * <p><b>线程模型</b>：句柄可被多线程并发使用（多线程各自到场计次，与 JDK
 * 同构——parties 计的是到场次数，跨进程不区分归属线程）；同一句柄上多个
 * 并发 {@code await} 相互独立。等待可中断（本调用栈），中断视同离场触发
 * 破障（尽力回报，网络不可达时由服务端会话清理兜底）。
 *
 * <p><b>世代状态机（用户可见面）</b>：空世代 → 部分到场（挂起）→ 第 N 次
 * 到场：无动作即时全体放行 / 带动作转"动作待决"（执行者回报后放行）；
 * 任一到场方离场 → 破障 → 新世代。世代号每 key 单调递增，应答与推送
 * 均回显世代，供幂等重发按原世代了结。
 *
 * <p><b>key 治理</b>：定型条目存续不回收（同 key 恒同 parties，跨家族互拒，
 * 同 key 同类型约定适用）。
 */
public interface OBarrier {

    /**
     * 阻塞至所属世代合拢放行，受等待兜底超时约束。
     *
     * <p>判定去留：本方到场计入当前世代（或旧世代重发按了结记录幂等了结）。
     * 同会话内应答丢失由闭环以同一请求 id 自动重发（不重复计次）。
     *
     * @throws InterruptedException  等待被中断（中断视同离场，当前世代破障）
     * @throws OBrokenBarrierException 所属世代已破障（含本方离场连带打破的场景除外——
     *                               本方超时以 {@link OpenLatchTimeoutException}
     *                               表达，其余方收本异常）
     * @throws OpenLatchTimeoutException 等待兜底超时（超时即破障，同世代全体收场）
     * @throws OpenLatchException     服务端显式拒绝（parties 断言不符、无主张加入
     *                               未定型屏障、队列满）或在途到场遇会话切换放弃
     */
    void await() throws InterruptedException;

    /**
     * 限时等待所属世代合拢。超时<b>即破障</b>：本方离场连带打破当前世代，
     * 同世代其余等待者收 {@link OBrokenBarrierException}（对齐 JDK 限时
     * await 超时的全局破障语义）。
     *
     * @param timeout 等待预算（{@code >= 0}）
     * @param unit    时间单位
     * @return {@code true} 世代合拢放行；{@code false} 本方超时（世代已破）
     * @throws InterruptedException     等待被中断（中断即离场，连带破障）
     * @throws OBrokenBarrierException  所属世代已破障
     * @throws OpenLatchException       服务端显式拒绝或会话切换放弃
     * @throws IllegalArgumentException {@code timeout < 0}
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 显式打破当前世代：全体在队等待者收 {@link OBrokenBarrierException}，
     * 此后新到场进入新世代正常合拢。幂等——屏障不存在或当前世代无到场
     * 记录时无操作成功返回。
     *
     * <p>本调用只发一次破障主张（请求经复制多数派提交），瞬态失败以
     * {@link OpenLatchException} 抛出，重试由调用方决定（幂等安全）。
     *
     * @throws OpenLatchException 提交失败（断连、超时、会话失效）
     */
    void breakBarrier();

    /**
     * 本句柄最近一次所见裁决是否为"破障"。句柄本地读数，MUST NOT 发起
     * 网络查询——多进程间不保证实时一致（另一进程刚打破的世代在下一次
     * await 前对本方法不可见）。
     *
     * @return 最近所见世代以破障收场为 {@code true}
     */
    boolean isBroken();

    /**
     * 定型许可数：创建者句柄恒返回主张值；纯加入句柄返回首次应答回显的
     * 定型值（从未获得应答前为 0，表示"未观测"）。
     *
     * @return parties（{@code >= 0}）
     */
    long getParties();

    /**
     * 屏障键。
     *
     * @return key
     */
    String key();
}
