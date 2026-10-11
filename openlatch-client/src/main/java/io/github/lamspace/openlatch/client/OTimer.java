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
 * 分布式延时触发（JDK {@code java.util.Timer} 定时单次标记的可判定子集，协议 v11）。
 *
 * <p><b>职责</b>：装载一枚会在未来某刻响一次、<b>全体共见</b>的标记——
 * {@link #schedule(long, TimeUnit)} 相对延迟经服务端应用点折算绝对到期时刻
 * 并写入复制账簿（代次 +1、清旧钟）；到点后一切到达的 {@link #await()} /
 * {@link #isFired()} 恒即刻通过（粘滞标记，fire 不需要任何观察者）；
 * {@link #disarm()} 撤销当代装载（代终结，在等旁观者以异常收束）。
 * 服务端账簿为复制状态：装载/撤销经 Raft 多数派确定；**到期本身不是
 * 迁移**——{@code marked = armed ∧ 判定时刻 ≥ fireAt} 是账簿与钟的纯函数，
 * 时钟走过到期点不产生任何日志足迹（"到期不是迁移，是时间的兑现"）。
 * 句柄无状态：同 key 的多个句柄（含跨进程）绑定同一服务端账簿。
 *
 * <p><b>与 {@link ODelayQueue} 延时形态的选型分界</b>：延时队列承载
 * <em>延时交接</em>（一元素恰一消费者、到期即入可见集、不可改期撤销）；
 * 本原语承载<em>广播单次标记</em>（一次装载全体共见、可改期可撤销、
 * 不消耗）。"到期后大家都能看到钟响了"用 {@code OTimer}，"延时把活交给
 * 一个人"用 {@code ODelayQueue}。
 *
 * <p><b>与 JDK 的差异面（全部显式声明）</b>：
 * <ul>
 *   <li><b>降级——无任务载荷与回调执行</b>：服务端不运行用户代码，触发
 *       形态为标记位与等待者唤醒而非 {@code TimerTask}（判例 Phaser 的
 *       onAdvance 同轴论证）；到期动作由观察者 {@link #await()} 返回后
 *       自行执行；</li>
 *   <li><b>降级——无周期重挂</b>：一次装载恰一发（{@code schedule(task,
 *       delay, period)} 不提供）——服务端周期自触即常驻重发变异，与到期
 *       零条目裁决相反；客户端循环 {@link #schedule} 即等价替代；</li>
 *   <li><b>降级——无绝对时刻装载</b>：{@code scheduleAt(Instant)} 不收——
 *       客户端墙钟偏差注入账簿即污染谓词判据；仅收相对延迟（绝对到期
 *       时刻恒由服务端应用点折算，判例 {@code offerDelayed} 同轴）；</li>
 *   <li><b>差异——改期以最新代为准</b>：{@link #schedule} 重装载换代清钟，
 *       已挂起的 {@link #await} 以最新代到期时刻为准（推后多睡/提前先响
 *       皆合法，无"旧代承诺"补偿；推荐 timed await 自救，判例条件
 *       "无人 signal 则永睡"运维提示同轴）；</li>
 *   <li><b>差异——两不可得形态不混读</b>：{@link #await(long, TimeUnit)}
 *       的 {@code false} 仅承载超时；当代被撤销（{@link #disarm()} 先于
 *       到期落地）为 {@link OpenLatchException} 异常收束——"等不到了"与
 *       "还没到"不共享返回值形态；</li>
 *   <li><b>差异——唤醒精度 tick 级</b>：到点唤醒滞后一个服务端扫描周期
 *       （默认 200ms 可配），正确性不依赖精度（谓词恒可重判，判例
 *       {@code ODelayQueue} 同款契约句）；</li>
 *   <li><b>差异——读数 advisory 且钟不统一</b>：{@link #isFired()} /
 *       {@link #isArmed()} / {@link #getRemainingMillis()} 为 Leader 本地
 *       抓取读数（即刻过期是契约，判例 {@code getPhase()}）；"已否到期"
 *       的判定随判定节点本地时钟，跨节点可见偏差 ≤ 节点间时钟偏移
 *       （显式降级声明——管理呈现面恒呈原始三元组、无折算歧义）；</li>
 *   <li><b>差异——每次调用至少一次网络往返</b>；{@code await} 族含挂起
 *       期间的分片保活重发。</li>
 * </ul>
 *
 * <p><b>增强面（强于 JDK）</b>：
 * <ul>
 *   <li>装载者与全体等待者跨进程共见同一次到期（JDK {@code Timer} 进程内
 *       私有且跑完即弃）；</li>
 *   <li><b>装载者进程死亡钟照响</b>：触发绑定 key 不绑定会话（与
 *       {@link OBarrier} "死亡即破障"、{@link OPhaser} "死亡即摘除"并列
 *       的死亡语义第三形态——本原语死亡零扰动，这正是"定时"承诺的与发起方
 *       生命无关的未来事件）；</li>
 *   <li><b>等待谓词跨换主无损自愈</b>：已到期即刻了结、未到期续挂
 *       （沿 {@link OPhaser} 同侧证据，对照 {@link OCondition} signal 丢失
 *       窗的防混读句）。</li>
 * </ul>
 *
 * <p><b>线程模型与幂等</b>：句柄线程安全；服务端以 (会话, 请求 id) 幂等——
 * 应答丢失的自动重发命中装载去重槽回放回声（不双换代）、命中等待登记幂等
 * 续挂；但<em>应用层重复调用</em> {@link #schedule} 即重装载换代（与 JDK
 * 重复 schedule 同判）。变异操作在途超时后跨会话切换会放弃（结果不确定时
 * 以 {@link #isArmed()} 读数核对，不跨会话盲目重放——v10 重挂必须原
 * request_id 的教训护栏由实现承载）。
 *
 * @since v11
 */
public interface OTimer {

    /**
     * 本句柄绑定的延时触发键。
     *
     * @return key
     */
    String key();

    /**
     * 装载/重装载：{@code delay} 后本代到期（应用点折算绝对到期时刻）。
     * 每次成功装载产生一个新代次并清除旧代的粘滞标记（round 语义）。
     *
     * @param delay 相对延迟（{@code >= 0}；0 即立即可共见）
     * @param unit  时间单位（MUST NOT 为 {@code null}）
     * @return 本次装载的代次回显（自 1 起单调递增，观察序）
     * @throws IllegalArgumentException   延迟为负
     * @throws OpenLatchException          服务端拒绝（horizon 越界、家族冲突、会话失效等）
     * @throws OpenLatchTimeoutException   请求超时且结果不确定（以 {@link #isArmed()} 核对）
     */
    long schedule(long delay, TimeUnit unit);

    /**
     * 撤销当代装载（代终结粘滞：当代永不再可标记，直至下一次 {@link #schedule}）。
     * 全体在等旁观者收异常收束（见 {@link #await()}）。幂等：已撤销再撤销
     * 照常返回。
     *
     * @throws OpenLatchException 服务端拒绝（无条目、家族冲突、会话失效等）
     */
    void disarm();

    /**
     * 无限等待当代标记（有界兜底：默认总超时后抛 {@link OpenLatchTimeoutException}
     * 而非永挂，超时后循环重入即 JDK 无限等待的等效惯用法）。
     *
     * @throws InterruptedException           等待中被中断（先尽力撤销挂起登记）
     * @throws OpenLatchException             当代被撤销（等待不可再满足的异常终态）
     * @throws OpenLatchTimeoutException      兜底总超时
     */
    void await() throws InterruptedException;

    /**
     * 限时等待当代标记。
     *
     * @param timeout 等待预算（{@code > 0}）
     * @param unit    时间单位（MUST NOT 为 {@code null}）
     * @return {@code true} 表示在预算内见到当代标记；{@code false} 仅表示超时
     *         （当代被撤销为 {@link OpenLatchException}，两形态不混读）
     * @throws IllegalArgumentException       预算非正
     * @throws InterruptedException           等待中被中断（先尽力撤销挂起登记）
     * @throws OpenLatchException              当代被撤销（等待不可再满足的异常终态）
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 当代是否已到期共见（advisory 读数，判定时刻为 Leader 本地时钟，即刻
     * 过期是契约）。
     *
     * @return 已标记为 {@code true}
     * @throws OpenLatchException 服务端拒绝（无条目等）
     */
    boolean isFired();

    /**
     * 当代是否仍在装（{@code true}=PENDING 待响或已响未撤，{@code false}=
     * 已 {@link #disarm()} 代终结；与 {@link #isFired()} 正交——后者是
     * "已响且未重装载"）。
     *
     * @return 在装为 {@code true}
     * @throws OpenLatchException 服务端拒绝（无条目等）
     */
    boolean isArmed();

    /**
     * 距当代到期还剩多少毫秒（客户端本地差值，advisory：读数即可能过期；
     * 已到期或已撤销返回 0）。
     *
     * @return 剩余毫秒（下限 0）
     * @throws OpenLatchException 服务端拒绝（无条目等）
     */
    long getRemainingMillis();
}
