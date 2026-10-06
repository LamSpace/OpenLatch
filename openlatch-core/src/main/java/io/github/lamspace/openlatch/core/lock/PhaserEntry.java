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

package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreInspection;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.PhaserOpType;
import io.github.lamspace.openlatch.core.command.PhaserOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.PhaserOpResult;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单 key 的相位器（JDK {@code Phaser} 可判定分布式子集）状态机。
 *
 * <p><b>职责</b>：承载动态注册/离场与按相位会合的复制侧账簿——注册配额
 * （按会话记账）、当前相位到场计数、相位推进判定、到场去重、挂起等待
 * 集的唤醒与三路回收。操作集为 {@link #phaserOp} 单一命令通道（七操作词
 * 表），经 {@link io.github.lamspace.openlatch.core.CoreEngine} 门面分派。
 *
 * <p><b>状态要素</b>（复制侧账簿 vs Leader 本地态的分界以 {@code 快照} 为
 * 准——前者入日志与快照、跨副本确定重放，后者进程易失、换主清零）：
 * <ul>
 *   <li>{@code phase}（相位号，自 0 起单调递增、不取模、永不回退——
 *       与屏障"世代自 1 起"的差别仅是起始值，JDK {@code getPhase()} 自 0
 *       对偶）；</li>
 *   <li>{@code registered}（注册总数）与每会话注册配额
 *       {@code partyQuota}（{@code sessionId → parties}——死亡摘除与
 *       {@code ARRIVE_AND_DEREGISTER} 扣减的归属域；条目锁内读写、快照
 *       导出按会话 id 升序保证确定性）；</li>
 *   <li>{@code arrived}（当前相位到场计数）与在场去重槽 {@code slots}
 *       （{@code (sessionId, requestId)} 插入序集合——应答丢失重发与重放
 *       的单计数判据）。计数与槽刻意分离：会话死亡摘除槽（含上一窗口）但
 *       <b>MUST NOT 回退计数</b>（"已到场事实不撤销"——发生过的计次是
 *       复制态事实，与队列"死亡不吞元素"对偶；槽残留死者身份会向快照
 *       泄漏已消亡会话的内部 sid，摘槽不摘账即免疫）。推进换代时旧槽滚动
 *       入 {@code prevPhase}/{@code prevSlots}（有界窗口，仅保最近一次
 *       推进——跨推进的到场重发据此终态回显且不双计数，判例屏障"最近完结
 *       世代了结记录"；更早窗口的迟到重发按新到场计，显式声明竞态，窗口
 *       下界同屏障完结世代账簿口径）；</li>
 *   <li>挂起等待集 {@code waiters}（{@code (sessionId, requestId) →
 *       expectedPhase} 到达序队列）——<b>Leader 本地态，不入日志与
 *       快照</b>；唤醒即出集（不留"已通知待重发"位——了结由纯谓词
 *       {@code phase > expectedPhase} 承载，重发/重挂命中即 {@code OK}，
 *       无需有界了结记录窗口，这是相对屏障"了结记录"的结构性简化）。</li>
 * </ul>
 *
 * <p><b>状态机（单相位视角）</b>：
 * <pre>
 *   应到集合(registered) ──REGISTER──▶ 扩大（不触发推进——新注册者
 *         │                              须到场才计入当相位）
 *         ├──ARRIVE 族──▶ arrived++ ──▶ arrived&gt;0 ∧ arrived≥registered ?
 *         │                                ├─是─▶ 推进：phase++、arrived=0、
 *         │                                │        清槽、全员唤醒出集（事件驱动）
 *         │                                └─否─▶ 续等（等待登记/旁观续挂）
 *         ├──ARRIVE_AND_DEREGISTER──▶ 同关键区 arrived++ ∧ 配额−1（零配额拒）
 *         ├──SESSION_CLOSE 摘除──▶ registered−=配额 ∧ 槽摘 ∧ 等待摘 ──▶ 重判推进
 *         └──registered 归零──▶ 空转（不终止、不推进；后续 REGISTER 自当前相位复活）
 * </pre>
 *
 * <p><b>合拢判据</b>：{@code arrived > 0 ∧ arrived ≥ registered}（等价于
 * JDK"所有已注册方均已到场"，含"应到集合因死亡/离场缩小后已满足"的提前
 * 推进形态；{@code arrived==0} 时恒不推进——纯缩减（如全员未到场死亡）
 * 归零属空转，无事实可推进）。不变式：每处状态迁移（注册/到场/离场/
 * 死亡摘除）之后同关键区内执行判据；推进后 {@code arrived=0 ∧ slots=∅}，
 * 而 {@code registered} 存续（注册跨相位有效，离场才会扣减——JDK 同判）。
 *
 * <p><b>护栏分界（配置非判定——"钳制属接入层"判例纪律）</b>：本条目
 * MUST NOT 依据 {@code max-parties-per-phaser} 拒绝注册——该判定唯一在
 * 受理点（单机门面/集群 Leader 预检），条目应用侧恒宽容（已提交条目在
 * 任何配置下照常回放，否则节点配置漂移将撕裂账簿——上限被竞态穿透的
 * 溢出界为受理并发度，属声明的有界宽容面）。唯一消费 {@code cfg} 的
 * 规则是 {@code AWAIT_ADVANCE} 的合并等待深度
 * （{@code maxQueueDepthPerKey}，超限 {@link Outcome#REJECT_QUEUE_FULL}）；
 * {@code ARRIVE_AND_AWAIT} 的等待半程恒宽容登记（到场事实已入账簿，
 * 拒绝挂起会把调用方悬在"到场未等待"的中间态——深度护栏对本请求族
 * 以到场计数为代价让步，判例 v9 ghost 宽容面）。
 *
 * <p><b>幂等口径</b>：{@code (sessionId, requestId)} 为到场与等待的双重
 * 身份位——同请求重发在任一时点（在场槽、挂起集、已推进后）均不双计数、
 * 不双推进、不双登记，应答回显当前账簿快照；{@code CANCEL} 幂等（未
 * 存在亦 {@code GRANTED}）；{@code REGISTER} 无幂等键（重复注册即重复
 * 加配额——JDK {@code register()} 同款语义，SDK 不自动重执，会话重建
 * 后的重注册属应用/工厂层的显式意图）。
 *
 * <p><b>与租约机制的关系</b>：无租约家族——{@link #leaseToken()}/
 * {@link #leaseExpiresAtMs()} 恒 0，永不入到期堆，{@link #forceExpire}
 * 不可达；等待者无租约，断连随 {@link #removeSession} 摘除（"死亡即退
 * 订"与 topic 订阅同口径——等待是订阅，不是持有）。
 *
 * <p><b>并发模型</b>：与 {@link BarrierEntry} 相同——全部迁移在
 * {@code synchronized(this)} 内完成；唤醒收集经 {@code notify} 参数、
 * 由调用方在条目锁外触发（本类 MUST NOT 持锁外呼）；时钟一律由调用方
 * 注入（{@code now}），本类不读系统时钟。等待项被唤醒即出集，故本条目
 * 无"已通知队首"形态——{@link #sweepNotifiedHead} 恒 {@code false}。
 *
 * <p><b>条目存续</b>：{@code isEmpty()} 恒 {@code false}——相位单调与配额
 * 账簿依赖条目存续（回收归零会丢"key 即 phaser 身份"的空转复活语义，
 * 判例循环屏障的条目存续约定）；无上限 key 由引擎 maxKeys 护栏兜底。
 */
public final class PhaserEntry implements KeyEntry {

    /**
     * 在场到场身份：(会话, 请求) 二元组，当前相位去重槽的键（插入序，
     * 快照导出按二元组升序确定性化）。
     *
     * @param sessionId 到场会话
     * @param requestId 到场操作的请求 id
     */
    public record Arrival(long sessionId, long requestId) {
    }

    /**
     * 每会话注册配额读数（观察面与快照导出形态）。
     *
     * @param sessionId            会话（引擎内部 sid，镜像侧折算逻辑 id）
     * @param parties              未离场的注册配额数（恒 &gt; 0，行存在即非零）
     * @param lastRegisterRequestId 该会话最近一次 REGISTER 的请求 id（0=尚无；
     *                              同 id 重发幂等回显、不双加配额——判例 v7
     *                              队列每会话去重槽， REGISTER 无天然换代故槽
     *                              不随推进清空、随行存续）
     * @param lastRegisterPhase    该次注册的到场相位回显（幂等重发回显源）
     */
    public record PartyView(long sessionId, int parties,
            long lastRegisterRequestId, long lastRegisterPhase) {
    }

    /**
     * 挂起等待项读数（观察面专用；已唤醒项不出现在此）。
     *
     * @param sessionId     等待会话
     * @param requestId     等待请求 id（唤醒通知的关联键）
     * @param expectedPhase 已见相位号（唤醒谓词=当前相位 &gt; 本值）
     * @param enqueuedAtMs  登记时刻（引擎时钟，毫秒；换主重挂后重新计时）
     */
    public record WaiterView(long sessionId, long requestId, long expectedPhase, long enqueuedAtMs) {
    }

    /**
     * 条目的复制态全量导出（影子表镜像与摘要比对的数据源）：仅含账簿，
     * 不含等待集（Leader 本地态）。列表均为确定性序。
     *
     * @param phase        当前相位号
     * @param registered   注册总数
     * @param arrived      当前相位到场计数
     * @param parties      配额表（会话 id 升序）
     * @param arrivals     在场去重槽（(会话,请求) 升序）
     * @param prevPhase    上一推进周期到场相位（-1=尚无换代）
     * @param prevArrivals 上一推进周期到场槽（升序；跨推进重发的终态判据）
     */
    public record ReplicatedState(long phase, int registered, int arrived,
            List<PartyView> parties, List<Arrival> arrivals,
            long prevPhase, List<Arrival> prevArrivals) {
    }

    /**
     * 挂起等待项（不可变；唤醒即出集，无"已通知"中间态）。
     *
     * @param sessionId     等待会话（引擎内部 sid）
     * @param requestId     等待请求 id（唤醒通知的关联键）
     * @param expectedPhase 已见相位号（唤醒谓词=当前相位 &gt; 本值）
     * @param enqueuedAtMs  登记时刻（毫秒，观察值）
     */
    private record Wait(long sessionId, long requestId, long expectedPhase, long enqueuedAtMs) {
    }

    /** 相位器键。 */
    private final String key;
    /** 当前相位号（自 0 起，每次合拢恰 +1，永不回退）。 */
    private long phase;
    /** 注册总数（= 各会话配额之和，双写一致性由同关键区迁移维护）。 */
    private int registered;
    /** 每会话注册配额（(会话 → parties)；死亡与离场是唯一减项）。 */
    private final Map<Long, PartyView> partyQuota = new LinkedHashMap<>();
    /** 当前相位到场计数（推进后清零；不随会话死亡回退）。 */
    private int arrived;
    /** 当前相位在场去重槽（插入序；推进时滚动入 {@code prevSlots} 后换新）。 */
    private Set<Arrival> slots = new LinkedHashSet<>();
    /** 上一推进周期的到场相位号（-1=尚无换代）。 */
    private long prevPhase = -1L;
    /** 上一推进周期的到场槽（跨推进重发的终态回显判据；再推进即滚出）。 */
    private Set<Arrival> prevSlots = new LinkedHashSet<>();
    /** 挂起等待集（到达序；Leader 本地态，不入快照）。 */
    private final ArrayDeque<Wait> waiters = new ArrayDeque<>();

    /**
     * 构造相位器条目（定型通道：PHASER 家族唯一的 REGISTER 建条目路径，
     * 引擎门面在无条目时装配；初始为空转账簿——phase 0、注册 0）。
     *
     * @param key 相位器键
     */
    public PhaserEntry(String key) {
        this.key = key;
    }

    /**
     * 快照重建工厂：以复制账簿直写装配，不经迁移规则（恢复不重演注册/
     * 到场判定）；等待集恢复恒空（Leader 本地态，客户端重挂补登记）。
     *
     * @param key          相位器键
     * @param phase        当前相位号
     * @param registered   注册总数
     * @param arrived      当前相位到场计数
     * @param parties      配额表（会话 id 升序导入）
     * @param arrivals     在场去重槽（升序导入）
     * @param prevPhase    上一推进周期到场相位（-1=无换代窗口）
     * @param prevArrivals 上一推进周期到场槽（升序导入）
     * @return 快照初态的条目
     */
    public static PhaserEntry restored(String key, long phase, int registered, int arrived,
            List<PartyView> parties, List<Arrival> arrivals,
            long prevPhase, List<Arrival> prevArrivals) {
        PhaserEntry e = new PhaserEntry(key);
        e.phase = phase;
        e.registered = registered;
        e.arrived = arrived;
        for (PartyView p : parties) {
            e.partyQuota.put(p.sessionId(), p);
        }
        e.slots = new LinkedHashSet<>(arrivals);
        e.prevPhase = prevPhase;
        e.prevSlots = new LinkedHashSet<>(prevArrivals);
        return e;
    }

    /**
     * 统一操作入口（单机受理形态，等价 {@code clusterMode=false} 的便捷重载）。
     *
     * @param cmd    操作命令（形状与建条目预检已由门面完成）
     * @param now    当前时刻（毫秒，登记时刻观察值）
     * @param cfg    限额配置（仅消费合并等待深度）
     * @param notify 唤醒收集列表，推进发生时由调用方在条目锁外触发
     * @return 操作结果（三计数为裁决时刻账簿快照）
     */
    public synchronized PhaserOpResult phaserOp(PhaserOpCommand cmd, long now,
            CoreConfig cfg, List<Waiter> notify) {
        return phaserOp(cmd, now, cfg, notify, false);
    }

    /**
     * 统一操作入口（七操作词表分派）。判定前置（会话有效、key 合法、
     * 家族匹配、形状防御）由引擎门面完成，抵达本方法的命令视为形状合法；
     * 全部规则在条目锁单关键区内依下列序执行：
     *
     * <ol>
     *   <li><b>REGISTER</b>：{@code parties} 加计入调用会话配额与注册总数
     *       （注册者进入<b>当前</b>相位的应到集合、不视为到场——JDK 同判）；
     *       MUST NOT 触发推进判定；回显当前相位。</li>
     *   <li><b>到场族</b>（ARRIVE / ARRIVE_AND_AWAIT / ARRIVE_AND_DEREGISTER）：
     *       先查在场槽——命中即幂等回显（不双计数；{@code ARRIVE_AND_AWAIT}
     *       的重复经步骤 5 的等待路径判定"续挂/已推进"）；未命中则
     *       {@code ARRIVE_AND_DEREGISTER} 须有本会话配额（零配额
     *       {@link Outcome#REJECT_PHASER_QUOTA}，先于任何迁移），随后
     *       {@code arrived++} 入槽、{@code ARRIVE_AND_DEREGISTER} 同关键区
     *       扣配额与注册总数，最后执行推进判定。到场恰触发合拢时
     *       {@code ARRIVE_AND_AWAIT} 直答完成（回显到场时相位——推进结果
     *       经后续查询可见），不发生等待。</li>
     *   <li><b>推进判定</b>（{@code arrived > 0 ∧ arrived ≥ registered}）：
     *       {@code phase++}、{@code arrived=0}、清槽；唤醒出集——挂起项
     *       恒满足 {@code expected < 新相位}（登记谓词保证），全员标记
     *       收集进 {@code notify} 并即时出集（无"已通知待重发"位，见类
     *       注）。事件驱动零定时器。</li>
     *   <li><b>AWAIT_ADVANCE</b>：纯谓词判定——{@code phase > expected}
     *       即刻完成（重挂/迟到/旁观已越相位形态）；否则查 (会话,请求)
     *       幂等——在集即续挂回 {@link Outcome#QUEUED}，不在集则受合并
     *       深度护栏（{@code maxQueueDepthPerKey}，超限
     *       {@link Outcome#REJECT_QUEUE_FULL}、等待集零扰动），通过则入集
     *       回 {@link Outcome#QUEUED}。</li>
     *   <li><b>ARRIVE_AND_AWAIT 等待半程</b>：到场与推进判定完成后，若
     *       相位未因本次到场推进，以到场时相位登记等待（恒宽容——深度
     *       护栏不拒已到场者的挂起，理由见类注"护栏分界"）。</li>
     *   <li><b>CANCEL</b>：按 (会话, 请求) 摘除挂起项，幂等完成（未存在
     *       亦 {@link Outcome#GRANTED}——判例条件 LEAVE）。</li>
     *   <li><b>QUERY</b>：三计数只读回显，零迁移零推进。</li>
     * </ol>
     *
     * @param cmd        操作命令（形状与建条目预检已由门面完成）
     * @param now        当前时刻（毫秒，登记时刻观察值）
     * @param cfg        限额配置（仅消费合并等待深度）
     * @param notify     唤醒收集列表，推进发生时由调用方在条目锁外触发
     * @param clusterMode 集群应用形态：true 时等待半程 MUST NOT 登记条目内
     *                    （等待簿记归服务端 {@code PhaserRegistry}，判例
     *                    "集群引擎恒不登记等待项"与 v9 条件双拓扑），且
     *                    AWAIT_ADVANCE/CANCEL/QUERY 抵达即拒
     * @return 操作结果（三计数为裁决时刻账簿快照）
     */
    public synchronized PhaserOpResult phaserOp(PhaserOpCommand cmd, long now,
            CoreConfig cfg, List<Waiter> notify, boolean clusterMode) {
        if (clusterMode && (cmd.op() == PhaserOpType.AWAIT_ADVANCE
                || cmd.op() == PhaserOpType.CANCEL || cmd.op() == PhaserOpType.QUERY)) {
            // 集群形态本地操作词 MUST NOT 抵达条目（受理点直连服务端簿记）：
            // 抵达即门面分派缺陷，拒绝零扰动。
            return PhaserOpResult.rejected(Outcome.REJECT_PHASER_NO_ENTRY);
        }
        switch (cmd.op()) {
            case REGISTER -> {
                long sid = cmd.sessionId();
                PartyView prior = partyQuota.get(sid);
                if (prior != null && prior.lastRegisterRequestId() == cmd.requestId()) {
                    // 同请求重发：幂等回显首计时的到场相位，不双加配额。
                    return PhaserOpResult.granted(prior.lastRegisterPhase(),
                            registered, arrived);
                }
                int parties = (prior == null ? 0 : prior.parties()) + cmd.parties();
                partyQuota.put(sid, new PartyView(sid, parties,
                        cmd.requestId(), phase));
                registered += cmd.parties();
                return PhaserOpResult.granted(phase, registered, arrived);
            }
            case ARRIVE, ARRIVE_AND_AWAIT, ARRIVE_AND_DEREGISTER -> {
                return arrive(cmd, now, notify, clusterMode);
            }
            case AWAIT_ADVANCE -> {
                return awaitAdvance(cmd, now, cfg);
            }
            case CANCEL -> {
                waiters.removeIf(w -> w.sessionId() == cmd.sessionId()
                        && w.requestId() == cmd.awaitRequestId());
                // 幂等完成：未存在（已唤醒出集/从未登记）亦成功——撤销是
                // 尽力收口，不是状态断言（判例 v9 LEAVE）。
                return PhaserOpResult.granted(phase, registered, arrived);
            }
            case QUERY -> {
                return PhaserOpResult.granted(phase, registered, arrived);
            }
            default -> {
                return PhaserOpResult.rejected(Outcome.REJECT_PHASER_NO_ENTRY);
            }
        }
    }

    /**
     * 会话摘除（{@code SESSION_CLOSE} 应用点与单机断连同路径）：
     * 该会话配额自注册总数减除、配额行删除、在场槽摘除（<b>计数不回退
     * ——已到场事实不撤销</b>）、挂起等待项摘除（"死亡即退订"，等待者
     * 无租约无账可碰）；摘除后执行推进判定（应到集合缩小可即时推进并
     * 唤醒——"死亡不空转"的机制本体）。该会话不在配额表且无等待项时
     * 零扰动（旁观等待之外的死亡与本条目无牵连）。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒——本条目无
     *                           "已通知"形态，忽略）
     * @param notify             通知收集列表，摘除驱动推进时由调用方锁外触发
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        PartyView quota = partyQuota.remove(sessionId);
        if (quota != null) {
            registered = Math.max(0, registered - quota.parties());
        }
        slots.removeIf(a -> a.sessionId() == sessionId);
        prevSlots.removeIf(a -> a.sessionId() == sessionId);
        waiters.removeIf(w -> w.sessionId() == sessionId);
        if (quota != null) {
            tripIfComplete(now, notify);
        }
    }

    /**
     * 不可达：相位器无租约、永不入到期堆（引擎 {@code expireDue} 无此
     * 条目的堆记录）。以断言级无操作实现，防御调用路径意外触达。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（相位器条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("phaser entry has no lease to expire");
    }

    /**
     * 恒 {@code false}：本条目无"已通知待重发"形态（唤醒即出集，重发的
     * 了结由 {@code phase > expected} 纯谓词承载），无清扫对象——接口
     * 契约的诚实空实现，MUST NOT 被误用作等待集周期清扫钩子。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表
     * @return 恒 {@code false}
     */
    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        return false;
    }

    @Override
    public KeyFamily family() {
        return KeyFamily.PHASER;
    }

    @Override
    public String key() {
        return key;
    }

    /** 恒 0：相位器无租约（契约读数占位，到期堆永不登记本条目）。 */
    @Override
    public long leaseToken() {
        return 0;
    }

    /** 恒 0：相位器无到期时刻（{@link #leaseToken()} 同理）。 */
    @Override
    public long leaseExpiresAtMs() {
        return 0;
    }

    /**
     * 恒 {@code false}：条目不随注册散尽或相位存续回收（相位单调与空转
     * 复活语义依赖条目存续，见类注）。
     *
     * @return 恒 {@code false}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * 挂起等待项读数（统计观察面；与锁/BARRIER 统一"本 key 当前排队
     * 等待项数"口径——等待就是等待，v9 合并计数纪律经 v10 延伸）。
     * 须在持有条目锁时调用。
     *
     * @return 挂起等待者数
     */
    @Override
    public synchronized int waiterCount() {
        return waiters.size();
    }

    /**
     * 当前相位号（观察面与镜像导出读取；须持锁）。
     *
     * @return 当前相位号
     */
    public synchronized long phase() {
        return phase;
    }

    /**
     * 注册总数（观察面与受理点配额预检读取；须持锁）。
     *
     * @return 注册总数
     */
    public synchronized int registered() {
        return registered;
    }

    /**
     * 当前相位到场计数（观察面读取；须持锁）。
     *
     * @return 当前相位到场计数
     */
    public synchronized int arrived() {
        return arrived;
    }

    /**
     * 配额表快照（账簿插入序=注册先后——跨副本确定性的导出序：引擎内部 sid
     * 各副本不同，MUST NOT 按其排序；判例屏障到场账簿的插入序约定）。
     * 须持锁。
     *
     * @return 不可变配额行列表（插入序）
     */
    public synchronized List<PartyView> partiesSnapshot() {
        return List.copyOf(partyQuota.values());
    }

    /**
     * 挂起等待明细快照（登记到达序；须持锁）。
     *
     * @return 不可变等待项列表
     */
    public synchronized List<WaiterView> waitersSnapshot() {
        List<WaiterView> out = new ArrayList<>(waiters.size());
        for (Wait w : waiters) {
            out.add(new WaiterView(w.sessionId(), w.requestId(), w.expectedPhase(),
                    w.enqueuedAtMs()));
        }
        return List.copyOf(out);
    }

    /**
     * 复制态全量快照（条目锁内一次拷贝；影子表镜像与摘要判据数据源）。
     *
     * @return 不可变的 {@link ReplicatedState} 导出
     */
    public synchronized ReplicatedState replicatedState() {
        return new ReplicatedState(phase, registered, arrived, partiesSnapshot(),
                List.copyOf(slots), prevPhase, List.copyOf(prevSlots));
    }

    /**
     * 明细只读快照：条目锁内拷贝三计数；等待项不入本快照的 {@code waiters}
     * 区段（phaser 等待无位次与许可语义，expected_phase 非 {@code WaiterSnapshot}
     * 可承载——明细经 {@link #waitersSnapshot()} 独立视图导出，判例条件
     * 等待与等待队列并列不并号）。相位器无租约与持有者，租约三元组与
     * 持有表恒零值/空表。纯读，MUST NOT 改变任何状态。
     *
     * @param now 采样时刻（毫秒，引擎时钟）
     * @return 本条目自洽的不可变明细快照
     */
    public synchronized CoreInspection.KeySnapshot snapshot(long now) {
        return new CoreInspection.KeySnapshot(key, KeyFamily.PHASER, false,
                0, 0, 0, 0,
                List.of(), List.of(),
                0, 0, 0, 0, List.of(),
                null, 0, 0, 0,
                0, 0, 0, false, null, null, null,
                0, 0, 0, 0, null,
                phase, registered, arrived);
    }

    /**
     * 到场族裁决（类注步骤 2/3/5）。
     *
     * @param cmd         命令
     * @param now         当前时刻
     * @param notify      唤醒收集列表
     * @param clusterMode 集群应用形态（true 时等待半程不登记条目内）
     * @return 结果（完成或挂起）
     */
    private PhaserOpResult arrive(PhaserOpCommand cmd, long now, List<Waiter> notify,
            boolean clusterMode) {
        Arrival a = new Arrival(cmd.sessionId(), cmd.requestId());
        boolean deregister = cmd.op() == PhaserOpType.ARRIVE_AND_DEREGISTER;
        if (slots.contains(a)) {
            // 幂等重发（在场槽属当前相位——其到场时相位即当前相位）：
            // 不双计数、不双扣配额；ARRIVE_AND_AWAIT 的等待半程续判
            // （在集即续挂、不在集且未推进则宽容登记——应答丢失形态）。
            if (cmd.op() == PhaserOpType.ARRIVE_AND_AWAIT) {
                return clusterMode
                        ? PhaserOpResult.queued(phase, registered, arrived)
                        : waitOrRegister(cmd.sessionId(), cmd.requestId(), phase, now);
            }
            return PhaserOpResult.granted(phase, registered, arrived);
        }
        if (prevSlots.contains(a)) {
            // 跨推进重发（到场已被上一周期计入、其槽随推进滚动至此）：
            // 终态回显 prevPhase，不双计数、不双扣配额、不再挂起——
            // 该请求的等待（若有）已随推进唤醒出集，重发仅补应答。
            return PhaserOpResult.granted(prevPhase, registered, arrived);
        }
        if (deregister) {
            PartyView quota = partyQuota.get(cmd.sessionId());
            if (quota == null || quota.parties() <= 0) {
                return PhaserOpResult.rejected(Outcome.REJECT_PHASER_QUOTA);
            }
        }
        long arrivalPhase = phase;
        slots.add(a);
        arrived++;
        if (deregister) {
            PartyView prior = partyQuota.get(cmd.sessionId());
            int quota = prior.parties() - 1;
            if (quota <= 0) {
                partyQuota.remove(cmd.sessionId());
            } else {
                partyQuota.put(cmd.sessionId(), new PartyView(cmd.sessionId(), quota,
                        prior.lastRegisterRequestId(), prior.lastRegisterPhase()));
            }
            registered--;
        }
        boolean tripped = tripIfComplete(now, notify);
        if (cmd.op() != PhaserOpType.ARRIVE_AND_AWAIT) {
            return PhaserOpResult.granted(arrivalPhase, registered, arrived, tripped);
        }
        if (tripped) {
            // 到场恰触发合拢：直答完成，回显到场时相位（JDK 返回语义）。
            return PhaserOpResult.granted(arrivalPhase, registered, arrived, true);
        }
        if (clusterMode) {
            // 集群形态不在条目登记等待（"集群引擎恒不登记等待项"不变式，判例
            // acquire 的 queueIfBusy=false 调用与 v9 条件双拓扑）：回执 QUEUED，
            // 等待簿记由 Leader 侧网关在应用副作用中登记进 PhaserRegistry。
            return PhaserOpResult.queued(arrivalPhase, registered, arrived);
        }
        return waitOrRegister(cmd.sessionId(), cmd.requestId(), arrivalPhase, now);
    }

    /**
     * AWAIT_ADVANCE 裁决（类注步骤 4）。
     *
     * @param cmd 命令
     * @param now 当前时刻
     * @param cfg 限额配置（合并深度）
     * @return 结果（即刻了结 / 挂起 / 深度超限）
     */
    private PhaserOpResult awaitAdvance(PhaserOpCommand cmd, long now, CoreConfig cfg) {
        long expected = cmd.expectedPhase();
        if (phase > expected) {
            return PhaserOpResult.granted(phase, registered, arrived);
        }
        for (Wait w : waiters) {
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                return PhaserOpResult.queued(phase, registered, arrived);
            }
        }
        if (waiters.size() >= cfg.maxQueueDepthPerKey()) {
            return PhaserOpResult.rejected(Outcome.REJECT_QUEUE_FULL);
        }
        waiters.addLast(new Wait(cmd.sessionId(), cmd.requestId(), expected, now));
        return PhaserOpResult.queued(phase, registered, arrived);
    }

    /**
     * 等待登记统一落点（到场族等待半程与幂等续判共用；恒宽容——已到场者
     * 的挂起不受深度拒绝，见类注"护栏分界"）。判定序：谓词已越即刻完成；
     * 在集即续挂（幂等，不双登记）；否则以 {@code expectedPhase} 入集。
     *
     * @param sessionId     等待会话
     * @param requestId     等待请求 id
     * @param expectedPhase 等待谓词基准（已见/到场时相位）
     * @param now           当前时刻（登记观察值）
     * @return 完成或挂起结果
     */
    private PhaserOpResult waitOrRegister(long sessionId, long requestId,
            long expectedPhase, long now) {
        if (phase > expectedPhase) {
            return PhaserOpResult.granted(phase, registered, arrived);
        }
        for (Wait w : waiters) {
            if (w.sessionId() == sessionId && w.requestId() == requestId) {
                return PhaserOpResult.queued(phase, registered, arrived);
            }
        }
        waiters.addLast(new Wait(sessionId, requestId, expectedPhase, now));
        return PhaserOpResult.queued(phase, registered, arrived);
    }

    /**
     * 推进判定与唤醒的统一落点（类注步骤 3）：判据
     * {@code arrived > 0 ∧ arrived ≥ registered} 成立时相位恰 +1、到场计数
     * 清零、在场槽清空，并把全部挂起等待项出集收集进 {@code notify}
     * （挂起项恒满足 {@code expected < 新相位}——登记谓词不变式）。
     *
     * <p>调用方义务：MUST 在条目锁内、任何账簿迁移之后调用（注册迁移
     * 例外——REGISTER MUST NOT 调用本方法）。
     *
     * @param now    当前时刻（毫秒）
     * @param notify 唤醒收集列表
     * @return 是否发生了推进
     */
    private boolean tripIfComplete(long now, List<Waiter> notify) {
        if (arrived <= 0 || arrived < registered) {
            return false;
        }
        prevPhase = phase;
        prevSlots = slots;
        phase++;
        arrived = 0;
        slots = new LinkedHashSet<>();
        if (!waiters.isEmpty()) {
            List<Wait> wake = new ArrayList<>(waiters);
            waiters.clear();
            for (Wait w : wake) {
                notify.add(new Waiter(w.sessionId(), w.requestId(), LockType.PHASER,
                        1, 0, w.enqueuedAtMs(), 0));
            }
        }
        return true;
    }
}
