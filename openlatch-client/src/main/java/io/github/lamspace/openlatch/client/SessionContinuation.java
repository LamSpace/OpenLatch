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
 * 单次逻辑写操作的**跨会话续发判定**。
 *
 * <p><b>职责</b>：记录一次逻辑写操作（一次 {@code put}/{@code offer(delay)}/… 调用，
 * 可能包含多次网络尝试）内部各次尝试的效果判定，回答唯一一个问题——
 * 「此刻可否把这次操作**换到新会话**上续发」。队列写车道（阻塞挂起环与立即式
 * 交换环）共用本判定，避免同一纪律在两处各写一遍而漂移。
 *
 * <p><b>为什么需要它</b>：服务端去重槽按 {@code (会话, op_seq)} 记账并原值重放
 * （引擎条目规则），**换会话即槽不中**——续发等同一次新写。因此"允许跨会话续发"
 * 等价于断言"本次逻辑操作此前从未有过可能已生效的尝试"。而单看"最近一次尝试"
 * 是不够的：一次明确零生效拒绝（非权威拒绝、应答缺码形）只证明**那一次**未生效，
 * 它无从证明更早的尝试未生效。
 *
 * <p><b>单调性契约</b>：{@link #possiblyApplied} 一经置位**永不回退**——任何后续
 * 的明确零生效拒绝都不得把它洗白。因此本类判定的是"整段历史"，而非"最近一次"。
 *
 * <p><b>线程模型</b>：非线程安全。每个逻辑写操作在**自己那条循环线程**上持有
 * 一个实例，无共享、无跨线程发布（与 {@code RemoteBlockingQueue} 两条写循环的
 * 单线程推进纪律一致）。
 *
 * <p><b>调用者义务</b>：①每次尝试的结果**恰好**回调一个 {@code on*} 方法
 * （未回调即等于把该次尝试当成未发生，会让判定失真）；②{@link #mayContinueOnNewSession()}
 * 为真且调用方据此续发后，MUST 调用 {@link #consumeContinuation()} 消费该许可——
 * 许可是**一次性**的，对应"已经在别的会话上重新发出"这一事实。
 */
final class SessionContinuation {

    /**
     * 本次逻辑操作是否已有"可能已生效"的尝试。
     *
     * <p>单调：一旦置位不再清除。置位来源见 {@link #onPossiblyApplied()} 与
     * {@link #onAcceptedAttempt()}。
     */
    private boolean possiblyApplied;

    /** 最近一次尝试是否被明确判为零生效（非权威拒绝、应答缺码形）。 */
    private boolean lastZeroEffectReject;

    /**
     * 构造判定（初始态：本操作尚无任何尝试——既无"可能已生效"，也无"明确零生效"；
     * 两标志皆为 {@code false}，故 {@link #mayContinueOnNewSession()} 初始为
     * {@code false}，即"还没有任何可以据以换会话的事实"）。
     */
    SessionContinuation() {
    }

    /**
     * 记录一次**可能已生效但未获应答**的尝试：应答丢失（读界超时）或传输失败。
     *
     * <p>该尝试的效果不可判定——它可能已在服务端生效，也可能没有。故既置单调位，
     * 又清除"最近一次零生效"（后者描述的是**本次**尝试，已被本方法取代）。
     */
    void onPossiblyApplied() {
        possiblyApplied = true;
        lastZeroEffectReject = false;
    }

    /**
     * 记录一次**已被受理**的尝试：服务端回挂起（{@code QUEUED}）或回弹
     * （{@code DENIED}）——请求已进入服务端账簿，效果**已**确立（未了结但已生效）。
     *
     * <p>与 {@link #onPossiblyApplied()} 同置单调位：此后若再收到明确零生效拒绝，
     * 那也是针对**后来那次**尝试的，不能反过来证明本次未生效。
     */
    void onAcceptedAttempt() {
        possiblyApplied = true;
        lastZeroEffectReject = false;
    }

    /**
     * 记录一次**明确零生效**的拒绝：非权威拒绝（{@code NOT_LEADER}）或应答缺码形。
     *
     * <p>只置"最近一次"标志，**不**清除单调位——这正是本类的核心纪律：明确拒绝
     * 不能洗白更早的不确定。
     */
    void onZeroEffectReject() {
        lastZeroEffectReject = true;
    }

    /**
     * 此刻可否把本次逻辑操作换到新会话上续发。
     *
     * @return 当且仅当"最近一次尝试明确零生效"且"本次操作此前从未有过可能已生效的
     *         尝试"时为 {@code true}
     */
    boolean mayContinueOnNewSession() {
        return lastZeroEffectReject && !possiblyApplied;
    }

    /**
     * 消费跨会话续发许可（许可为一次性）。
     *
     * <p>调用方在**已经**据 {@link #mayContinueOnNewSession()} 在新会话上重建并发出
     * 请求后调用。单调位不受影响：本次操作仍然"曾有过可能已生效的尝试"吗——不，
     * 此处恰恰是"从未有过"才拿到许可，故单调位本就为假；清除的只是"最近一次"标志。
     */
    void consumeContinuation() {
        lastZeroEffectReject = false;
    }
}
