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

import io.github.lamspace.openlatch.core.KeyFamily;

import java.util.List;

/**
 * key 级状态条目的生命周期契约——{@link LockTable} 中值的公共抽象。
 *
 * <p><b>职责边界</b>：本接口只收编"与命令语义无关"的生命周期与清理行为
 * （身份、家族判别、空判定、租约读数、等待者读数、会话摘除、到期强制
 * 回收、已通知队首清扫）。授予/释放/续租等命令语义 MUST NOT 进入本接口——各家族
 * 差异大（锁有读写重入、Semaphore 有许可计数、Latch 无租约），强行统一
 * 会产出满是不可达分支的抽象；命令分派由 {@code CoreEngine} 按
 * {@link #family()} 完成（Phase 3 T1 design D2）。
 *
 * <p><b>并发模型</b>：实现类的本接口方法与其余状态迁移一样，全部在
 * 条目自身监视器（{@code synchronized(this)}）内完成并对外提供线程安全；
 * 调用方（{@code CoreEngine}）为原子完成"成员回查 + 状态迁移 + 条目移除"
 * 而额外持有的条目锁与之重入，不构成第二把锁。任一实现 MUST NOT 在持锁
 * 期间调用本接口之外的跨条目协作方法。
 *
 * <p><b>家族定型</b>：{@link #family()} 是实现随构造确定的不可变判别，
 * 跨家族请求的拒绝判定在 {@code CoreEngine} 侧完成，条目自身不做互斥检查。
 */
public interface KeyEntry {

    /**
     * 条目所属锁键（{@link LockTable} 的映射键）。
     *
     * @return 锁键
     */
    String key();

    /**
     * 条目家族判别，构造时定型、之后不变。
     *
     * @return 所属家族
     */
    KeyFamily family();

    /**
     * 是否已无任何需要保留的状态——各家族对"空"的判据不同（锁：无持有
     * 且无等待；Semaphore：无持有且无等待；Latch：无在等），但语义一致：
     * 返回 {@code true} 时条目可从 {@link LockTable} 移除且不影响正确性。
     *
     * @return 条目可回收返回 true
     */
    boolean isEmpty();

    /**
     * 当前租约凭证；家族无租约语义（或当前无持有）时为 0。供
     * {@code CoreEngine.expireDue} 的陈旧校验读取。
     *
     * @return 租约凭证，无则 0
     */
    long leaseToken();

    /**
     * 当前租约到期时刻（毫秒）；家族无租约语义（或当前无持有）时为 0。
     * 供 {@code CoreEngine.expireDue} 的陈旧校验读取——凭证与到期时刻
     * 双值一致才执行强制回收（ABA 防护）。
     *
     * @return 到期时刻（毫秒），无则 0
     */
    long leaseExpiresAtMs();

    /**
     * 会话清理：摘除指定会话在本条目中的全部痕迹（持有、等待），并在
     * 清理后出现"可推进的队首"时将其收集进 {@code notify}。会话关闭
     * 是无条件生效的清理，不设拒绝分支。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
    void removeSession(long sessionId, long now, long headReplyTimeoutMs, List<Waiter> notify);

    /**
     * 租约到期强制回收：清除该条目全部持有与租约（锁：写侧与读侧；
     * Semaphore：归还持有者全部许可），不触碰等待队列，并在可推进时
     * 收集队首通知。仅由 {@code CoreEngine.expireDue} 在陈旧校验通过后
     * 调用；无租约家族（Latch）永不入到期堆，本方法对其不可达。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
    void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify);

    /**
     * 已通知队首的响应超时清扫（通知丢失兜底）：队首处于"已通知、待重发"
     * 且响应截止时刻已过时将其出队，并对新队首做通知收集。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 是否移除了超时队首
     */
    boolean sweepNotifiedHead(long now, long headReplyTimeoutMs, List<Waiter> notify);

    /**
     * 等待队列条目读数（统计观察面，Phase 3 T2）：锁/Semaphore 为等待队列
     * 长度，Latch 为 awaiter 队列长度——三家统一为"本 key 当前排队等待项数"，
     * 供 {@code CoreEngine.stats()} 聚合。只读，MUST NOT 改变队列状态。
     * 须在持有条目锁时调用（{@code CoreEngine} 保证）。
     *
     * @return 等待者数量，无等待者返回 0
     */
    int waiterCount();
}
