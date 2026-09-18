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

package io.github.lamspace.openlatch.core;

import java.util.List;

/**
 * 引擎明细只读观察面的一次性快照。
 *
 * <p><b>定位</b>：{@link CoreStats} 聚合读数之外的**明细级**观察面——
 * 按 key 条目携带持有者、等待队列与租约的完整可见状态，是上层管理协议
 * （ADMIN_* 四消息）单机形态的权威数据源；集群形态读复制态镜像，
 * 结构由 server 侧对齐本快照的字段口径。
 *
 * <p><b>一致性语义</b>：逐条目在条目锁内拷贝、条目间无全局原子性——
 * 返回值是采样时刻的弱一致快照（{@code sampledAtMs} 为采样起始时刻，
 * 各条目的 {@code waitedMs}/{@code remainingLeaseMs} 均以该时刻折算）。
 * 单条目快照内部自洽（位次连续、持有列表与租约读数同锁内取得）。
 * 消费者 MUST NOT 据此做授予判定，仅供观测。
 *
 * <p><b>线程安全</b>：本记录及其全部嵌套值对象不可变，构造完成后可被
 * 任意线程并发读。
 *
 * @param keys         全部 key 条目的明细快照（遍历序，不保证排序）
 * @param sessionCount 登记在册的会话数（与 {@link CoreStats#sessionCount()} 同源口径）
 * @param sampledAtMs  采样基准时刻（毫秒，引擎时钟）
 */
public record CoreInspection(
        List<KeySnapshot> keys,
        int sessionCount,
        long sampledAtMs) {

    /**
     * 单 key 条目的明细快照——四家族共用一个值形态，家族外字段取零值
     * （锁家族无许可/屏障字段，Semaphore 家族无屏障字段，Latch 家族无租约
     * 与持有者，ATOMIC 家族无租约/持有者/等待者、仅原子四字段有效）。
     * 不可变值对象。
     *
     * @param key              锁键
     * @param family           条目家族
     * @param reentrant        锁家族可重入性定型（非锁家族恒 {@code false}，无意义）
     * @param leaseToken       当前租约凭证，无持有为 0
     * @param leaseMs          实际生效租期（毫秒），无持有为 0
     * @param leaseExpiresAtMs 当前到期时刻（毫秒），无持有为 0
     * @param remainingLeaseMs 剩余租约（采样时刻折算，下限 0），无租约为 0
     * @param holders          持有者列表（锁：写侧在前、读者依表序；Semaphore：
     *                         依持有登记序；Latch：恒空）
     * @param waiters          等待队列（位次依序隐含于列表下标；Latch 为 awaiter；
     *                         非 Leader 集群镜像恒空——等待队列非复制状态）
     * @param permitsTotal     Semaphore 许可总量（非该家族为 0）
     * @param permitsAvailable Semaphore 当前可用许可（非该家族为 0）
     * @param latchTotal       Latch 定型初始计数（非该家族为 0）
     * @param latchRemaining   Latch 当前剩余计数（非该家族为 0）
     * @param latchParticipants Latch 参与会话集（非该家族为空表）
     * @param atomicKind       ATOMIC 形态判别（非该家族为 {@code null}）
     * @param atomicInitial    ATOMIC 定型初值（非该家族为 0）
     * @param atomicValue      ATOMIC 当前值（非该家族为 0）
     * @param atomicVersion    ATOMIC 版本戳（非该家族为 0）
     */
    public record KeySnapshot(
            String key,
            KeyFamily family,
            boolean reentrant,
            long leaseToken,
            long leaseMs,
            long leaseExpiresAtMs,
            long remainingLeaseMs,
            List<HolderSnapshot> holders,
            List<WaiterSnapshot> waiters,
            int permitsTotal,
            int permitsAvailable,
            long latchTotal,
            long latchRemaining,
            List<Long> latchParticipants,
            io.github.lamspace.openlatch.core.LockType atomicKind,
            long atomicInitial,
            long atomicValue,
            long atomicVersion) {
    }

    /**
     * 持有者明细。
     *
     * @param sessionId 持有者所属会话
     * @param threadId  持有者线程标识
     * @param count     重入层数（锁）或持有许可数（Semaphore）
     * @param role      持有角色判别
     */
    public record HolderSnapshot(
            long sessionId,
            long threadId,
            int count,
            HolderRole role) {
    }

    /**
     * 持有角色：锁写侧（含互斥/重入）、锁读侧、许可持有者。core 侧的结构
     * 判别，协议词汇（{@code writer}/{@code reader}/{@code holder} 字符串）
     * 由 server 管理协议层映射。
     */
    public enum HolderRole {
        /** 锁写侧持有者。 */
        WRITER,
        /** 锁读侧持有者。 */
        READER,
        /** Semaphore 许可持有者。 */
        HOLDER
    }

    /**
     * 等待队列项明细。
     *
     * @param sessionId  等待者所属会话
     * @param requestId  挂起的原请求 id（Latch 等待者身份即 (会话, 请求)）
     * @param threadId   请求线程标识（Latch awaiter 恒 0——await 无归属线程概念）
     * @param permits    请求许可数（锁与屏障恒 1）
     * @param enqueuedAtMs 入队时刻（引擎时钟，毫秒）
     * @param waitedMs   已等待时长（采样时刻折算，下限 0）
     * @param notified   是否处于"已通知、待重发"状态
     */
    public record WaiterSnapshot(
            long sessionId,
            long requestId,
            long threadId,
            int permits,
            long enqueuedAtMs,
            long waitedMs,
            boolean notified) {
    }
}
