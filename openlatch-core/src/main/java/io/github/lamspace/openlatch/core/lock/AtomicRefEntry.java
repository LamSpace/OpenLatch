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

import io.github.lamspace.openlatch.core.AtomicOp;
import io.github.lamspace.openlatch.core.CoreInspection;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AtomicRefOpCommand;
import io.github.lamspace.openlatch.core.result.AtomicRefOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 单 key 的有值引用状态机——ATOMIC 家族的载荷形态（与
 * {@link AtomicEntry} 标量形态同族互斥）：值不绑定
 * {@code (sessionId, threadId)} 归属，无租约、无等待队列，任何操作的挂起与
 * 通知路径对其不可达。
 *
 * <p><b>值域</b>：不透明字节数组。{@code null}（null 态）与零长度数组
 * （空字节串）是两个可区分的合法值——判据、落值与槽回放全程保持两态
 * 独立；相等性判定用 {@link Arrays#equals}（含 null 与空串的互斥）。
 * 本类与引擎 MUST NOT 解释载荷内容（无反序列化、无编码假设），也
 * MUST NOT 以本地配置复核载荷字节数——尺寸钳制是接入层的专属判定点，
 * 进入状态机的命令视为已钳制（节点本地配置参与 apply 判定会引入
 * 跨副本回放分歧）。
 *
 * <p><b>状态要素</b>：
 * <ul>
 *   <li>{@code kind}（恒 {@link LockType#ATOMIC_REFERENCE}，随条目创建定型
 *       后不变；跨形态请求的互拒在引擎门面按条目类型裁决）；</li>
 *   <li>{@code initial}（定型初值：建条目时非 null 主张即记录主张值，
 *       null 表示无主张；后续请求携带的非 null 主张与之字节一致才放行
 *       ——标量形态"0 不主张"规则的 presence 对应物）；</li>
 *   <li>{@code value}（当前载荷，可为 null）；</li>
 *   <li>{@code version}（版本戳：每次成功改变值的写操作恰 +1；GET 与
 *       未落值的失败 CAS 均不推进——单调性是超时复判与 ABA 消除的
 *       唯一依据）；</li>
 *   <li>去重槽 {@code (slotSession, slotOpSeq, 应答四元组)}：与标量形态
 *       同构，槽内 old/value 为载荷（可为 null）。单槽覆盖实际故障窗，
 *       更早序号的迟到重发不被去重，属已声明契约边界。</li>
 * </ul>
 *
 * <p><b>操作判定顺序</b>（{@link #op}，首个命中即为结果；与
 * {@link AtomicEntry#op} 的规则集逐位同构）：
 * <ol>
 *   <li><b>值域检查</b>：{@link AtomicOp#ADD} 对本形态为值域外操作 →
 *       {@link Outcome#REJECT_ATOMIC_RANGE}（字节串无加法，判例布尔
 *       形态 ADD 拒绝）；</li>
 *   <li><b>初值断言</b>：非 null 的 {@code initial} 主张与定型初值字节
 *       不符 → {@link Outcome#REJECT_ATOMIC_INIT}（条目零扰动）；</li>
 *   <li><b>GET 短路</b>：返回当前读数，不占槽、不推版本；</li>
 *   <li><b>去重重放</b>：{@code opSeq >= 1} 且与会话槽一致 → 重放存储
 *       应答（载荷字节级一致）；</li>
 *   <li><b>执行写操作</b>：成功写推版本恰 +1；CAS 家族未命中不推版本
 *       （{@code applied=false} 非拒绝）；处理完毕覆盖槽位。</li>
 * </ol>
 * 全部判定在条目监视器内完成，操作两两线性化。
 *
 * <p><b>数组所有权</b>：命令携带的载荷数组在 {@link #op} 内被存储为
 * 引用（不做复制），值迁移一律引用替换、既有数组内容永不原地修改——
 * 因此存储数组可安全共享给结果与观察面；构造命令的一方在传入后
 * MUST NOT 再修改这些数组。
 *
 * <p><b>与租约机制的关系</b>：{@link #leaseToken()} / {@link #leaseExpiresAtMs()}
 * 恒 0，条目永不入到期堆，{@link #forceExpire} 不可达；
 * {@link #removeSession} 为无操作——载荷不属于任何会话，会话的创建、
 * 持有、死亡均不构成值的回滚理由。
 *
 * <p><b>生命周期</b>：{@link #isEmpty()} 恒 {@code false}——0 长度与 null
 * 都是合法状态而非回收信号；条目一经创建常驻至进程重启后按快照
 * 恢复（载荷驻留成本由每 key 钳制上限封顶、由观察面承担可见性）。
 *
 * <p><b>并发模型</b>：与 {@link AtomicEntry} 相同——全部迁移在
 * {@code synchronized(this)} 内完成；本类无通知路径，
 * {@link #waiterCount()} 恒 0。快照读数方法须在持有条目锁时调用。
 */
public final class AtomicRefEntry implements KeyEntry {

    /** 有值引用键。 */
    private final String key;
    /** 形态判别（恒 ATOMIC_REFERENCE，创建定型）。 */
    private final LockType kind;
    /** 定型初值主张（null=建条目时无主张）。 */
    private final byte[] initial;
    /** 当前载荷（null=null 态；零长度=空字节串）。 */
    private byte[] value;
    /** 版本戳（每次成功写恰 +1）。 */
    private long version;
    /** 去重槽：最近被处理写操作的逻辑会话（0=空槽）。 */
    private long slotSession;
    /** 去重槽：该写操作的 op_seq。 */
    private long slotOpSeq;
    /** 去重槽：存储应答 applied。 */
    private boolean slotApplied;
    /** 去重槽：存储应答 oldValue（可为 null）。 */
    private byte[] slotOldValue;
    /** 去重槽：存储应答 value（可为 null）。 */
    private byte[] slotValue;
    /** 去重槽：存储应答 version。 */
    private long slotVersion;

    /**
     * 构造有值引用条目（懒建路径：引擎在首条引用写命令上调用）。
     *
     * @param key     有值引用键
     * @param kind    形态判别（须为 {@link LockType#ATOMIC_REFERENCE}）
     * @param initial 建条目初值主张载荷（{@code null}=无主张，条目初值为 null 态）
     * @throws NullPointerException     key/kind 为 {@code null}
     * @throws IllegalArgumentException kind 非有值引用形态
     */
    public AtomicRefEntry(String key, LockType kind, byte[] initial) {
        this.key = Objects.requireNonNull(key);
        if (kind != LockType.ATOMIC_REFERENCE) {
            throw new IllegalArgumentException("not a reference kind: " + kind);
        }
        this.kind = kind;
        this.initial = initial;
        this.value = initial;
    }

    /**
     * 快照重建工厂：以全字段快照直接装配，不经 {@link #op} 判定路径；
     * 载荷数组按重建输入原样接管（含 null 与零长度两态）。
     *
     * @param key          有值引用键
     * @param initial      定型初值主张（null=无主张）
     * @param value        当前载荷（null=null 态）
     * @param version      版本戳
     * @param slotSession  去重槽会话（0=空槽）
     * @param slotOpSeq    去重槽序号
     * @param slotApplied  槽应答 applied
     * @param slotOldValue 槽应答 oldValue 载荷（可为 null）
     * @param slotValue    槽应答 value 载荷（可为 null）
     * @param slotVersion  槽应答 version
     * @return 快照初态的条目
     */
    public static AtomicRefEntry restored(String key, byte[] initial, byte[] value,
            long version, long slotSession, long slotOpSeq, boolean slotApplied,
            byte[] slotOldValue, byte[] slotValue, long slotVersion) {
        AtomicRefEntry e = new AtomicRefEntry(key, LockType.ATOMIC_REFERENCE, initial);
        e.value = value;
        e.version = version;
        e.slotSession = slotSession;
        e.slotOpSeq = slotOpSeq;
        e.slotApplied = slotApplied;
        e.slotOldValue = slotOldValue;
        e.slotValue = slotValue;
        e.slotVersion = slotVersion;
        return e;
    }

    /**
     * 执行一条有值引用操作命令。判定顺序与零扰动规则见类注释；
     * 全部迁移在本条目监视器内完成。
     *
     * @param cmd 引用形态命令（载荷数组传入后不得再修改）
     * @return 操作结果（{@link Outcome#GRANTED} 携带载荷应答四元组，
     *         或值域/初值类拒绝——拒绝态四元组为零值形）
     */
    public synchronized AtomicRefOpResult op(AtomicRefOpCommand cmd) {
        // 规则 1：值域检查——ADD 对字节串无定义（判例布尔形态 ADD 拒绝）。
        if (cmd.op() == AtomicOp.ADD) {
            return AtomicRefOpResult.rejected(Outcome.REJECT_ATOMIC_RANGE);
        }
        // 规则 2：初值断言（null 为不主张；非 null 主张与定型初值字节相符才放行）。
        if (cmd.initial() != null && !Arrays.equals(cmd.initial(), initial)) {
            return AtomicRefOpResult.rejected(Outcome.REJECT_ATOMIC_INIT);
        }
        // 规则 3：GET 短路——不占槽、不推版本（applied=false 与未命中 CAS 同旗标，
        // 区分由应答回显的 op 承担）。
        if (cmd.op() == AtomicOp.GET) {
            return new AtomicRefOpResult(Outcome.GRANTED, false, value, value, version);
        }
        // 规则 4：去重重放（同会话同序号；GET 不入此路径）。
        if (cmd.opSeq() >= 1 && cmd.sessionId() == slotSession && cmd.opSeq() == slotOpSeq) {
            return new AtomicRefOpResult(Outcome.GRANTED, slotApplied,
                    slotOldValue, slotValue, slotVersion);
        }
        // 规则 5：执行写操作。
        AtomicRefOpResult r = execute(cmd);
        if (cmd.opSeq() >= 1) {
            slotSession = cmd.sessionId();
            slotOpSeq = cmd.opSeq();
            slotApplied = r.applied();
            slotOldValue = r.oldValue();
            slotValue = r.value();
            slotVersion = r.version();
        }
        return r;
    }

    /**
     * 写操作执行体（{@link #op} 规则 5，条目锁内调用）：按 op 分派落载荷，
     * 成功落值恰 +1 版本；CAS 家族未命中回当前读数不推进。期望与比较均
     * 以 {@link Arrays#equals} 字节判定（null 与空串互不相等）。
     *
     * @param cmd 已通过值域与初值断言检查的写命令
     * @return 操作结果
     */
    private AtomicRefOpResult execute(AtomicRefOpCommand cmd) {
        return switch (cmd.op()) {
            case GET, ADD -> throw new IllegalStateException("handled before execute");
            case SET, GET_AND_SET -> {
                byte[] old = value;
                value = cmd.operand();
                version++;
                yield new AtomicRefOpResult(Outcome.GRANTED, true, old, value, version);
            }
            case CAS -> {
                if (!Arrays.equals(cmd.expected(), value)) {
                    yield new AtomicRefOpResult(Outcome.GRANTED, false, value, value, version);
                }
                byte[] old = value;
                value = cmd.operand();
                version++;
                yield new AtomicRefOpResult(Outcome.GRANTED, true, old, value, version);
            }
            case CAS_STAMPED -> {
                boolean versionOk = cmd.expectedVersion() == 0
                        || cmd.expectedVersion() == version;
                if (!Arrays.equals(cmd.expected(), value) || !versionOk) {
                    yield new AtomicRefOpResult(Outcome.GRANTED, false, value, value, version);
                }
                byte[] old = value;
                value = cmd.operand();
                version++;
                yield new AtomicRefOpResult(Outcome.GRANTED, true, old, value, version);
            }
        };
    }

    /**
     * 形态判别（快照序列化与管理观察读取）。须在持有条目锁时调用。
     *
     * @return 定型形态（恒 {@link LockType#ATOMIC_REFERENCE}）
     */
    public synchronized LockType kind() {
        return kind;
    }

    /**
     * 定型初值主张（断言比对基准，快照序列化侧读取；null=无主张）。
     * 须在持有条目锁时调用。
     *
     * @return 初值载荷或 {@code null}
     */
    public synchronized byte[] initial() {
        return initial;
    }

    /**
     * 当前载荷（快照序列化侧与管理观察读取；null=null 态）。
     * 须在持有条目锁时调用。
     *
     * @return 当前载荷或 {@code null}
     */
    public synchronized byte[] value() {
        return value;
    }

    /**
     * 当前版本戳（快照序列化侧与管理观察读取）。须在持有条目锁时调用。
     *
     * @return 版本戳
     */
    public synchronized long version() {
        return version;
    }

    /**
     * 去重槽会话读数（快照序列化侧读取；0=空槽）。须在持有条目锁时调用。
     *
     * @return 槽内逻辑会话 id，空槽为 0
     */
    synchronized long slotSession() {
        return slotSession;
    }

    /**
     * 去重槽序号读数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 槽内 op_seq，空槽为 0
     */
    synchronized long slotOpSeq() {
        return slotOpSeq;
    }

    /**
     * 去重槽应答 applied 读数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 槽应答 applied
     */
    synchronized boolean slotApplied() {
        return slotApplied;
    }

    /**
     * 去重槽应答 oldValue 载荷读数（快照序列化侧读取；可为 null）。
     * 须在持有条目锁时调用。
     *
     * @return 槽应答 oldValue 载荷或 {@code null}
     */
    synchronized byte[] slotOldValue() {
        return slotOldValue;
    }

    /**
     * 去重槽应答 value 载荷读数（快照序列化侧读取；可为 null）。
     * 须在持有条目锁时调用。
     *
     * @return 槽应答 value 载荷或 {@code null}
     */
    synchronized byte[] slotValue() {
        return slotValue;
    }

    /**
     * 去重槽应答 version 读数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 槽应答 version
     */
    synchronized long slotVersion() {
        return slotVersion;
    }

    @Override
    public KeyFamily family() {
        return KeyFamily.ATOMIC;
    }

    @Override
    public String key() {
        return key;
    }

    /** 恒 0：有值引用条目无租约（契约读数占位，到期堆永不登记本条目）。 */
    @Override
    public long leaseToken() {
        return 0;
    }

    /** 恒 0：有值引用条目无到期时刻（{@link #leaseToken()} 同理）。 */
    @Override
    public long leaseExpiresAtMs() {
        return 0;
    }

    /**
     * 恒 {@code false}：有值引用条目无常驻判据的"空"点——null 与 0 长度
     * 载荷都是合法状态而非回收信号（见类注释生命周期）。
     *
     * @return 恒 {@code false}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * 无操作：载荷不绑定会话归属，会话关闭 MUST NOT 改变本条目的值、
     * 版本戳与去重槽。
     *
     * @param sessionId          被关闭会话（忽略）
     * @param now                当前时刻（忽略）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒，忽略）
     * @param notify             通知收集列表（恒不产出）
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        // 无操作：原子值与会话生死无关（契约声明，勿在此添加任何清理）。
    }

    /**
     * 不可达：有值引用条目无租约、永不入到期堆（引擎 {@code expireDue}
     * 无此形态登记路径）。
     *
     * @param now                当前时刻
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（有值引用条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("atomic reference entry has no lease to expire");
    }

    /**
     * 恒 {@code false}：有值引用条目无等待队列，"已通知待重发"状态机对其
     * 不存在（形参保留以对齐 {@link KeyEntry} 契约）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表（恒不产出）
     * @return 恒 {@code false}
     */
    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        return false;
    }

    /** 恒 0：有值引用条目无等待队列（统计观察面）。 */
    @Override
    public synchronized int waiterCount() {
        return 0;
    }

    /**
     * 明细只读快照：条目锁内拷贝形态、载荷读数与版本戳（标量值字段
     * 恒零值形，载荷读数入 {@code atomicRef*} 字段）。有值引用家族无
     * 租约、无持有者、无等待队列，对应字段恒零值/空表；去重槽不入本
     * 快照（属复制状态而非观察字段）。纯读，MUST NOT 改变任何状态。
     *
     * @param now 采样时刻（毫秒，引擎时钟）
     * @return 本条目自洽的不可变快照
     */
    public synchronized CoreInspection.KeySnapshot snapshot(long now) {
        return new CoreInspection.KeySnapshot(key, KeyFamily.ATOMIC, false,
                0, 0, 0, 0,
                List.of(), List.of(),
                0, 0, 0, 0, List.of(),
                kind, 0, 0, version,
                0, 0, 0, false, null,
                initial, value);
    }
}
