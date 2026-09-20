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
import io.github.lamspace.openlatch.core.command.AtomicOpCommand;
import io.github.lamspace.openlatch.core.result.AtomicOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;

import java.util.List;
import java.util.Objects;

/**
 * 单 key 的原子变量状态机——库内第一个非锁家族条目：值不绑定
 * {@code (sessionId, threadId)} 归属，无租约、无等待队列，任何操作的挂起与
 * 通知路径对其不可达。
 *
 * <p><b>状态要素</b>：
 * <ul>
 *   <li>{@code kind}（形态：long / integer / boolean，随条目创建定型后不变，
 *       跨形态请求以 {@link Outcome#REJECT_TYPE_MISMATCH} 拒绝）；</li>
 *   <li>{@code initial}（定型初值：建条目时的非零主张值，0 表示建条目时
 *       无主张；后续请求携带的非零主张与之一致才放行）；</li>
 *   <li>{@code value}（当前值：integer 形态恒落 int32 域，boolean 形态恒
 *       属 {0,1}，long 形态全域且加法溢出按二进制补码 wrap——与 JDK
 *       {@code AtomicLong}/{@code AtomicInteger} 的溢出语义逐项一致）；</li>
 *   <li>{@code version}（版本戳：每次成功改变值的写操作恰 +1；GET 与
 *       未落值的失败 CAS 均不推进。该单调性是超时复判与 ABA 消除的
 *       唯一依据，MUST NOT 被任何读路径扰动）；</li>
 *   <li>去重槽 {@code (slotSession, slotOpSeq, 应答四元组)}：记录最近一条
 *       <em>被处理</em>的写操作（含 applied=false 的 CAS 未命中）及其应答。
 *       同 {@code (session, opSeq)} 重放请求直接返回存储应答——超时重发
 *       不双加、不双判定的机制本体。单槽即覆盖实际故障窗（客户端仅在
 *       最近一次请求超时后重发），更早序号的迟到重发不被去重，属
 *       已声明契约边界。</li>
 * </ul>
 *
 * <p><b>操作判定顺序</b>（{@link #op}，首个命中即为结果）：
 * <ol>
 *   <li><b>形态/值域检查</b>：命令形态与条目定型不符 →
 *       {@link Outcome#REJECT_TYPE_MISMATCH}；布尔形态的落值/期望值越出
 *       {0,1} 或携带 ADD → {@link Outcome#REJECT_ATOMIC_RANGE}；</li>
 *   <li><b>初值断言</b>：非零 {@code initialValue} 主张与定型初值不符 →
 *       {@link Outcome#REJECT_ATOMIC_INIT}；</li>
 *   <li><b>GET 短路</b>：返回当前读数，不占槽、不推版本；</li>
 *   <li><b>去重重放</b>：{@code opSeq >= 1} 且与会话槽一致 → 重放存储应答；</li>
 *   <li><b>执行写操作</b>：成功写推版本恰 +1；CAS 家族未命中不推版本；
 *       处理完毕覆盖槽位（成功与未命中均占槽）。</li>
 * </ol>
 * 前五步全部在条目监视器内完成，两两操作线性化；条件加
 * （{@code ADD} 携 {@code expectedVersion > 0}）版本不符时以
 * {@code GRANTED + applied=false} 拒绝落值，与 CAS 未命中同形。
 *
 * <p><b>与租约机制的关系</b>：{@link #leaseToken()} / {@link #leaseExpiresAtMs()}
 * 恒 0，条目永不入到期堆，{@link #forceExpire} 不可达（引擎侧无登记路径）；
 * {@link #removeSession} 为无操作——值不属于任何会话，会话的创建、持有、
 * 死亡均不构成值的回滚理由（契约可见语义，指南与客户端 Javadoc 同步声明）。
 *
 * <p><b>生命周期</b>：{@link #isEmpty()} 恒 {@code false}——本条目无天然
 * "空"点（0 值是合法状态而非回收信号），一经创建常驻至进程重启后按
 * 快照恢复；key 基数治理由服务端保护限额与观察面承担，不提供删除语义。
 *
 * <p><b>并发模型</b>：与 {@link LockEntry} 相同——全部迁移在
 * {@code synchronized(this)} 内完成；本类无通知路径（{@code notify} 参数
 * 恒不产生），{@link #waiterCount()} 恒 0。快照读数方法须在持有条目锁时
 * 调用（引擎与状态机侧保证）。
 */
public final class AtomicEntry implements KeyEntry {

    /** 原子变量键。 */
    private final String key;
    /** 形态判别（创建定型，之后不变）。 */
    private final LockType kind;
    /** 定型初值（非零主张时记录主张值；无主张为 0）。 */
    private final long initial;
    /** 当前值（kind 值域内）。 */
    private long value;
    /** 版本戳（每次成功写恰 +1）。 */
    private long version;
    /** 去重槽：最近被处理写操作的逻辑会话（0=空槽）。 */
    private long slotSession;
    /** 去重槽：该写操作的 op_seq。 */
    private long slotOpSeq;
    /** 去重槽：存储应答 applied。 */
    private boolean slotApplied;
    /** 去重槽：存储应答 oldValue。 */
    private long slotOldValue;
    /** 去重槽：存储应答 value。 */
    private long slotValue;
    /** 去重槽：存储应答 version。 */
    private long slotVersion;

    /**
     * 构造原子条目（懒建路径：引擎在首条写命令上调用）。
     *
     * @param key     原子变量键
     * @param kind    形态判别（须为三原子类型之一）
     * @param initial 建条目初值（非零主张值或默认的 0）
     * @throws NullPointerException     key/kind 为 {@code null}
     * @throws IllegalArgumentException kind 非原子形态
     */
    public AtomicEntry(String key, LockType kind, long initial) {
        this.key = Objects.requireNonNull(key);
        if (kind != LockType.ATOMIC_LONG && kind != LockType.ATOMIC_INTEGER
                && kind != LockType.ATOMIC_BOOLEAN) {
            throw new IllegalArgumentException("not an atomic kind: " + kind);
        }
        this.kind = kind;
        this.initial = initial;
        this.value = initial;
    }

    /**
     * 快照重建工厂：以全字段快照直接装配，不经 {@link #op} 判定路径；
     * 供 {@code CoreEngine.restoreFrom} 与复制状态装载使用。
     *
     * @param key          原子变量键
     * @param kind         形态判别
     * @param initial      定型初值
     * @param value        当前值
     * @param version      版本戳
     * @param slotSession  去重槽会话（0=空槽）
     * @param slotOpSeq    去重槽序号
     * @param slotApplied  槽应答 applied
     * @param slotOldValue 槽应答 oldValue
     * @param slotValue    槽应答 value
     * @param slotVersion  槽应答 version
     * @return 快照初态的条目
     */
    public static AtomicEntry restored(String key, LockType kind, long initial, long value,
            long version, long slotSession, long slotOpSeq, boolean slotApplied,
            long slotOldValue, long slotValue, long slotVersion) {
        AtomicEntry e = new AtomicEntry(key, kind, initial);
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
     * 执行一条原子操作命令。判定顺序与零扰动规则见类注释；
     * 本方法不校验会话存在性（引擎侧完成，与锁家族的条目内规则分工一致）。
     *
     * @param cmd 原子操作命令（{@code kind} 与条目形态不符时拒）
     * @return 操作结果（{@link Outcome#GRANTED} 携带应答四元组，
     *         或形态/值域/初值类拒绝——拒绝态四元组为零值形）
     */
    public synchronized AtomicOpResult op(AtomicOpCommand cmd) {
        // 规则 1a：形态互拒（同家族跨形态按类型不匹配处理，零扰动）。
        if (cmd.kind() != kind) {
            return AtomicOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
        }
        // 规则 1b：布尔值域与 ADD 不适用于布尔形态。
        Outcome rangeBad = checkBooleanRange(cmd);
        if (rangeBad != null) {
            return AtomicOpResult.rejected(rangeBad);
        }
        // 规则 2：初值断言（0 不主张；与定型初值不符拒绝，零扰动）。
        if (cmd.initialValue() != 0 && cmd.initialValue() != initial) {
            return AtomicOpResult.rejected(Outcome.REJECT_ATOMIC_INIT);
        }
        // 规则 3：GET 短路——不占槽、不推版本（applied=false 与未命中 CAS 同旗标，
        // 区分由应答回显的 op 承担）。
        if (cmd.op() == AtomicOp.GET) {
            return new AtomicOpResult(Outcome.GRANTED, false, value, value, version);
        }
        // 规则 4：去重重放（同会话同序号；GET 不入此路径）。
        if (cmd.opSeq() >= 1 && cmd.sessionId() == slotSession && cmd.opSeq() == slotOpSeq) {
            return new AtomicOpResult(Outcome.GRANTED, slotApplied,
                    slotOldValue, slotValue, slotVersion);
        }
        // 规则 5：执行写操作。
        AtomicOpResult r = execute(cmd);
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
     * 写操作执行体（{@link #op} 规则 5，条目锁内调用）：按 op 分派落值，
     * 成功改变值时版本戳恰 +1。未命中 CAS 与条件加版本不符不推版本、
     * 仍产出 {@code GRANTED + applied=false} 的有效读数。
     *
     * @param cmd 已通过形态/值域/断言检查的写命令
     * @return 操作结果
     */
    private AtomicOpResult execute(AtomicOpCommand cmd) {
        return switch (cmd.op()) {
            case GET -> throw new IllegalStateException("GET handled before execute");
            case SET, GET_AND_SET -> {
                long old = value;
                value = toDomain(cmd.operand());
                version++;
                yield new AtomicOpResult(Outcome.GRANTED, true, old, value, version);
            }
            case ADD -> {
                if (cmd.expectedVersion() > 0 && cmd.expectedVersion() != version) {
                    // 条件加断言不符：拒绝落值，读数即当前态（与未命中 CAS 同形）。
                    yield new AtomicOpResult(Outcome.GRANTED, false, value, value, version);
                }
                long old = value;
                value = toDomain(value + cmd.operand());
                version++;
                yield new AtomicOpResult(Outcome.GRANTED, true, old, value, version);
            }
            case CAS -> {
                if (cmd.expected() != value) {
                    yield new AtomicOpResult(Outcome.GRANTED, false, value, value, version);
                }
                long old = value;
                value = toDomain(cmd.operand());
                version++;
                yield new AtomicOpResult(Outcome.GRANTED, true, old, value, version);
            }
            case CAS_STAMPED -> {
                boolean versionOk = cmd.expectedVersion() == 0 || cmd.expectedVersion() == version;
                if (cmd.expected() != value || !versionOk) {
                    yield new AtomicOpResult(Outcome.GRANTED, false, value, value, version);
                }
                long old = value;
                value = toDomain(cmd.operand());
                version++;
                yield new AtomicOpResult(Outcome.GRANTED, true, old, value, version);
            }
        };
    }

    /**
     * 布尔形态值域与操作适用性检查：非布尔形态恒通过；布尔形态的
     * SET/GET_AND_SET/CAS/CAS_STAMPED 落值与期望值 MUST 属 {0,1}，
     * ADD 对布尔形态不适用（客户端面不存在该组合，服务端权威兜底）。
     *
     * @param cmd 待检命令
     * @return 拒绝态；通过为 {@code null}
     */
    private static Outcome checkBooleanRange(AtomicOpCommand cmd) {
        if (cmd.kind() != LockType.ATOMIC_BOOLEAN) {
            return null;
        }
        return switch (cmd.op()) {
            case ADD -> Outcome.REJECT_ATOMIC_RANGE;
            case SET, GET_AND_SET -> inBool(cmd.operand()) ? null : Outcome.REJECT_ATOMIC_RANGE;
            case CAS, CAS_STAMPED -> inBool(cmd.operand()) && inBool(cmd.expected())
                    ? null : Outcome.REJECT_ATOMIC_RANGE;
            case GET -> null;
        };
    }

    /**
     * 值是否属布尔值域 {0,1}。
     *
     * @param v 待检值
     * @return 属域为 {@code true}
     */
    private static boolean inBool(long v) {
        return v == 0 || v == 1;
    }

    /**
     * 形态值域折算：INTEGER 截断为 int32（溢出 wrap，与 JDK
     * {@code AtomicInteger} 一致）；LONG/BOOLEAN 原值（布尔的落值合法性
     * 已在 {@link #checkBooleanRange} 保证，此处不二次折算）。
     *
     * @param v 待折算值
     * @return 值域内的存储值
     */
    private long toDomain(long v) {
        return kind == LockType.ATOMIC_INTEGER ? (int) v : v;
    }

    /**
     * 形态判别（快照序列化与管理观察读取）。须在持有条目锁时调用。
     *
     * @return 定型形态
     */
    public synchronized LockType kind() {
        return kind;
    }

    /**
     * 定型初值（断言比对基准，快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 定型初值（无主张创建为 0）
     */
    public synchronized long initial() {
        return initial;
    }

    /**
     * 当前值（快照序列化侧与管理观察读取）。须在持有条目锁时调用。
     *
     * @return 值域内的当前值
     */
    public synchronized long value() {
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
     * 去重槽应答 oldValue 读数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 槽应答 oldValue
     */
    synchronized long slotOldValue() {
        return slotOldValue;
    }

    /**
     * 去重槽应答 value 读数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 槽应答 value
     */
    synchronized long slotValue() {
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

    /** 恒 0：原子条目无租约（契约读数占位，到期堆永不登记本条目）。 */
    @Override
    public long leaseToken() {
        return 0;
    }

    /** 恒 0：原子条目无到期时刻（{@link #leaseToken()} 同理）。 */
    @Override
    public long leaseExpiresAtMs() {
        return 0;
    }

    /**
     * 恒 {@code false}：原子条目无常驻判据的"空"点——0 值是合法状态而非
     * 回收信号，删除会引入"删后读数回 0"的复活竞态且无任何安全删除时机
     * （见类注释生命周期节）。
     *
     * @return 恒 {@code false}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * 无操作：值不绑定会话归属，会话关闭 MUST NOT 改变本条目的值、版本戳
     * 与去重槽（引擎侧原子命令不登记会话触及集，本方法对原子条目实际
     * 不可达；实现保留为契约的防御声明位）。
     *
     * @param sessionId          要清理的会话（忽略）
     * @param now                当前时刻（毫秒，忽略）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒，忽略）
     * @param notify             通知收集列表（恒不产出）
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        // 无操作：原子值与会话生死无关（契约声明，勿在此添加任何清理）。
    }

    /**
     * 不可达：原子条目无租约、永不入到期堆（引擎 {@code expireDue} 无此
     * 条目的堆记录）。以断言级无操作实现，防御调用路径意外触达。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（原子条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("atomic entry has no lease to expire");
    }

    /**
     * 恒 {@code false}：原子条目无等待队列，"已通知待重发"状态机对其
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

    /** 恒 0：原子条目无等待队列（统计观察面）。 */
    @Override
    public synchronized int waiterCount() {
        return 0;
    }

    /**
     * 明细只读快照：条目锁内拷贝形态、定型初值、当前值与版本戳。
     * 原子家族无租约、无持有者、无等待队列，对应字段恒零值/空表；
     * 去重槽不入本快照（属复制状态而非观察字段）。纯读，MUST NOT 改变任何状态。
     *
     * @param now 采样时刻（毫秒，引擎时钟）
     * @return 本条目自洽的不可变快照
     */
    public synchronized CoreInspection.KeySnapshot snapshot(long now) {
        return new CoreInspection.KeySnapshot(key, KeyFamily.ATOMIC, false,
                0, 0, 0, 0,
                List.of(), List.of(),
                0, 0, 0, 0, List.of(),
                kind, initial, value, version,
                0, 0, 0, false, null);
    }
}
