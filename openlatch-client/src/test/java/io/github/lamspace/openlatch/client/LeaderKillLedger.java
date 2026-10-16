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
 * 换主持久锚的**账本判据与有界推进纪律**（测试支撑，零产品面）。
 *
 * <p><b>为什么存在</b>：换主/启动窗内的原子读写在持续负载下会遇到契约**已声明**的
 * 显式终态（会话换代拒绝、请求预算耗尽、连接不可用——原子族为 exactly-once
 * 有意让渡活性，契约原文即"不确定窗口以 {@code getStamped()} 复核，MUST NOT
 * 盲目重发"）。锚若把这些终态一律判失败，断言的其实是**比契约更严的活性**
 * （要求每一步都在预算内一次成功）。本类把锚的判据改到契约面——
 * <b>不静默错值</b>——与 {@link ClientClusterIT} 中队列锚同款口径，但更进一步：
 * 显式失败后**不跳过安全面断言**，而是先以幂等读复核解除不确定、再据实况推进，
 * 把"换主窗内账本不得被写坏"这条断言跑完。
 *
 * <p><b>三条纪律</b>：
 * <ol>
 *   <li><b>不盲目重发</b>：写遇显式终态后先读复核——复核显示增量已生效即以成功
 *       收场（重发已生效的增量会重复生效），只有复核证明未生效才继续尝试。</li>
 *   <li><b>有界</b>：推进与复核读都受尝试上限与时限约束。放宽的是"预算内一次
 *       成功"，<b>不是</b>"可以不收敛"——真 wedge（推不动、读面整体不可用）
 *       仍以 {@link AssertionError} 收场。</li>
 *   <li><b>非法态即缺陷</b>：复核读到的账本态只允许落在"未生效／已生效一次"的
 *       集合内；第三态（双加后的值、被写成第三方载荷、版本漂移、已提交态丢失）
 *       一律 {@link AssertionError}——<b>与"被容忍的显式异常族"刻意区分开</b>，
 *       故 {@link AssertionError} 绝不被本类的任何 catch 吞掉。</li>
 * </ol>
 *
 * <p><b>判据是纯函数</b>：{@link #assertAdvanceResolved}、{@link #assertAdvanceResolvedRef}
 * 与 {@link #assertDurable} 不依赖集群、可直接喂值——{@code LeaderKillLedgerTest}
 * 用它们做**负向控制**，证明"放宽"没有变成"删掉判据"。
 *
 * <p><b>前提</b>：本类假定复核读反映的是已应用的合法态（单写者、本 keyspace 由
 * 该锚独占）。若读出现第三态，说明客户端自身的复核-推进纪律被破坏（如陈旧读导致
 * 重复生效）——那正是该报的缺陷。
 */
final class LeaderKillLedger {

    /** 推进与复核读的尝试上限（含显式失败后的重试）。 */
    static final int MAX_ATTEMPTS = 16;

    /** 单步推进时限（毫秒）：超出即判"推不动"。 */
    static final long ADVANCE_DEADLINE_MS = 30_000L;

    /** 测试支撑类禁止实例化。 */
    private LeaderKillLedger() {
    }

    // ==================== 纯判定（负向控制直接喂值） ====================

    /**
     * 增量推进的账本判据（标量）：本次逻辑增量为 {@code delta}、已确认前置值为
     * {@code pre} 时，复核读到的值 MUST 落在 {@code {pre, pre + delta}}。
     *
     * @param what     判据标识（入错误信息）
     * @param pre      已确认的前置值
     * @param delta    本次逻辑增量
     * @param observed 复核读到的值
     * @throws AssertionError 读到第三态（静默重复生效／错值）
     */
    static void assertAdvanceResolved(String what, long pre, long delta, long observed) {
        if (observed != pre && observed != pre + delta) {
            throw new AssertionError(what + "：增量复核读到非法账本态 observed=" + observed
                    + "（合法＝{" + pre + ", " + (pre + delta) + "}）——静默重复生效或错值");
        }
    }

    /**
     * 增量推进的账本判据（引用载荷）：本次期望载荷 {@code expected}、目标载荷
     * {@code update} 时，复核读到的载荷 MUST 落在 {@code {expected, update}}。
     *
     * @param what     判据标识（入错误信息）
     * @param expected 已确认的前置载荷（可为 {@code null}）
     * @param update   本次目标载荷（可为 {@code null}）
     * @param observed 复核读到的载荷（可为 {@code null}）
     * @throws AssertionError 读到第三方载荷
     */
    static void assertAdvanceResolvedRef(String what, String expected, String update,
            String observed) {
        if (!java.util.Objects.equals(observed, expected)
                && !java.util.Objects.equals(observed, update)) {
            throw new AssertionError(what + "：载荷复核读到非法账本态 observed=" + observed
                    + "（合法＝{" + expected + ", " + update + "}）——被写成第三方值");
        }
    }

    /**
     * 换主后已提交账本态的存续判据：值与版本戳 MUST 逐项等于前置已确认态
     * （已提交值不丢、不双加、版本戳不漂移）。
     *
     * @param what            判据标识（入错误信息）
     * @param observedValue   换主后读到的值
     * @param observedVersion 换主后读到的版本戳
     * @param expectedValue   前置已确认值
     * @param expectedVersion 前置已确认版本戳
     * @throws AssertionError 值或版本戳漂移
     */
    static void assertDurable(String what, long observedValue, long observedVersion,
            long expectedValue, long expectedVersion) {
        if (observedValue != expectedValue || observedVersion != expectedVersion) {
            throw new AssertionError(what + "：换主后账本漂移 observed=(" + observedValue + ", v"
                    + observedVersion + ") expected=(" + expectedValue + ", v" + expectedVersion
                    + ")——已提交值丢失／双加／版本戳漂移");
        }
    }

    // ==================== 有界推进（锚的实际步进） ====================

    /**
     * 有界把标量推进到 {@code pre + delta}：先复核、未达目标才尝试写、写遇显式
     * 终态则下一轮先复核（**不盲目重发**）。非法态即时抛错；推不动则以有界失败收场。
     *
     * @param what  判据标识
     * @param a     标量句柄
     * @param pre   已确认的前置值
     * @param delta 本次逻辑增量
     * @return 推进后的复核读数（等于 {@code pre + delta}）
     * @throws InterruptedException 等待被中断
     * @throws AssertionError       非法账本态或有界推进未达目标
     */
    static long advanceScalar(String what, OAtomicLong a, long pre, long delta)
            throws InterruptedException {
        long target = pre + delta;
        long deadline = System.currentTimeMillis() + ADVANCE_DEADLINE_MS;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Long observed = readScalarOrNull(what + "·复核", a);
            if (observed == null) {
                if (System.currentTimeMillis() >= deadline) {
                    break;
                }
                continue; // 读面瞬态不可用：有界重试
            }
            if (observed == target) {
                return observed; // 已生效（本轮写成功，或此前那次其实已生效）
            }
            assertAdvanceResolved(what, pre, delta, observed);
            try {
                long returned = a.addAndGet(delta);
                assertAdvanceResolved(what, pre, delta, returned);
                if (returned == target) {
                    return returned;
                }
            } catch (OpenLatchException explicitTerminalState) {
                // 显式终态（会话换代拒绝／预算耗尽／连接不可用）：不盲目重发，
                // 下一轮由复核决定"已生效"还是"继续推进"。
            }
            if (System.currentTimeMillis() >= deadline) {
                break;
            }
        }
        throw new AssertionError(what + "：有界推进未达目标（target=" + target + "，尝试上限="
                + MAX_ATTEMPTS + "，时限=" + ADVANCE_DEADLINE_MS + "ms）——活性面须仍可收敛");
    }

    /**
     * 有界把引用载荷从 {@code expected} 推到 {@code update}（CAS 形态：期望值 +
     * 每会话去重槽双保险）。先复核、未达目标才尝试 CAS、遇显式终态则下一轮先复核。
     *
     * @param what     判据标识
     * @param r        引用句柄
     * @param expected 已确认的前置载荷（可为 {@code null}）
     * @param update   本次目标载荷（可为 {@code null}）
     * @return 推进后的复核读数（等于 {@code update}）
     * @throws InterruptedException 等待被中断
     * @throws AssertionError       非法账本态或有界推进未达目标
     */
    static String advanceRef(String what, OAtomicReference r, String expected, String update)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + ADVANCE_DEADLINE_MS;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            RefRead read = readRefOrNull(what + "·复核", r);
            if (read == null) {
                if (System.currentTimeMillis() >= deadline) {
                    break;
                }
                continue; // 读面瞬态不可用：有界重试
            }
            if (java.util.Objects.equals(read.payload(), update)) {
                return read.payload();
            }
            // 读成功但载荷不是目标：只允许停在"未生效"的前置载荷上（读到 null 态
            // 与"读失败"由 RefRead 区分——已提交载荷丢失属非法态，须报缺陷）
            assertAdvanceResolvedRef(what, expected, update, read.payload());
            try {
                r.compareAndSetString(expected, update);
            } catch (OpenLatchException explicitTerminalState) {
                // 显式终态：下一轮由复核决定
            }
            if (System.currentTimeMillis() >= deadline) {
                break;
            }
        }
        throw new AssertionError(what + "：有界推进未达目标（update=" + update + "，尝试上限="
                + MAX_ATTEMPTS + "，时限=" + ADVANCE_DEADLINE_MS + "ms）——活性面须仍可收敛");
    }

    /**
     * 有界读标量账本（值与版本戳）：读幂等可重发，但受尝试上限约束；
     * 读面整体不可用即判失败（不得静默略过断言）。
     *
     * @param what 判据标识
     * @param a    标量句柄
     * @return 读数对
     * @throws InterruptedException 等待被中断
     * @throws AssertionError       读面在有界重试内不可用
     */
    static OAtomicLong.Stamped readScalarStamped(String what, OAtomicLong a)
            throws InterruptedException {
        OpenLatchException last = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                return a.getStamped();
            } catch (OpenLatchException transientReadFailure) {
                last = transientReadFailure;
            }
        }
        throw new AssertionError(what + "：读面在有界重试内不可用（" + MAX_ATTEMPTS + " 次）", last);
    }

    /**
     * 有界读引用账本（载荷与版本戳）。
     *
     * @param what 判据标识
     * @param r    引用句柄
     * @return 读数对
     * @throws InterruptedException 等待被中断
     * @throws AssertionError       读面在有界重试内不可用
     */
    static OAtomicReference.Stamped readRefStamped(String what, OAtomicReference r)
            throws InterruptedException {
        OpenLatchException last = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                return r.getStamped();
            } catch (OpenLatchException transientReadFailure) {
                last = transientReadFailure;
            }
        }
        throw new AssertionError(what + "：读面在有界重试内不可用（" + MAX_ATTEMPTS + " 次）", last);
    }

    /**
     * 复核读标量值（显式终态回 {@code null}，由调用方决定重试或收场）。
     *
     * @param what 判据标识（仅用于诊断，不在此处抛）
     * @param a    标量句柄
     * @return 当前值；读遇显式终态返回 {@code null}
     * @throws InterruptedException 等待被中断
     */
    private static Long readScalarOrNull(String what, OAtomicLong a) throws InterruptedException {
        try {
            return a.getStamped().value();
        } catch (OpenLatchException transientReadFailure) {
            return null;
        }
    }

    /**
     * 复核读引用载荷：读成功返回 {@link RefRead}（其载荷可为 {@code null}＝真 null 态），
     * 读遇显式终态返回 {@code null}——两种"null"刻意分开，免得把"已提交载荷丢失"
     * 误当作"读不可用"而静默重试。
     *
     * @param what 判据标识（仅用于诊断，不在此处抛）
     * @param r    引用句柄
     * @return 读数；读遇显式终态返回 {@code null}
     * @throws InterruptedException 等待被中断
     */
    private static RefRead readRefOrNull(String what, OAtomicReference r)
            throws InterruptedException {
        try {
            return new RefRead(r.getStamped().valueAsString());
        } catch (OpenLatchException transientReadFailure) {
            return null;
        }
    }

    /**
     * 引用载荷读数（把"读到 null 态"与"读失败"分开的载体）。
     *
     * @param payload 载荷（可为 {@code null}＝null 态）
     */
    private record RefRead(String payload) {
    }
}
