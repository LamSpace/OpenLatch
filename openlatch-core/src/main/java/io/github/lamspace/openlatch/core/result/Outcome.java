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

package io.github.lamspace.openlatch.core.result;

/**
 * 获取结果。将协议单值 {@code REJECT_KEY} 细分为
 * {@link #REJECT_KEY_EMPTY} / {@link #REJECT_KEY_TOO_LONG} 两值，
 * 使 server 层无需重校验即可映射到
 * 协议 {@code KEY_EMPTY} / {@code KEY_TOO_LONG}。
 *
 * <p><b>返回步骤与优先级</b>：会话校验（{@link #REJECT_SESSION}）→
 * key 校验（{@link #REJECT_KEY_EMPTY} / {@link #REJECT_KEY_TOO_LONG}）→
 * 家族判定（key 已有他家族条目 → {@link #REJECT_TYPE_MISMATCH}，先于
 * 会话登记，条目状态零扰动）→
 * 条目内规则（重入/快路径/队首重发 → {@link #GRANTED}，立即式无快路径 →
 * {@link #DENIED}，幂等去重或入队 → {@link #QUEUED}，队列满 →
 * {@link #REJECT_QUEUE_FULL}，Semaphore 总量断言不成立 →
 * {@link #REJECT_SEMAPHORE_TOTAL}）。会话校验在预检与条目锁内各执行一次，
 * 两个检查点均返回 {@link #REJECT_SESSION}。
 *
 * <p><b>原子通道口径</b>：ATOMIC 家族的判定顺序为会话预检 → key 校验 →
 * 家族/形态判定（{@link #REJECT_TYPE_MISMATCH}）→ 布尔值域检查
 * （{@link #REJECT_ATOMIC_RANGE}）→ 初值断言（{@link #REJECT_ATOMIC_INIT}）→
 * 去重槽重放或操作执行（结果恒 {@link #GRANTED}，CAS 家族成败由
 * {@code applied} 承载、不占用本枚举判别位）。
 *
 * <p><b>屏障通道口径</b>：BARRIER 家族的判定顺序为会话预检 → key 校验 →
 * 家族判定（{@link #REJECT_TYPE_MISMATCH}）→ parties 断言
 * （{@link #REJECT_BARRIER_PARTIES}）→ 世代了结记录重发判定（已了结即回
 * {@link #GRANTED} 或 {@link #BARRIER_BROKEN}）→ 队列满护栏
 * （{@link #REJECT_QUEUE_FULL}）→ 到场入队（{@link #QUEUED}，当回合拢时
 * 无动作形态回 {@link #GRANTED}、动作形态以执行者标记回 {@link #QUEUED}）。
 * 动作了结的非指定执行者/未知世代回报回 {@link #REJECT_BARRIER_ACTION}。
 * {@link #BARRIER_BROKEN} 是在带裁决（等待项所属世代已破障）而非请求错误，
 * server 层映射协议同名状态码。
 */
public enum Outcome {
    /** 授予：携带租约凭证与实际租约。重入/快路径/队首重发命中均返回此值。 */
    GRANTED,
    /** 排队：携带 1 起的队列位次，等待队首通知后重发；重复请求去重时也返回当前位次。 */
    QUEUED,
    /**
     * 拒绝：立即式请求（{@code queueIfBusy=false}/{@code wait_ms=0}）在不存在
     * 快路径时返回——锁被占用，或虽无持有者但等待队列非空（队首已通知、
     * 待重发窗口——规则 3 禁止越过在队者）。仅当请求未排队时可能返回。
     */
    DENIED,
    /** 拒绝：锁键为 {@code null} 或空，先于长度与条目内规则校验。 */
    REJECT_KEY_EMPTY,
    /** 拒绝：锁键的 UTF-8 字节长度超过上限，先于条目内规则校验。 */
    REJECT_KEY_TOO_LONG,
    /** 拒绝：该 key 等待队列已满，先于入队动作检查。 */
    REJECT_QUEUE_FULL,
    /** 拒绝：会话不存在或已关闭；预检与条目锁内权威校验均可返回。 */
    REJECT_SESSION,
    /**
     * 拒绝：key 条目的家族与请求类型不属于同一家族（如锁 key 上请求
     * Semaphore），条目状态与会话触及集零扰动；server 层映射协议
     * {@code INVALID_REQUEST}。
     */
    REJECT_TYPE_MISMATCH,
    /**
     * 拒绝：Semaphore 许可总量断言不成立——建条目请求缺失 {@code > 0}
     * 的 {@code permitsTotal}，或既有条目上非零主张与定型值不符；
     * 条目状态零扰动，server 层
     * 映射协议 {@code INVALID_REQUEST}。
     */
    REJECT_SEMAPHORE_TOTAL,
    /**
     * 拒绝：Latch 初始计数断言不成立——屏障不存在且请求（countDown/await
     * 任一通道）未携带 {@code > 0} 的 {@code total}（含对不存在屏障的无断言
     * 扣减/挂起），或既有屏障上非零主张与定型值不符；条目状态零扰动，
     * server 层映射协议 {@code INVALID_REQUEST}。
     */
    REJECT_LATCH_TOTAL,
    /**
     * 拒绝：原子变量初值主张不成立——既有条目上非零 {@code initial_value}
     * 与定型初值不符；条目状态零扰动，server 层映射协议
     * {@code INVALID_REQUEST}（判例：{@link #REJECT_SEMAPHORE_TOTAL} /
     * {@link #REJECT_LATCH_TOTAL} 的非零主张规则）。
     */
    REJECT_ATOMIC_INIT,
    /**
     * 拒绝：原子变量参数越出形态值域——布尔形态的落值/期望值不在
     * {0,1}，或布尔形态携带 ADD 操作；条目状态零扰动，server 层
     * 映射协议 {@code INVALID_REQUEST}。
     */
    REJECT_ATOMIC_RANGE,
    /**
     * 拒绝：循环屏障许可数断言不成立——屏障不存在且请求未携带
     * {@code > 0} 的 {@code parties}（含对不存在屏障的无主张离场外操作），
     * 或既有屏障上非零主张与定型值不符；条目状态零扰动，server 层
     * 映射协议 {@code INVALID_REQUEST}（判例：{@link #REJECT_SEMAPHORE_TOTAL} /
     * {@link #REJECT_LATCH_TOTAL} 的非零主张规则）。
     */
    REJECT_BARRIER_PARTIES,
    /**
     * 拒绝：循环屏障动作了结回报不被受理——回报者非该世代指定执行者、
     * 世代号未知或已滚出窗口、或该世代并未处于动作待决态；条目状态
     * 零扰动，server 层映射协议 {@code INVALID_REQUEST}。
     */
    REJECT_BARRIER_ACTION,
    /**
     * 在带裁决：循环屏障等待项所属世代已破障（离场即破障：在队到场者
     * 超时离场/本地中断/会话死亡/显式 {@code breakBarrier()} 任一触发）；
     * 非请求错误，连接与会话不受影响，server 层映射协议同名状态码
     * {@code BARRIER_BROKEN}。
     */
    BARRIER_BROKEN
}
