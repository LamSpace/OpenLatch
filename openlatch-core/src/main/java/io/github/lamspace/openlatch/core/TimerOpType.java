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

/**
 * 延时触发操作判别（TIMER 家族命令通道的操作词表）。
 *
 * <p>与协议 {@code openlatch.TimerOp} 逐项对应（枚举序即线路值：0–4），
 * server 层负责映射；core 不感知线路形态。复制边界按状态迁移性分轨：
 * {@link #SCHEDULE}/{@link #DISARM} 为变异操作（每一变异恰一条复制条目——
 * 装载决定"未来某刻对谁可见"是复制态，沿 v5/v10"迁移入日志"纪律，
 * 落地纪律第 2 条对本原语不豁免）；{@link #AWAIT}/{@link #CANCEL}/
 * {@link #QUERY} 零日志（等待登记与摘除、读数是 Leader 本地裁决——
 * "等待是订阅不是状态、观察不是迁移"判例族的直接延伸）。
 *
 * <p><b>到期不占操作词</b>：{@code marked = armed ∧ 判定时刻 ≥ fireAtMs}
 * 是账簿与钟的纯函数——"到期不是迁移，是时间的兑现"（v7 就绪驱动
 * "到期是可见性判定"公式的类目化）。不存在也不需要"fire"条目或词值：
 * 时钟走过到期点时日志恒零新增，此为常驻反向守卫（与"等待误入日志即红"
 * 对偶）。
 */
public enum TimerOpType {
    /** 装载/重装载：相对延迟在应用点折算绝对到期时刻；换代清钟（代次 +1、粘滞标记归伪）；无条目时建条目并定型 TIMER 家族。 */
    SCHEDULE,
    /** 撤销装载：当代代终结粘滞（永不再可标记），同关键区唤醒全体在等旁观者以撤销终态了结；幂等回声。 */
    DISARM,
    /** 等待当代标记：已到期即刻完成、已撤销即刻拒绝、未到期挂起——谓词当下重评即完备，无代次入参。 */
    AWAIT,
    /** 撤销等待登记：按 (会话, 请求) 摘除等待项；幂等，无对应登记亦成功（判例条件 LEAVE / v10 CANCEL）。 */
    CANCEL,
    /** 读数：代次/装载态/绝对到期时刻/到期折算四元组，零迁移零推进（Leader 本地，不入日志）。 */
    QUERY
}
