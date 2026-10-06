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
 * 相位器操作判别（PHASER 家族命令通道的操作词表）。
 *
 * <p>与协议 {@code openlatch.PhaserOp} 逐项对应（枚举序即线路值：0–6），
 * server 层负责映射；core 不感知线路形态。复制边界按状态迁移性分轨：
 * {@link #REGISTER}/{@link #ARRIVE}/{@link #ARRIVE_AND_AWAIT}/
 * {@link #ARRIVE_AND_DEREGISTER} 为变异操作（每一变异恰一条复制条目——
 * 注册配额、到场计数与相位推进全是决定未来合拢的复制态）；
 * {@link #AWAIT_ADVANCE}/{@link #CANCEL}/{@link #QUERY} 零日志——等待登记
 * 与摘除、读数是 Leader 本地裁决（"等待是订阅不是状态、观察不是迁移"，
 * "等待不入日志"判例族的 v10 类目）。
 */
public enum PhaserOpType {
    /** 注册：parties 个匿名参与者计入调用会话配额与注册总数；无条目时建条目并定型 PHASER 家族。 */
    REGISTER,
    /** 到场：当相位计数一次（不等待）；应答回显到场相位号。 */
    ARRIVE,
    /** 到场并等待：到场经复制账簿计次，等待半程为本地登记——到场恰触发合拢时直答完成，否则挂起。 */
    ARRIVE_AND_AWAIT,
    /** 到场并离场：同关键区内到场计数与调用会话配额扣减一并生效（零配额拒绝——JDK 匿名 party 的显式化收紧）。 */
    ARRIVE_AND_DEREGISTER,
    /** 纯等待：按已见相位号旁观（无需注册配额）；已推进即刻了结，否则挂起等待合拢广播。 */
    AWAIT_ADVANCE,
    /** 撤销等待登记：按 (会话, 请求) 摘除；幂等，无对应登记亦成功（判例条件 LEAVE）。 */
    CANCEL,
    /** 读数：相位号/注册总数/当前到场数三计数，零迁移零推进（Leader 本地，不入日志）。 */
    QUERY
}
