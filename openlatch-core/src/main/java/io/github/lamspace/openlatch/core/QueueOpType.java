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
 * 队列操作判别（QUEUE 家族命令通道的操作词表）。
 *
 * <p>与协议 {@code openlatch.QueueOp} 逐项对应（枚举序即线路值：0–4），
 * server 层负责映射；core 不感知线路形态。写类操作（PUT/TAKE/DRAIN）
 * 携带会话内单调 {@code op_seq}，由条目每会话去重槽保证应答丢失重发的
 * 幂等；读类操作（PEEK/SIZE）{@code op_seq} 恒 0、不参与去重，且与写类
 * 一样即时线性化于条目锁临界区（无挂起分支）。
 */
public enum QueueOpType {
    /** 入队：容量不足时按阻塞位分派——挂起（QUEUED）或立即拒绝（DENIED）。 */
    PUT,
    /** 出队消费：无可消费元素时按阻塞位分派——挂起（QUEUED）或立即拒绝（DENIED）。 */
    TAKE,
    /** 批量摘取：恒立即式，自队首起摘出至多上限个可消费元素，空结果为正常终态。 */
    DRAIN,
    /** 队首读数：与 TAKE 同出队谓词（延时形态未到期即不可见），零迁移、不建条目。 */
    PEEK,
    /** 驻留元素数读数（延时形态含未到期项），零迁移、不建条目。 */
    SIZE
}
