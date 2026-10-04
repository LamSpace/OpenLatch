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

package io.github.lamspace.openlatch.core.command;

import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.QueueOpType;

/**
 * 队列操作命令（QUEUE 家族唯一命令通道的输入值对象）。
 *
 * <p><b>职责</b>：承载一次队列操作的全部判定输入——身份（会话/请求）、
 * key、形态判别、阻塞位、容量断言、元素载荷、延时、批量上限与写序号。
 * 本记录只作数据搬运，不做语义校验；形状合法性（元素非 null、字段-形态
 * 互斥矩阵、尺寸钳制）属接入层裁决，抵达 core 的命令视为已受理（条目侧
 * MUST NOT 复核载荷尺寸/容量上限/批量预算——节点本地配置参与判定会引入
 * 跨副本回放分歧）。
 *
 * <p><b>不可变性</b>：{@code element} 数组在构造时做防御性复制，记录深
 * 不可变，跨线程传递无需额外同步。
 *
 * @param sessionId   发起会话（引擎内部 sid）
 * @param requestId   原请求信封 request_id（挂起去重与唤醒重发的身份位）
 * @param key         队列键
 * @param kind        形态判别（{@link LockType#QUEUE} / {@link LockType#DELAY_QUEUE}）
 * @param op          操作判别
 * @param blocking    阻塞位：true=不可满足时挂起等待（仅 PUT/TAKE 有意义）；
 *                    false=立即式，不可满足回 DENIED
 * @param capacity    容量定型断言（非零主张判例 permits_total：建条目必须非零，
 *                    既有条目上非零主张须与定型值一致，0=不主张）
 * @param element     PUT 的元素载荷（非 null——元素非空属接入层已校验的既定事实；
 *                    零长度数组为合法空串元素；非 PUT 为 {@code null}）
 * @param delayMs     延时注入（仅 DELAY_QUEUE 形态 PUT 有效，≥0；绝对到期时刻
 *                    由应用点以条目携带时刻折算，命令侧只承载相对值）
 * @param maxElements DRAIN 提取上限（>0；0=取条目容量为缺省上限；非 DRAIN 恒 0）
 * @param opSeq       会话内单调写序号（PUT/TAKE/DRAIN ≥1；PEEK/SIZE 恒 0 不
 *                    参与去重）
 */
public record QueueOpCommand(
        long sessionId,
        long requestId,
        String key,
        LockType kind,
        QueueOpType op,
        boolean blocking,
        long capacity,
        byte[] element,
        long delayMs,
        int maxElements,
        long opSeq) {

    /**
     * 构造并对元素载荷做防御性复制。
     */
    public QueueOpCommand {
        element = element == null ? null : element.clone();
    }
}
