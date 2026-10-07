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

import io.github.lamspace.openlatch.core.TimerOpType;

/**
 * 延时触发操作命令（TIMER 家族唯一命令通道的输入值对象）。
 *
 * <p><b>职责</b>：承载一次延时触发操作的全部判定输入——身份（会话/请求）、
 * key、操作判别、相对延迟与待撤销等待请求号。本记录只作数据搬运，不做
 * 语义校验；形状合法性（字段-操作互斥矩阵、delay 下界非负）属接入层
 * 裁决，抵达 core 的命令视为已受理（条目侧 MUST NOT 复核——判例
 * {@code PhaserOpCommand}/{@code QueueOpCommand} 的"钳制属接入层"纪律；
 * horizon 上限判定在受理点门面，条目应用侧不消费）。
 *
 * <p><b>字段择用</b>（互斥矩阵由接入层钉定，条目按 op 消费）：
 * {@code delayMs} 仅 SCHEDULE 有效（≥0；0 即立即可共见——合法装载，
 * {@code null}=未主张仅出现在非 SCHEDULE 命令的搬运中，条目忽略）；
 * {@code awaitRequestId} 仅 CANCEL 有效（指向被撤销等待项的原请求信封
 * request_id）。DISARM/AWAIT/QUERY 两字段均不消费。
 *
 * @param sessionId       发起会话（引擎内部 sid；装载归属记账仅用于去重槽，
 *                        触发本身绑定 key 不绑定会话）
 * @param requestId       原请求信封 request_id（装载去重槽与等待登记的
 *                        身份位——应答丢失重发/换主重挂的幂等锚点，
 *                        重挂 MUST 以原 id 重发——v10 真缺陷教训护栏）
 * @param key             延时触发键
 * @param op              操作判别
 * @param delayMs         相对延迟毫秒（仅 SCHEDULE 消费；{@code null}=未主张）
 * @param awaitRequestId  被撤销等待项的原 request_id（仅 CANCEL 消费）
 */
public record TimerOpCommand(
        long sessionId,
        long requestId,
        String key,
        TimerOpType op,
        Long delayMs,
        long awaitRequestId) {
}
