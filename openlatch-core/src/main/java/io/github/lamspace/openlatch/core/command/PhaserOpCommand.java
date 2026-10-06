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

import io.github.lamspace.openlatch.core.PhaserOpType;

/**
 * 相位器操作命令（PHASER 家族唯一命令通道的输入值对象）。
 *
 * <p><b>职责</b>：承载一次相位器操作的全部判定输入——身份（会话/请求）、
 * key、操作判别、注册计数、已见相位号（带 presence）与待撤销等待请求号。
 * 本记录只作数据搬运，不做语义校验；形状合法性（字段-操作互斥矩阵、
 * parties 下界、expected_phase 非负）属接入层裁决，抵达 core 的命令视为
 * 已受理（条目侧 MUST NOT 复核——判例 {@code QueueOpCommand} 的
 * "钳制属接入层"纪律）。
 *
 * <p><b>字段择用</b>（互斥矩阵由接入层钉定，条目按 op 消费）：
 * {@code parties} 仅 REGISTER 有效（≥1；A_D 的离场扣减量恒为 1，不占本
 * 字段）；{@code expectedPhase} 非 {@code null} 仅 AWAIT_ADVANCE 有效
 * （等待谓词=当前相位 > 本值；{@code awaitAdvance(0)} 合法故用可空
 * 承载"未主张"与"主张 0"两态——线路侧对应 {@code optional sint64} 的
 * presence 纪律）；{@code awaitRequestId} 仅 CANCEL 有效（指向被撤销
 * 等待项的原请求信封 request_id）。
 *
 * @param sessionId       发起会话（引擎内部 sid）
 * @param requestId       原请求信封 request_id（到场去重槽与等待登记的
 *                        身份位——应答丢失重发/换主重挂的幂等锚点）
 * @param key             相位器键
 * @param op              操作判别
 * @param parties         注册计数（仅 REGISTER 消费，其余忽略）
 * @param expectedPhase   已见相位号（仅 AWAIT_ADVANCE 消费；{@code null}=未主张）
 * @param awaitRequestId  被撤销等待项的原 request_id（仅 CANCEL 消费）
 */
public record PhaserOpCommand(
        long sessionId,
        long requestId,
        String key,
        PhaserOpType op,
        int parties,
        Long expectedPhase,
        long awaitRequestId) {
}
