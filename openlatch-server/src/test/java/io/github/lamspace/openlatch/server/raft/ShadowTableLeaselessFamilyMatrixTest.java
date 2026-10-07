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

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.LockType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LockType 全值矩阵绊线：逐一裁决 {@link ShadowTable#isLeaselessFamilyType}
 * 对协议 {@code LockType} 全部合法取值的判类——值域 0–5（REENTRANT/SIMPLE/
 * READ/WRITE/FAIR/SEMAPHORE）为租约形态 MUST NOT 入到期清扫跳过集，其余形态
 * MUST 入。协议新增形态时判定点与本判据必须同时表态，任一不匹配本测试即红
 * ——把"每新增常驻家族须人工记忆向清扫清单补行"的判例式复发面终结为机械
 * 守卫（影子到期误摘 phaser 修复的防再犯臂），与
 * {@code StateMachinePhaserTest} 回放道存续夹具互为分工（表级判定面 vs
 * 应用点链路面）。
 */
class ShadowTableLeaselessFamilyMatrixTest {

    @Test
    void everyLockTypeValueIsClassifiedExactlyOnce() {
        for (LockType t : LockType.values()) {
            if (t == LockType.UNRECOGNIZED) {
                continue;
            }
            int value = t.getNumber();
            if (value <= LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                // 租约形态：镜像条目到期字段携真实租约到期时刻，须入清扫判定面。
                assertThat(ShadowTable.isLeaselessFamilyType(value))
                        .as("租约形态 %s(%d) 不得入到期清扫跳过集", t.name(), value)
                        .isFalse();
            } else {
                // 常驻形态：镜像条目到期字段恒 0，误入清扫判定面即幻影键危害。
                assertThat(ShadowTable.isLeaselessFamilyType(value))
                        .as("无租约形态 %s(%d) 必须入到期清扫跳过集", t.name(), value)
                        .isTrue();
            }
        }
    }
}
