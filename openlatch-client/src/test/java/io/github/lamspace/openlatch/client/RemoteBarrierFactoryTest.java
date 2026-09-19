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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 循环屏障句柄工厂的参数校验（无服务器形态）：null key / null action 拒绝、
 * 非正 parties 拒绝、返回类型与 key/getParties 读数正确（纯加入句柄在
 * 首次应答前回 0）。行为语义在 {@code ClientBarrierIT} 钉死。
 */
class RemoteBarrierFactoryTest {

    /** 未连接客户端（工厂不触发网络，操作才触发）。 */
    private final OpenLatchClient client =
            OpenLatchClient.builder().address("127.0.0.1:1").build();

    @Test
    void factoriesReturnHandlesWithKeyAndParties() {
        OBarrier creator = client.newBarrier("stage", 3);
        OBarrier withAction = client.newBarrier("stage", 3, () -> { });
        OBarrier joiner = client.newBarrier("stage");
        assertThat(creator).isInstanceOf(RemoteBarrier.class);
        assertThat(withAction).isInstanceOf(RemoteBarrier.class);
        assertThat(joiner).isInstanceOf(RemoteBarrier.class);
        assertThat(creator.key()).isEqualTo("stage");
        assertThat(creator.getParties()).isEqualTo(3);
        assertThat(joiner.getParties()).isZero(); // 未观测（纯加入句柄无网络读数）
        assertThat(joiner.isBroken()).isFalse();
    }

    @Test
    void nullAndNonPositiveArgumentsRejected() {
        assertThatThrownBy(() -> client.newBarrier(null, 2))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.newBarrier(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.newBarrier("k", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.newBarrier("k", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.newBarrier("k", 2, null))
                .isInstanceOf(NullPointerException.class);
    }
}
