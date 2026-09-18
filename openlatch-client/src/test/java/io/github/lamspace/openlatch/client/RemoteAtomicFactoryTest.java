package io.github.lamspace.openlatch.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 原子句柄工厂的参数校验（无服务器形态）：null key 拒绝、负初值主张拒绝、
 * 返回类型与 key 读数正确。行为语义在 {@code ClientAtomicIT} 钉死。
 */
class RemoteAtomicFactoryTest {

    /** 未连接客户端（工厂不触发网络，操作才触发）。 */
    private final OpenLatchClient client =
            OpenLatchClient.builder().address("127.0.0.1:1").build();

    @Test
    void factoriesReturnMatchingFormsAndKey() {
        OAtomicLong l = client.newAtomicLong("counter");
        OAtomicInteger i = client.newAtomicInteger("counter", 1);
        OAtomicBoolean b = client.newAtomicBoolean("flag", true);
        assertThat(l.key()).isEqualTo("counter");
        assertThat(i.key()).isEqualTo("counter");
        assertThat(b.key()).isEqualTo("flag");
        assertThat(l).isInstanceOf(RemoteAtomicLong.class);
        assertThat(i).isInstanceOf(RemoteAtomicInteger.class);
        assertThat(b).isInstanceOf(RemoteAtomicBoolean.class);
    }

    @Test
    void nullKeyRejected() {
        assertThatThrownBy(() -> client.newAtomicLong(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.newAtomicInteger(null, 1))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> client.newAtomicBoolean(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void negativeInitialClaimRejected() {
        assertThatThrownBy(() -> client.newAtomicLong("k", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.newAtomicInteger("k", -2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
