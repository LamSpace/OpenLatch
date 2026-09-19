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

/**
 * 循环屏障破障异常：等待项所属世代已破障的在带裁决。
 *
 * <p><b>触发面</b>（离场即破障，作用域限该世代）：同世代任一到场方的
 * {@link OBarrier#await(long, java.util.concurrent.TimeUnit)} 超时或本地中断、
 * 显式 {@link OBarrier#breakBarrier()}、动作执行者抛异常、以及任一在队到场方
 * 的会话死亡（进程退出/失联清理）。破障后新到场进入新世代正常合拢，
 * 破障不粘滞——这与 JDK {@code CyclicBarrier} 的"broken 持续到 reset"
 * 是有意差异（跨进程无"下一批无辜参与者应被历史破障卡死"的理由）。
 *
 * <p><b>异常形态</b>：非受检（库内失败一律非受检的统一纪律），替代 JDK 的
 * 受检 {@code BrokenBarrierException}；迁移 JDK 代码时须显式捕获本异常。
 */
public class OBrokenBarrierException extends OpenLatchException {

    /** 序列化版本标识。 */
    private static final long serialVersionUID = 1L;

    /**
     * 以破障事实消息构造。
     *
     * @param message 说明（通常含屏障键）
     */
    public OBrokenBarrierException(String message) {
        super(message);
    }

    /**
     * 以破障事实与原因构造。
     *
     * @param message 说明
     * @param cause   原因
     */
    public OBrokenBarrierException(String message, Throwable cause) {
        super(message, cause);
    }
}
