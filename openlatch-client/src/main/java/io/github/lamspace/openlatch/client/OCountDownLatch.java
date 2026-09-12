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

import java.util.concurrent.TimeUnit;

/**
 * 分布式倒计数屏障：一次性倒计数栅栏，
 * 计数归零瞬间全体等待者放行。
 *
 * <p><b>定型与初始化</b>：初始计数由携带非零 {@code total} 断言的首个请求
 * （{@code newCountDownLatch(key, count)} 创建者句柄的任一操作）定型；
 * 纯加入句柄（{@code newCountDownLatch(key)}）对不存在（或参与者散尽后
 * 回收）的屏障被以 {@code INVALID_REQUEST} 拒绝。屏障无租约：等待者不
 * 持有资源、不参与看门狗与续租。
 *
 * <p><b>一次性</b>：归零后 {@code await} 立即通过、{@code countDown} 为
 * 无操作；不支持重置——新一轮屏障使用新 key。
 *
 * <p><b>countDown 的投递语义</b>：至多一次——请求超时以异常结束并交由
 * 调用方裁决（服务端结果不可知，客户端不自动重发以防重复扣减）。
 * await 则全程幂等：通知驱动的同 id 重发与断线重连后的自动重发在
 * 服务端按 {@code (会话, 请求)} 去重。
 *
 * <p><b>实例化</b>：{@link OpenLatchClient#newCountDownLatch(String, long)}
 * （创建者/断言句柄）与 {@link OpenLatchClient#newCountDownLatch(String)}
 * （纯加入句柄）。句柄无状态，同一 key 可多实例并存。
 */
public interface OCountDownLatch {

    /**
     * 屏障键。
     *
     * @return 屏障键
     */
    String key();

    /**
     * 等待屏障归零（受客户端等待兜底超时约束）。断线重连后自动重发
     * （幂等），屏障已归零时立即通过。
     *
     * @throws InterruptedException          等待被中断
     * @throws LockAcquisitionTimeoutException 兜底超时仍未归零
     * @throws OpenLatchException            屏障不存在（纯加入）/ 定型不符 /
     *                                       队列满等被服务端显式拒绝
     */
    void await() throws InterruptedException;

    /**
     * 限时等待屏障归零。
     *
     * @param timeout 等待时长
     * @param unit    时长单位
     * @return 归零放行返回 {@code true}；到时未归零返回 {@code false}
     * @throws InterruptedException     等待被中断
     * @throws IllegalArgumentException {@code timeout < 0}
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 纯初始化：不扣减、只定型（携带创建者句柄的 {@code total} 断言建立
     * 屏障或校验既有定型）。创建者"先建屏障、后派工、再 await"的编排入口。
     *
     * @return 当前剩余计数
     * @throws OpenLatchException     纯加入句柄（无断言能力）或定型不符
     * @throws OpenLatchTimeoutException 请求超时（结果不可知）
     */
    long init();

    /**
     * 倒计数 1（至多一次投递，见接口契约）。
     *
     * @return 生效后的剩余计数
     * @throws OpenLatchException     服务端拒绝（屏障不存在且未定型 / 定型不符）
     * @throws OpenLatchTimeoutException 请求超时（服务端结果不可知，不自动重发）
     */
    long countDown();

    /**
     * 倒计数 {@code n}（下限 0；归零瞬间全体等待者放行）。
     *
     * @param n 扣减量（{@code >= 1}）
     * @return 生效后的剩余计数
     * @throws OpenLatchException     服务端拒绝或非法参数
     * @throws OpenLatchTimeoutException 请求超时（结果不可知，不自动重发）
     * @throws IllegalArgumentException {@code n < 1}
     */
    long countDown(long n);
}
