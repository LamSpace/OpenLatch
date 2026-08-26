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

package io.github.lamspace.openlatch.client.internal;

import java.time.Duration;

/**
 * 客户端配置（不可变，详设 §6.7 默认值表）。
 *
 * <p>由 {@link io.github.lamspace.openlatch.client.OpenLatchClient.Builder} 校验并构建，
 * 构建后各组件只读共享，无任何可变状态。所有超时类参数必须为正数时长；
 * 重连退避上限不得小于初始退避；工作线程数至少为 1。校验在 Builder 中完成，
 * 本 record 不做重复校验。
 *
 * @param host                  服务器主机名或地址（必填）
 * @param port                  服务器端口（必填）
 * @param requestTimeout        单个请求（获取/释放/续租）的超时，默认 5s
 * @param defaultWaitTimeout    {@code lock()} 的总等待兜底超时，默认 30s
 * @param connectTimeout        TCP 连接 + 握手的总超时，默认 3s
 * @param reconnectInitialBackoff 重连指数退避初始值，默认 200ms
 * @param reconnectMaxBackoff   重连指数退避上限，默认 10s
 * @param workerThreads         客户端 Netty EventLoop 线程数，默认 1
 */
public record ClientConfig(
        String host,
        int port,
        Duration requestTimeout,
        Duration defaultWaitTimeout,
        Duration connectTimeout,
        Duration reconnectInitialBackoff,
        Duration reconnectMaxBackoff,
        int workerThreads) {
}
