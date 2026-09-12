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
import java.util.List;

/**
 * 客户端配置（不可变）。
 *
 * <p>由 {@link io.github.lamspace.openlatch.client.OpenLatchClient.Builder} 校验并构建，
 * 构建后各组件只读共享，无任何可变状态。所有超时类参数必须为正数时长；
 * 重连退避上限不得小于初始退避；工作线程数至少为 1。校验在 Builder 中完成，
 * 本 record 不做重复校验。
 *
 * <p><b>种子语义</b>：{@code host/port} 即种子列表首项（单地址入口的
 * 兼容形态）；{@code seeds} 为全部已解析种子地址（首项与 host/port 一致），
 * 断连重连先试原地址、失败后按序轮询本表；集群 Leader 改连提示地址不在表内时，
 * 强制发现亦以本表为扇出集合。
 *
 * <p><b>安全配置</b>：
 * {@code tlsEnabled=true} 时，客户端对每一次连接尝试（主连接、种子发现探针、
 * 断连重连）以 PEM 文件（{@code tlsTrustStore} 可信任 CA；mTLS 时
 * {@code tlsClientCert}/{@code tlsClientKey}）执行 TLS 握手；{@code authToken}
 * 非空时 HELLO 携带该业务令牌。安全项默认全关 = 明文无令牌形态，行为与现状
 * 逐字节一致。路径为字符串（本 record 纯数据，PEM 可读性由装配层校验）。
 *
 * @param host                  服务器主机名或地址（必填；种子列表首项）
 * @param port                  服务器端口（必填；种子列表首项端口）
 * @param seeds                 种子地址表（非空，首项 = host:port；单地址入口为一元表）
 * @param requestTimeout        单个请求（获取/释放/续租）的超时，默认 5s
 * @param defaultWaitTimeout    {@code lock()} 的总等待兜底超时，默认 30s
 * @param connectTimeout        TCP 连接 + 握手的总超时，默认 3s
 * @param reconnectInitialBackoff 重连指数退避初始值，默认 200ms
 * @param reconnectMaxBackoff   重连指数退避上限，默认 10s
 * @param workerThreads         客户端 Netty EventLoop 线程数，默认 1
 * @param tlsEnabled            是否启用 TLS（默认 false = 明文）
 * @param tlsTrustStore         可信任 CA 的 PEM 证书集路径；{@code null} 取系统默认信任
 * @param tlsClientCert         mTLS 客户端 PEM 证书路径；{@code null} 表示单向 TLS
 * @param tlsClientKey          mTLS 客户端 PEM 私钥路径；与 {@code tlsClientCert} 成对
 * @param authToken             业务令牌（HELLO 携带）；{@code null} 表示不鉴权
 */
public record ClientConfig(
        String host,
        int port,
        List<SeedAddress> seeds,
        Duration requestTimeout,
        Duration defaultWaitTimeout,
        Duration connectTimeout,
        Duration reconnectInitialBackoff,
        Duration reconnectMaxBackoff,
        int workerThreads,
        boolean tlsEnabled,
        String tlsTrustStore,
        String tlsClientCert,
        String tlsClientKey,
        String authToken) {

    /**
     * 兼容构造：种子列表 + 全部安全项默认关闭的形态（旧 9 参签名的
     * 内部构造与测试调用点）。
     *
     * @param host                  服务器主机
     * @param port                  服务器端口
     * @param seeds                 种子地址表
     * @param requestTimeout        请求超时
     * @param defaultWaitTimeout    等待兜底超时
     * @param connectTimeout        连接超时
     * @param reconnectInitialBackoff 初始退避
     * @param reconnectMaxBackoff   退避上限
     * @param workerThreads         EventLoop 线程数
     */
    public ClientConfig(String host, int port, List<SeedAddress> seeds,
            Duration requestTimeout, Duration defaultWaitTimeout, Duration connectTimeout,
            Duration reconnectInitialBackoff, Duration reconnectMaxBackoff, int workerThreads) {
        this(host, port, seeds, requestTimeout, defaultWaitTimeout, connectTimeout,
                reconnectInitialBackoff, reconnectMaxBackoff, workerThreads,
                false, null, null, null, null);
    }

    /**
     * 兼容构造：8 参形态（单地址 = 一元种子表，安全项默认关闭）。
     *
     * @param host                  服务器主机
     * @param port                  服务器端口
     * @param requestTimeout        请求超时
     * @param defaultWaitTimeout    等待兜底超时
     * @param connectTimeout        连接超时
     * @param reconnectInitialBackoff 初始退避
     * @param reconnectMaxBackoff   退避上限
     * @param workerThreads         EventLoop 线程数
     */
    public ClientConfig(String host, int port, Duration requestTimeout, Duration defaultWaitTimeout,
            Duration connectTimeout, Duration reconnectInitialBackoff, Duration reconnectMaxBackoff,
            int workerThreads) {
        this(host, port, List.of(new SeedAddress(host, port)), requestTimeout, defaultWaitTimeout,
                connectTimeout, reconnectInitialBackoff, reconnectMaxBackoff, workerThreads);
    }

    /**
     * 种子地址（host 与 port 已分离，构造期校验在 Builder 完成）。
     *
     * @param host 主机名或地址
     * @param port 端口
     */
    public record SeedAddress(String host, int port) {
    }
}
