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

import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 集群运行时装配（design D6：{@code RaftSubsystem} 与 §9 配置并入 gateway
 * 交付的落点）：按固定顺序组装子系统、网关、等待队列、会话协调器与到期
 * 驱动，并把消费端点交给接入层（{@code ServerSessionHandler} 的集群分支）。
 *
 * <p><b>构造顺序契约</b>：{@link #create} 完成全部装配并启动 Raft 服务——
 * 调用方 MUST 在开放客户端接入端口之前完成（spec"先组网后开端口"）；
 * {@link #close} 逆序：在途回执以可重试错误终结 → 摘除探针/扫描线程 →
 * 关停 Raft 服务。
 *
 * <p><b>线程模型</b>：装配/关停由服务器启动/关停线程独占；创建后各组件
 * 并发性遵循各自类注释（网关在应用线程回调，探针在守护线程）。
 */
public final class ClusterRuntime {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(ClusterRuntime.class);

    /** Raft 子系统（server + 客户端池 + 内核/状态机）。 */
    private final RaftSubsystem subsystem;
    /** Leader 侧等待队列（任期作用域）。 */
    private final WaitQueue waitQueue;
    /** 复制网关（提交通道与应用回执完成点）。 */
    private final ReplicationGateway gateway;
    /** 会话集群协调器。 */
    private final SessionCoordinator sessionCoordinator;
    /** 租约到期驱动（Leader 扫描提交）。 */
    private final LeaseExpiryDriver expiryDriver;
    /** 写请求集群处理器。 */
    private final ClusterRequestHandler requestHandler;

    /**
     * 私有装配构造（仅 {@link #create} 调用）。
     *
     * @param subsystem          Raft 子系统
     * @param waitQueue          等待队列
     * @param gateway            复制网关
     * @param sessionCoordinator 会话协调器
     * @param expiryDriver       到期驱动
     * @param requestHandler     写请求处理器
     */
    private ClusterRuntime(RaftSubsystem subsystem, WaitQueue waitQueue,
                           ReplicationGateway gateway, SessionCoordinator sessionCoordinator,
                           LeaseExpiryDriver expiryDriver, ClusterRequestHandler requestHandler) {
        this.subsystem = subsystem;
        this.waitQueue = waitQueue;
        this.gateway = gateway;
        this.sessionCoordinator = sessionCoordinator;
        this.expiryDriver = expiryDriver;
        this.requestHandler = requestHandler;
    }

    /**
     * 组装并启动集群运行时。
     *
     * @param clusterConfig 集群配置（{@code enabled=true} 且已校验）
     * @param config        服务器配置（限额/租约参数）
     * @param registry      连接注册表（AWAIT_NOTIFY 投递与断连路由）
     * @return 已启动的运行时
     * @throws IOException Raft 服务启动失败（端口占用、存储不可写）
     */
    public static ClusterRuntime create(ClusterConfig clusterConfig, ServerConfig config,
                                        ServerSessionRegistry registry) throws IOException {
        RaftSubsystem subsystem = new RaftSubsystem(clusterConfig, config.toCoreConfig());
        subsystem.start();
        WaitQueue waitQueue = new WaitQueue(config.maxQueueDepthPerKey(), config.headReplyTimeoutMs());
        ReplicationGateway gateway =
                new ReplicationGateway(subsystem, subsystem.core(), waitQueue, registry);
        SessionCoordinator sessionCoordinator = new SessionCoordinator(
                subsystem, gateway, subsystem.core(), registry, config);
        LeaseExpiryDriver expiryDriver =
                new LeaseExpiryDriver(subsystem, subsystem.core(), gateway, config.leaseTickIntervalMs());
        gateway.setExpiryDriver(expiryDriver);
        expiryDriver.start();
        ClusterRequestHandler handler =
                new ClusterRequestHandler(gateway, subsystem.core(), waitQueue, config);
        log.info("cluster runtime up: node={}, peers={}", clusterConfig.nodeId(), clusterConfig.peers());
        return new ClusterRuntime(subsystem, waitQueue, gateway, sessionCoordinator,
                expiryDriver, handler);
    }

    /**
     * 逆序关停：在途可重试终结 → 探针/扫描线程 → Raft 服务。幂等。
     */
    public void close() {
        gateway.close();
        sessionCoordinator.close();
        expiryDriver.close();
        subsystem.close();
        log.info("cluster runtime down: node={}", subsystem.clusterConfig().nodeId());
    }

    /**
     * 写请求集群处理器（接入层路由目标）。
     *
     * @return 处理器
     */
    public ClusterRequestHandler requestHandler() {
        return requestHandler;
    }

    /**
     * 语义内核（只读消费：digest/预检）。
     *
     * @return 内核
     */
    public LockStateMachineCore core() {
        return subsystem.core();
    }

    /**
     * 复制网关（测试注入条目/断言在途用）。
     *
     * @return 网关
     */
    public ReplicationGateway gateway() {
        return gateway;
    }

    /**
     * Leader 侧等待队列（断言排队深度/位次用）。
     *
     * @return 等待队列
     */
    public WaitQueue waitQueue() {
        return waitQueue;
    }

    /**
     * Raft 子系统（角色与 commitInfos 查询）。
     *
     * @return 子系统
     */
    public RaftSubsystem subsystem() {
        return subsystem;
    }

    /**
     * 会话集群协调器（HELLO 与断连传播的接入层路由目标）。
     *
     * @return 协调器
     */
    public SessionCoordinator sessionCoordinator() {
        return sessionCoordinator;
    }

    /**
     * 复制状态摘要（退出门与演练断言入口）。
     *
     * @return SHA-256 hex
     */
    public String digest() {
        return subsystem.core().digest();
    }
}
