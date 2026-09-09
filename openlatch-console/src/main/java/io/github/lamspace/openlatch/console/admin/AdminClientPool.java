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

package io.github.lamspace.openlatch.console.admin;

import io.github.lamspace.openlatch.console.ConsoleConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 节点连接池（Phase 3 T3 design D6，spec"节点连接管理与故障降级"）：
 * 配置地址列表 → 每节点一个 {@link AdminClient}（独立连接、独立退避窗），
 * 共享单个 daemon {@link EventLoopGroup}（每连接一问一答的串行流量，
 * 一线程组即可承载全部节点）。
 *
 * <p><b>生命周期</b>：Spring 容器关停时 {@link #destroy()} 收编——先关
 * 各客户端连接、再优雅终止线程组（spec"关闭时优雅释放全部对节点的连接"）。
 *
 * <p><b>线程安全</b>：客户端表构造后只读；{@link AdminClient} 自身串行化
 * 请求。多 Web 线程并发查询不同/相同节点均安全（同节点相互排队）。
 */
@Component
public final class AdminClientPool implements DisposableBean {

    /** 地址字符串（host:port）→ 客户端。 */
    private final Map<String, AdminClient> clients;
    /** 共享 IO 线程组（daemon，控制台关停统一回收）。 */
    private final EventLoopGroup group;

    /**
     * 依配置建池（不预连接——全部客户端懒握手）。
     *
     * @param config 控制台配置
     */
    public AdminClientPool(ConsoleConfig config) {
        this.group = new NioEventLoopGroup(1, r -> {
            Thread t = new Thread(r, "openlatch-console-io");
            t.setDaemon(true);
            return t;
        });
        Map<String, AdminClient> map = new LinkedHashMap<>();
        for (ConsoleConfig.Address a : config.addresses()) {
            map.put(display(a), new AdminClient(a, config.adminToken(),
                    config.requestTimeoutMs(), group));
        }
        this.clients = Map.copyOf(map);
    }

    /**
     * 全部节点客户端（配置序）。
     *
     * @return 展示名 → 客户端 的有序快照
     */
    public Map<String, AdminClient> nodes() {
        return clients;
    }

    /**
     * 指定展示名节点的客户端。
     *
     * @param display {@link #display 展示名}
     * @return 客户端；未配置为 {@code null}
     */
    public AdminClient node(String display) {
        return clients.get(display);
    }

    /**
     * 配置地址的列表序（页面循环与默认节点选择用）。
     *
     * @return 展示名有序表
     */
    public List<String> nodeNames() {
        return List.copyOf(clients.keySet());
    }

    /**
     * 地址展示名（页面标识与查询参数键）。
     *
     * @param a 地址
     * @return {@code host:port}
     */
    public static String display(ConsoleConfig.Address a) {
        return a.host() + ":" + a.port();
    }

    /**
     * 关停：逐节点断开连接，随后终止共享线程组（幂等由 Netty 保证）。
     */
    @Override
    public void destroy() {
        clients.values().forEach(AdminClient::close);
        group.shutdownGracefully();
    }
}
