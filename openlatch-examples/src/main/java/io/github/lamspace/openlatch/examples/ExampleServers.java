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

package io.github.lamspace.openlatch.examples;

import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;

/**
 * 示例共享夹具：进程内内嵌服务器（design D6）。
 *
 * <p>示例的可运行性以"自包含"为前提——不需要事先在机器上部署服务器；
 * 生产部署路径（{@code java -jar} 独立进程）见 README Quick Start。
 */
public final class ExampleServers {

    /**
     * 私有构造：工具类。
     */
    private ExampleServers() {
    }

    /**
     * 以服务端默认配置启动内嵌服务器（临时端口）。
     *
     * @return 已启动的服务器，调用方负责 {@code stop()}
     */
    public static OpenLatchServer startDefault() {
        return start(0, ServerConfig.defaults());
    }

    /**
     * 以短租约档位启动内嵌服务器（默认租约 2s、最小租约钳制降至 500ms、
     * 扫描 200ms），供看门狗/锁丢失类示例在秒级窗口内观察行为。
     *
     * @return 已启动的服务器，调用方负责 {@code stop()}
     */
    public static OpenLatchServer startFastExpiry() {
        ServerConfig d = ServerConfig.defaults();
        return start(0, new ServerConfig(0, d.workerThreads(), d.idleTimeoutMs(),
                2_000L, 500L, d.maxLeaseMs(), 200L, d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection()));
    }

    /**
     * 按给定配置启动服务器。
     *
     * @param port   监听端口（0 为临时端口）
     * @param config 服务器配置（端口以 {@code port} 为准）
     * @return 已启动的服务器
     */
    private static OpenLatchServer start(int port, ServerConfig config) {
        ServerConfig cfg = new ServerConfig(port, config.workerThreads(),
                config.idleTimeoutMs(), config.defaultLeaseMs(), config.minLeaseMs(),
                config.maxLeaseMs(), config.leaseTickIntervalMs(), config.headReplyTimeoutMs(),
                config.maxKeyLength(), config.maxQueueDepthPerKey(),
                config.maxInflightPerConnection());
        OpenLatchServer server = new OpenLatchServer(cfg);
        server.start();
        System.out.printf("[example] embedded OpenLatch server started on 127.0.0.1:%d%n",
                server.port());
        return server;
    }
}
