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

package io.github.lamspace.openlatch.server;

/**
 * 端到端测试的服务器夹具：一律绑定临时端口（0）避免端口竞争。
 */
final class TestServers {

    /** 常量工具类，禁止实例化。 */
    private TestServers() {
    }

    /**
     * 默认配置仅替换端口。
     *
     * @param port 监听端口
     * @return 服务器配置
     */
    static ServerConfig config(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), d.idleTimeoutMs(), d.defaultLeaseMs(),
                d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /**
     * 短租约快扫描配置：租约到期类端到端用例用。
     *
     * @param port 监听端口
     * @return 服务器配置
     */
    static ServerConfig fastExpiryConfig(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), d.idleTimeoutMs(), 200L,
                100L, d.maxLeaseMs(), 100L, d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /**
     * 短空闲时限配置：空闲断连用例用。
     *
     * @param port 监听端口
     * @return 服务器配置
     */
    static ServerConfig fastIdleConfig(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), 800L, d.defaultLeaseMs(),
                d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /**
     * 构造并启动服务器。
     *
     * @param config 服务器配置
     * @return 已启动的服务器，用例负责关停
     */
    static OpenLatchServer start(ServerConfig config) {
        OpenLatchServer server = new OpenLatchServer(config);
        server.start();
        return server;
    }
}
