package io.github.lamspace.openlatch.server;

/**
 * 端到端测试的服务器夹具：一律绑定临时端口（0）避免端口竞争。
 */
final class TestServers {

    private TestServers() {
    }

    static ServerConfig config(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), d.idleTimeoutMs(), d.defaultLeaseMs(),
                d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /** 短租约快扫描配置：租约到期类端到端用例用。 */
    static ServerConfig fastExpiryConfig(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), d.idleTimeoutMs(), 200L,
                100L, d.maxLeaseMs(), 100L, d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /** 短空闲时限配置：空闲断连用例用。 */
    static ServerConfig fastIdleConfig(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), 800L, d.defaultLeaseMs(),
                d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    static OpenLatchServer start(ServerConfig config) {
        OpenLatchServer server = new OpenLatchServer(config);
        server.start();
        return server;
    }
}
