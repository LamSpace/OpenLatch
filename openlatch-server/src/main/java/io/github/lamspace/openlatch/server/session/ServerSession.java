package io.github.lamspace.openlatch.server.session;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Channel 绑定的会话簿记：握手状态、sessionId、inflight 计数（设计说明书 §5.1）。
 * 实例经 {@link #KEY} 存于 Channel 属性；握手成功后另行登记到
 * {@link ServerSessionRegistry} 供通知桥反查。
 */
public final class ServerSession {

    public static final AttributeKey<ServerSession> KEY = AttributeKey.valueOf("openlatch.session");

    private final Channel channel;
    private final AtomicInteger inflight = new AtomicInteger();
    private volatile long sessionId;
    private volatile boolean handshaken;
    private volatile boolean closed;

    public ServerSession(Channel channel) {
        this.channel = channel;
    }

    public Channel channel() {
        return channel;
    }

    public long sessionId() {
        return sessionId;
    }

    public boolean isHandshaken() {
        return handshaken;
    }

    /** 握手成功时调用：激活会话。仅由单 IO 线程调用，无需同步。 */
    public void activate(long sessionId) {
        this.sessionId = sessionId;
        this.handshaken = true;
    }

    /** 幂等关闭标记：断连清理只执行一次。返回 true 表示本次调用是首次关闭。 */
    public boolean markClosed() {
        boolean was = closed;
        closed = true;
        return !was;
    }

    /**
     * inflight +1 并检查是否超过限额。超限返回 false（调用方回 {@code OVERLOADED}
     * 且不计入在途）。
     */
    public boolean tryBeginRequest(int maxInflight) {
        int current = inflight.incrementAndGet();
        if (current > maxInflight) {
            inflight.decrementAndGet();
            return false;
        }
        return true;
    }

    /** 响应完成后递减在途计数。 */
    public void endRequest() {
        inflight.decrementAndGet();
    }

    public int inflight() {
        return inflight.get();
    }
}
