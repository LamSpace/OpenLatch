package io.github.lamspace.openlatch.core;

/**
 * 事件出口。core 不持有任何连接相关对象，仅通过此接口向外报告"该通知谁"；
 * server 层实现该接口并翻译为协议 {@code AWAIT_NOTIFY} 写回对应 Channel。
 * 回调在条目锁之外触发（见设计说明书 §4.9.5）。
 */
public interface CoreEventListener {
    /** key 的队首等待者可以重试获取（对应协议 AWAIT_NOTIFY）。 */
    void notifyHead(long sessionId, long requestId, String key);
}
