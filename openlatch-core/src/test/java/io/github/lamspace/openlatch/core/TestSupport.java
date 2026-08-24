package io.github.lamspace.openlatch.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/** 测试用手工时钟：推进租约无需 sleep。 */
final class MutableClock implements Clock {
    private long now = 1_000_000L;

    @Override
    public long nowMs() {
        return now;
    }

    void advance(long ms) {
        now += ms;
    }

    void set(long ms) {
        now = ms;
    }
}

/** 测试用记录型监听器：记录通知序列。 */
final class RecordingListener implements CoreEventListener {

    record Event(long sessionId, long requestId, String key) {}

    private final List<Event> events = new ArrayList<>();

    @Override
    public void notifyHead(long sessionId, long requestId, String key) {
        events.add(new Event(sessionId, requestId, key));
    }

    List<Event> events() {
        return List.copyOf(events);
    }

    int count() {
        return events.size();
    }

    Event last() {
        return events.get(events.size() - 1);
    }

    void clear() {
        events.clear();
    }
}

/** 测试用队列监听器：按 sessionId 路由到各线程的队列，供并发测试阻塞等待通知。 */
final class QueueingListener implements CoreEventListener {

    record Event(long sessionId, long requestId, String key) {}

    private final ConcurrentHashMap<Long, BlockingQueue<Event>> queues = new ConcurrentHashMap<>();

    BlockingQueue<Event> register(long sessionId) {
        BlockingQueue<Event> q = new LinkedBlockingQueue<>();
        queues.put(sessionId, q);
        return q;
    }

    @Override
    public void notifyHead(long sessionId, long requestId, String key) {
        BlockingQueue<Event> q = queues.get(sessionId);
        if (q != null) {
            q.add(new Event(sessionId, requestId, key));
        }
    }
}
