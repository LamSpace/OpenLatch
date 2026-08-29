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

package io.github.lamspace.openlatch.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/** 测试用手工时钟：推进租约无需 sleep。 */
final class MutableClock implements Clock {
    /** 当前时刻（毫秒）。初值 1_000_000 为任意非零基点：用例只经 {@link #advance} 相对推进，绝对值不承载语义。 */
    private long now = 1_000_000L;

    @Override
    public long nowMs() {
        return now;
    }

    /**
     * 向前推进时钟。
     *
     * @param ms 推进的毫秒数
     */
    void advance(long ms) {
        now += ms;
    }

    /**
     * 直接设置当前时刻。
     *
     * @param ms 目标时刻（毫秒）
     */
    void set(long ms) {
        now = ms;
    }
}

/** 测试用记录型监听器：记录通知序列。 */
final class RecordingListener implements CoreEventListener {

    /** 一次 {@code notifyHead} 回调的入参快照（数据载体）。 */
    record Event(long sessionId, long requestId, String key) {}

    /** 按通知到达顺序追加的事件流水。 */
    private final List<Event> events = new ArrayList<>();

    @Override
    public void notifyHead(long sessionId, long requestId, String key) {
        events.add(new Event(sessionId, requestId, key));
    }

    /**
     * 已记录的通知序列快照。
     *
     * @return 事件列表（副本）
     */
    List<Event> events() {
        return List.copyOf(events);
    }

    /**
     * 已记录的通知数。
     *
     * @return 事件数量
     */
    int count() {
        return events.size();
    }

    /**
     * 最近一条通知。
     *
     * @return 最后一条事件
     */
    Event last() {
        return events.get(events.size() - 1);
    }

    /** 清空记录。 */
    void clear() {
        events.clear();
    }
}

/** 测试用队列监听器：按 sessionId 路由到各线程的队列，供并发测试阻塞等待通知。 */
final class QueueingListener implements CoreEventListener {

    /** 一次 {@code notifyHead} 回调的入参快照（数据载体，投递到对应会话队列）。 */
    record Event(long sessionId, long requestId, String key) {}

    /** sessionId → 该会话登记队列的登记表：{@link #register} 写入，{@code notifyHead} 按会话查投。 */
    private final ConcurrentHashMap<Long, BlockingQueue<Event>> queues = new ConcurrentHashMap<>();

    /**
     * 为会话登记通知队列。
     *
     * @param sessionId 会话
     * @return 该会话的通知队列，供阻塞等待
     */
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
