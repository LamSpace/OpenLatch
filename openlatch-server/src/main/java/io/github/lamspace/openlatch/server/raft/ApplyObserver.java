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

import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;

/**
 * 条目应用观察接口：{@link LockStateMachine} 在每条复制条目应用完成后
 * （含本节点作为 Follower 的回放）回调消费方，承载两类 Leader 侧职责——
 * 在途请求回执完成（{@code ReplicationGateway}）与等待队列/到期驱动联动
 * （详设 §4.5 应用结果收口，design D3/D9）。
 *
 * <p><b>回调线程</b>：状态机应用线程（单线程、条目间无并发，design D10）。
 * 实现 MUST NOT 阻塞（会停滞整条复制流水线）；涉及客户端 I/O 的动作
 * 须立即转投到目标连接所属 EventLoop（{@code channel.eventLoop().execute}）。
 *
 * <p><b>角色语义</b>：回调不区分 Leader/Follower——回执完成在任何副本都有
 * 意义（条目由本节点提交、由对端 Leader 确认的情形同样经回放抵达）；
 * 队列登记、唤醒通知等 Leader-only 副作用由实现自行以
 * {@link #onLeaderChanged(boolean)} 维护的角色标志裁决。
 */
public interface ApplyObserver {

    /**
     * 一条条目已应用完成。
     *
     * @param entry  已应用的复制条目（携带类型、seq、时刻与载荷）
     * @param result 应用回执（非复制状态）
     */
    void onApplied(RaftLogEntry entry, ApplyResult result);

    /**
     * Leadership 变更通知。
     *
     * <p>失去 Leadership 时实现须让全部在途回执以可重试错误完成（§8 切换窗口
     * 快速失败）；重新当选时清空上一任期遗留的等待队列（任期作用域 FIFO，
     * §4.4/design D9）。
     *
     * @param leader 本节点当前是否为 Leader
     */
    default void onLeaderChanged(boolean leader) {
    }
}
