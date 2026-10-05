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

package io.github.lamspace.openlatch.client;

/**
 * 订阅句柄（v8）——一次 {@link OTopic#subscribe(OTopicMessageHandler)} 的
 * 观察与生命周期入口。
 *
 * <p>换主后 SDK 自动重订阅对本句柄透明：句柄引用与语义存续，服务端路由键
 * （{@code subscription_id}）在内部重映射；因此 {@link #droppedCount()} 的
 * term 基线随重订阅重置（跨窗缺口不累计——换主窗丢量以"断档"而非"缺口"
 * 呈现，属至多一次契约面）。
 */
public interface OTopicSubscription {

    /**
     * 观察丢弃计数：当前 Leader 任期内<b>本订阅</b>的估计丢失条数 =
     * 服务端侧 drop-newest（经 {@code topicSeq} 同任期缺口推断）+ SDK 本地
     * 二级缓冲溢出计数。非精确审计（换 term 基线重置、跨 term 不含）；
     * 持续增长即"消费跟不上广播速率"的告警信号。
     *
     * @return 本任期累计丢弃条数
     */
    long droppedCount();

    /**
     * 本句柄是否为当前活跃订阅（同 key 再次 subscribe 替换后旧句柄转
     * {@code false}；显式 {@link #close()} 后为 {@code false}）。
     *
     * @return 活跃返回 true
     */
    boolean isActive();

    /**
     * 退订并停止交付（幂等）：服务端摘除登记、缓冲与去重槽；本地停止
     * 派发并丢弃未派发的在途消息。与 {@link OTopic#unsubscribe()} 同效。
     */
    void close();
}
