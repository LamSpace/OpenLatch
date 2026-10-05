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
 * 交付给订阅者的一条广播消息（不可变读数，v8）。
 *
 * <p>载荷为发布侧字节的原样副本（服务端不解释）；{@code topicSeq} 仅在
 * <b>同一 Leader 任期</b>内可比较（换主后重新起算，跨任期序列号无先后语义）。
 * 交付不承诺送达成功之外的任何回执——至多一次契约见 {@link OTopic}。
 */
public interface OTopicMessage {

    /**
     * 消息所属 topic 键。
     *
     * @return key
     */
    String key();

    /**
     * Leader 任期内的受理序号（本订阅视角单调升序；缺口即丢弃推断依据，
     * 见 {@link OTopicSubscription#droppedCount()}；换 term 重新起算）。
     *
     * @return {@code >= 1} 的序号
     */
    long topicSeq();

    /**
     * 发布方逻辑会话 id（0=服务端不可知）。仅观察用途——跨进程会话身份
     * 不构成本句柄的权限或归属语义。
     *
     * @return 发布会话 id
     */
    long publisherSessionId();

    /**
     * 服务端受理时刻（epoch 毫秒，受理节点时钟——与服务端租约时钟同口径
     * 的"受理节点"语义，多订阅者间不做同步承诺）。
     *
     * @return 受理时刻
     */
    long publishedAtMs();

    /**
     * 消息体字节（原样副本，零长度=合法空消息；每次回调返回独立副本，
     * 调用方可安全持有）。
     *
     * @return 载荷字节
     */
    byte[] payload();
}
