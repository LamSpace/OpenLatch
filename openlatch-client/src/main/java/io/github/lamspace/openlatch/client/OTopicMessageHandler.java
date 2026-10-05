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
 * 广播消息处理器（{@link OTopic#subscribe(OTopicMessageHandler)} 的回调面）。
 *
 * <p><b>线程与序承诺</b>：回调在 SDK dispatcher 线程执行（绝不占用网络
 * EventLoop），<b>单订阅内串行</b>——对齐 JDK {@code Flow.Subscriber.onNext}
 * 的不重入承诺；不同订阅（含同 key 的不同进程）间无相对序。
 *
 * <p><b>异常纪律</b>：回调抛出的任何异常被 SDK 吞并记录日志，不中断后续
 * 交付、不影响连接与会话（对齐 {@code Flow} 对 onError 之外的宽容语义）。
 * 慢回调的后果是本地二级缓冲溢出丢弃（drop-newest，计入
 * {@link OTopicSubscription#droppedCount()}）——耗时处理请自行转交业务线程池。
 */
@FunctionalInterface
public interface OTopicMessageHandler {

    /**
     * 交付一条广播消息（至多一次：可能丢、可能因跨换主重试而双投）。
     *
     * @param message 不可变消息读数
     */
    void onMessage(OTopicMessage message);
}
