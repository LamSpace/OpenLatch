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

/**
 * 事件出口。core 不持有任何连接相关对象，仅通过此接口向外报告"该通知谁"；
 * server 层实现该接口并翻译为协议 {@code AWAIT_NOTIFY} 写回对应 Channel。
 * 回调在条目锁之外触发（见设计说明书 §4.9.5）。
 */
public interface CoreEventListener {
    /**
     * key 的队首等待者可以重试获取（对应协议 AWAIT_NOTIFY）。
     *
     * @param sessionId 等待者所属会话
     * @param requestId 原获取请求的请求 id
     * @param key       锁键
     */
    void notifyHead(long sessionId, long requestId, String key);
}
