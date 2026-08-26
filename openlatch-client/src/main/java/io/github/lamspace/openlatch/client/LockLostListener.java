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
 * 锁丢失监听器（详设 §6.1/§6.6）。
 *
 * <p>回调在客户端专用的单线程执行器上调用，<b>不得执行阻塞操作</b>；
 * 单个回调抛出的异常被客户端捕获并记录，不影响其他监听器。
 */
@FunctionalInterface
public interface LockLostListener {

    /**
     * 锁丢失通知。
     *
     * @param key   丢失的锁键
     * @param cause 丢失原因（续租失败、断连失锁等）
     */
    void onLockLost(String key, LockLostException cause);
}
