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

package io.github.lamspace.openlatch.core.command;

/**
 * 条件变量操作词（signal 家族，v9）。AWAIT 不在词表内——await 是
 * {@link AcquireCommand} 携带 {@code condition} 分量的折叠形态（释放半程
 * 经复制、登记半程 Leader 本地），三操作均为纯 Leader 本地裁决：
 * 等待集与等待队列之间的位次搬运或摘除，MUST NOT 产生复制日志条目
 * （"signal 是事件不是状态"，判例 topic 零日志豁免的类目化）。
 */
public enum ConditionOp {

    /** 唤醒一人：该条件到达序集队首搬入等待队列尾。 */
    SIGNAL,

    /** 唤醒全员：按到达序全部搬运入等待队列。 */
    SIGNAL_ALL,

    /** 撤登：客户端本地超时/中断路径按 (会话, 请求) 摘除等待集登记。 */
    LEAVE
}
