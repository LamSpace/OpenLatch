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

package io.github.lamspace.openlatch.core.lock;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * key → {@link LockEntry} 的映射与条目生命周期。用 {@link ConcurrentHashMap} 承载，
 * 条目创建用 {@link #computeIfAbsent}，销毁用条件移除 {@link #remove(String, LockEntry)}，
 * 避免移除/创建竞态（设计说明书 §4.9.1）。
 */
public final class LockTable {

    /** 构造空锁表。 */
    public LockTable() {
    }

    private final ConcurrentHashMap<String, LockEntry> entries = new ConcurrentHashMap<>();

    /**
     * 键不存在则创建条目并登记。
     *
     * @param key     锁键
     * @param factory 条目创建函数，仅在键不存在时调用
     * @return 现有或新建的条目
     */
    public LockEntry computeIfAbsent(String key, Function<String, LockEntry> factory) {
        return entries.computeIfAbsent(key, factory);
    }

    /**
     * 按锁键查找条目。
     *
     * @param key 锁键
     * @return 条目；不存在返回 {@code null}
     */
    public LockEntry get(String key) {
        return entries.get(key);
    }

    /**
     * 仅当当前值仍为 {@code entry} 时移除，返回是否移除成功。
     *
     * @param key   锁键
     * @param entry 期望的当前条目
     * @return 是否移除成功
     */
    public boolean remove(String key, LockEntry entry) {
        return entries.remove(key, entry);
    }

    /**
     * 弱一致遍历，供到期扫描使用。
     *
     * @return 全部条目的弱一致视图
     */
    public Collection<LockEntry> values() {
        return entries.values();
    }
}
