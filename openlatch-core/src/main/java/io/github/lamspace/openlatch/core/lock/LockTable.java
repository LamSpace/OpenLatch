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

    private final ConcurrentHashMap<String, LockEntry> entries = new ConcurrentHashMap<>();

    public LockEntry computeIfAbsent(String key, Function<String, LockEntry> factory) {
        return entries.computeIfAbsent(key, factory);
    }

    public LockEntry get(String key) {
        return entries.get(key);
    }

    /** 仅当当前值仍为 {@code entry} 时移除，返回是否移除成功。 */
    public boolean remove(String key, LockEntry entry) {
        return entries.remove(key, entry);
    }

    /** 弱一致遍历，供到期扫描使用。 */
    public Collection<LockEntry> values() {
        return entries.values();
    }
}
