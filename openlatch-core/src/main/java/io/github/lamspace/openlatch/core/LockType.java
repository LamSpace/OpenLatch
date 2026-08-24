package io.github.lamspace.openlatch.core;

/**
 * 锁类型，与协议层 {@code LockType} 一一对应。core 不依赖 protocol，
 * 故在此定义独立枚举，由 server 层做映射。
 */
public enum LockType {
    /** 可重入互斥（默认）。 */
    REENTRANT,
    /** 不可重入互斥。 */
    SIMPLE,
    /** 读锁。 */
    READ,
    /** 写锁。 */
    WRITE
}
