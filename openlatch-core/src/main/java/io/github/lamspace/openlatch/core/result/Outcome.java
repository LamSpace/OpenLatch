package io.github.lamspace.openlatch.core.result;

/**
 * 获取结果。相较设计说明书 §4.2 将 {@code REJECT_KEY} 细分为空/超长两值，
 * 使 server 层无需重校验即可映射到协议 {@code KEY_EMPTY} / {@code KEY_TOO_LONG}。
 */
public enum Outcome {
    GRANTED,
    QUEUED,
    DENIED,
    REJECT_KEY_EMPTY,
    REJECT_KEY_TOO_LONG,
    REJECT_QUEUE_FULL,
    REJECT_SESSION
}
