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
