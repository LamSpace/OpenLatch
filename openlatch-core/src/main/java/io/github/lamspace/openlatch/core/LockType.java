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
 *
 * <p><b>兼容性</b>：{@link #READ} 之间相互兼容（可多读者共存）；
 * {@link #READ} 与 {@link #SIMPLE}、{@link #REENTRANT}、{@link #WRITE}、
 * {@link #FAIR} 互斥；后四者相互之间亦互斥（同一时刻至多一个写侧持有者）。
 *
 * <p><b>重入归属</b>：归属由 {@code (sessionId, threadId)} 唯一确定。
 * {@link #SIMPLE} 不可重入（同归属重复获取将排队或拒绝）；
 * {@link #REENTRANT}、{@link #WRITE} 与 {@link #FAIR} 可重入；
 * {@link #READ} 的同归属重复获取按读侧计数重入。
 *
 * <p><b>同 key 同类型约定</b>（与 Redisson 约定一致）：同一 key 应始终
 * 使用一致的锁类型。条目的可重入性由首次请求定型（{@code SIMPLE} 为
 * 不可重入，其余为可重入），建条目后不再变化。
 */
public enum LockType {
    /** 可重入互斥（默认）：同归属可重复获取，持有计数逐层递增、逐层释放。 */
    REENTRANT,
    /** 不可重入互斥：同归属在持有期间重复获取将排队或拒绝。 */
    SIMPLE,
    /** 读锁：读者间共享，与任何写侧互斥；整 key 共用一个租约凭证。 */
    READ,
    /** 写锁：与任何持有者互斥，可重入。 */
    WRITE,
    /**
     * 显式公平承诺互斥：语义与 {@link #REENTRANT} 逐项等价
     * （同族别名、互通互认），作为公平承诺的 API 标识；授予顺序等于排队
     * 顺序的保证由 FIFO 队列本体承载。
     */
    FAIR,
    /**
     * 许可门闸：非锁家族类型，仅作请求定型判别——携带此
     * 类型的命令由门面分派至 {@code SemaphoreEntry}，不参与
     * 上述互斥矩阵。
     */
    SEMAPHORE,
    /**
     * 倒计数屏障：非锁家族类型，仅作 key 定型判别——屏障
     * 操作经门面独立入口（{@code countDown}/{@code latchAwait}）进入
     * {@code LatchEntry}，不经获取/释放/续租命令通道。
     */
    LATCH,
    /**
     * 原子 long 形态：非锁家族类型，仅作 key 定型与操作判别——
     * 原子操作经门面独立入口（{@code atomicOp}）进入 {@code AtomicEntry}，
     * 不经获取/释放/续租命令通道；ACQUIRE 携带本类型属请求形状错误，
     * 入口即拒。
     */
    ATOMIC_LONG,
    /** 原子 int 形态：值落 int32 域（溢出 wrap），其余语义与 {@link #ATOMIC_LONG} 同构。 */
    ATOMIC_INTEGER,
    /** 原子 boolean 形态：值域 {0,1}（0=false、1=true），其余语义与 {@link #ATOMIC_LONG} 同构。 */
    ATOMIC_BOOLEAN,
    /**
     * 循环屏障：非锁家族类型，仅作 key 定型判别——屏障操作经门面
     * 独立入口（{@code barrierAwait}/{@code barrierLeave}/
     * {@code barrierActionDone}）进入 {@code BarrierEntry}，不经获取/
     * 释放/续租命令通道。
     */
    BARRIER,
    /**
     * 有值引用形态（枚举序与协议 {@code LOCK_TYPE_ATOMIC_REFERENCE} 数值
     * 对齐）：非锁家族类型，仅作 key 定型与操作判别——引用操作经门面
     * 独立入口（{@code atomicRefOp}，命令为 {@code AtomicRefOpCommand}）
     * 进入 {@code AtomicRefEntry}，不经获取/释放/续租命令通道；与三标量
     * 形态同族互斥（同 key 跨形态以类型不匹配拒绝）。值域为不透明字节
     * （null 与空字节串为两个可区分的合法值），无 ADD 面；载荷尺寸钳制
     * 属接入层，条目侧不复核。ACQUIRE 携带本类型属请求形状错误，入口即拒。
     */
    ATOMIC_REFERENCE,
    /**
     * 有界队列形态（枚举序与协议 {@code LOCK_TYPE_QUEUE} 数值 12 对齐）：
     * 非锁家族类型，仅作 key 定型与操作判别——队列操作经门面独立入口
     * （{@code queueOp}，命令为 {@code QueueOpCommand}）进入
     * {@code QueueEntry}，不经获取/释放/续租命令通道；出队按到达序
     * （FIFO）。元素为不透明字节且不可为 null（与有值引用形态的 null
     * 语义刻意不同），容量由首次写入定型，元素绑定 key 不绑定会话。
     * ACQUIRE 携带本类型属请求形状错误，入口即拒。
     */
    QUEUE,
    /**
     * 延时队列形态（枚举序与协议 {@code LOCK_TYPE_DELAY_QUEUE} 数值 13
     * 对齐）：与 {@link #QUEUE} 同族互斥（同 key 跨形态以类型不匹配拒绝）。
     * 出队按最早到期序、同到期时刻内保持到达序（相对 JDK
     * {@code DelayQueue} 的语义增强，契约显式声明）；元素携带应用点
     * 折算的绝对到期时刻，未到期仅不可见、不消失（到期不触发回收）。
     * ACQUIRE 携带本类型属请求形状错误，入口即拒。
     */
    DELAY_QUEUE,
    /**
     * 相位器家族（枚举序与协议 {@code LOCK_TYPE_PHASER} 数值 14 对齐）：
     * 非锁家族类型，仅作 key 定型判别——动态注册/到场/离会的会合原语，
     * 操作经门面独立入口（{@code phaserOp}，命令为 {@code PhaserOpCommand}）
     * 进入 {@code PhaserEntry}，不经获取/释放/续租命令通道；形态由通道
     * 隐含、无子形态判别（判例 {@link #BARRIER}——线路请求不携本值，
     * ACQUIRE/其余车道携带属请求形状错误，入口即拒）。参与者配额按会话
     * 记账，会话死亡隐式摘除且已到场事实不撤销（与屏障"死亡即破障"
     * 刻意分轨，见 {@code PhaserEntry} 契约）。
     */
    PHASER
}
