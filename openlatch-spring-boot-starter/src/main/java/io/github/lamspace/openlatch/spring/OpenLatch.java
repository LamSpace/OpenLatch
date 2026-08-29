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

package io.github.lamspace.openlatch.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import io.github.lamspace.openlatch.client.LockType;

/**
 * 声明式分布式锁（详设 §8.3，M4 定案：{@code type} 直接复用客户端公开枚举
 * {@link LockType}，不另造 {@code LockMode}，design D2）。
 *
 * <p><b>语义</b>：方法调用前按 {@link #key()} 的 SpEL 求值结果获取锁，方法
 * 返回（或抛出异常）后释放。获取方式由 {@link #waitTime()} 决定：
 * <ul>
 *   <li>{@code < 0}：排队获取，受客户端等待总超时（
 *       {@code openlatch.default-wait-timeout}，默认 30s）兜底；</li>
 *   <li>{@code = 0}：立即式尝试，锁被占直接判失败，不排队；</li>
 *   <li>{@code > 0}：限时等待 {@code waitTime(timeUnit)}，到时未授予判失败。</li>
 * </ul>
 * 获取超时或被拒一律抛
 * {@link io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException}
 * 且业务方法不执行；其余错误（如会话过期、服务不可达）原样抛出，
 * 等待被中断抛 {@link io.github.lamspace.openlatch.client.OpenLatchException}。
 *
 * <p><b>租约</b>：{@link #leaseTime()} 为 0 时使用服务端默认租约（30s）并由
 * 看门狗自动续租；大于 0 时按请求值授予（服务端钳制到配置区间），看门狗
 * 按实际生效租约续租。客户端存活期间不主动释放的锁由看门狗持续续租；
 * 续租中断（进程死亡、断连超时判定）时锁最迟在租约到期后被服务端回收。
 *
 * <p><b>使用约束</b>：
 * <ul>
 *   <li>SpEL 按参数名求值（{@code #paramName}）要求编译开启
 *       {@code -parameters}（starter 自身模块已开启，应用工程须自行开启）；</li>
 *   <li>基于 Spring AOP 代理：同类自调用（{@code this.method()}）不经过
 *       代理，注解不生效——与 {@code @Transactional} 同一限制；</li>
 *   <li>与 {@code @Transactional} 同标注时锁在事务外层（获取先于事务开启、
 *       释放晚于提交）；</li>
 *   <li>{@code SIMPLE} 类型不可重入：同线程持锁期间再次进入同 key 的注解
 *       方法会排队等待自身，直至租约到期（详设 §4.4 自锁警示）；</li>
 *   <li>锁可能在持有期间丢失（断连、租约失效），丢失经客户端
 *       {@link io.github.lamspace.openlatch.client.LockLostListener} 通道通知；
 *       切面释放时遇已丢失的锁静默跳过，不掩盖业务结果；</li>
 *   <li>Phase 1 不支持持读升级写 / 持写降级读特判；{@code READ}/{@code WRITE}
 *       的并发与排队行为遵循服务端严格 FIFO 语义。</li>
 * </ul>
 *
 * <p><b>线程模型</b>：注解方法可被任意线程并发调用；同 key 的互斥/共享/排队
 * 裁决全部在服务端完成，重入计数由服务端维护（跨代理层同线程嵌套获取
 * 计数递增、逐层释放）。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OpenLatch {

    /**
     * 锁键的 SpEL 表达式（详设 §8.3）。求值上下文注入方法形参
     * （{@code #参数名} 及 {@code #p0}/{@code #a0} 位置引用）；
     * 求值结果必须为非空字符串，否则抛
     * {@link io.github.lamspace.openlatch.client.OpenLatchException}。
     *
     * @return SpEL 表达式
     */
    String key();

    /**
     * 锁类型（客户端公开枚举，design D2）。
     *
     * @return 锁类型，默认可重入互斥
     */
    LockType type() default LockType.REENTRANT;

    /**
     * 等待时长：{@code < 0} 排队（总超时兜底）、{@code = 0} 立即式、
     * {@code > 0} 限时等待（单位由 {@link #timeUnit()} 决定）。
     *
     * @return 等待时长，默认 -1（排队）
     */
    long waitTime() default -1;

    /**
     * 请求租约时长（单位 {@link #timeUnit()}）。0 表示使用服务端默认租约
     * 并接受看门狗自动续租；大于 0 按请求值（服务端钳制）。
     *
     * @return 租约时长，默认 0
     */
    long leaseTime() default 0;

    /**
     * {@link #waitTime()} 与 {@link #leaseTime()} 的时间单位。
     *
     * @return 时间单位，默认秒
     */
    TimeUnit timeUnit() default TimeUnit.SECONDS;
}
