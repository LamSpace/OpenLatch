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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.protocol.StatusCode;

/**
 * {@link OpenLatch} 注解的锁切面（详设 §8.3，M4 定案见 design D7）。
 *
 * <p><b>职责与流程</b>：{@code @Around("@annotation(openLatch)")} 拦截标注
 * 方法，依次执行——①SpEL 求值锁键（解析结果按"方法 + 表达式"缓存）；
 * ②经客户端异步内核获取锁（{@code waitTime} 三分支映射为
 * {@link AcquireSpec} 的 {@code waitMs}，总超时计时由客户端内部承担，
 * 本切面仅以本地兜底时限等待结果）；③执行业务方法；④finally 语义释放：
 * 释放失败分类处理——锁已丢失（{@code INVALID_TOKEN}/{@code NOT_HELD}/
 * {@code SESSION_EXPIRED}/断连）静默跳过并记 debug 日志（丢失事件已由
 * 客户端锁丢失通道通知，design D3；会话过期仅计入释放侧，获取侧原样
 * 传播，见使用约束），其余失败若业务已成功则抛出，业务已抛异常则仅记
 * 日志、不掩盖业务结果；释放等待被中断时恢复中断标志并放行、不上抛
 * （锁状态未确认，最坏随租约到期兜底）。
 *
 * <p><b>与事务的交互</b>：{@code @Order(0)} 使本切面排序先于事务通知
 * （默认 {@code LOWEST_PRECEDENCE}），即锁在事务外层——获取先于事务开启、
 * 释放晚于提交。该顺序有自动化测试锁定。
 *
 * <p><b>线程模型</b>：切面单例、无可变共享状态（表达式缓存为
 * {@link ConcurrentHashMap}，评测上下文每次调用新建）；业务方法可在任意
 * 线程执行，锁归属线程 = 业务执行线程（{@code Thread.currentThread()
 * .threadId()}）。
 *
 * <p><b>使用约束</b>：基于 Spring AOP，自调用不拦截；获取超时或被拒一律抛
 * {@link LockAcquisitionTimeoutException} 且业务不执行，其余获取错误
 * （会话过期、服务不可达等）原样传播。
 */
@Aspect
@Order(0)
public class OpenLatchAspect {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(OpenLatchAspect.class);

    /** 本地兜底等待在客户端自身超时/请求超时之外追加的裕量（毫秒）。 */
    private static final long BLOCK_SLACK_MS = 1_000;

    /** 锁操作入口客户端。 */
    private final OpenLatchClient client;

    /** SpEL 解析器（线程安全，仅产出不带编译器的解释模式表达式）。 */
    private final ExpressionParser parser = new SpelExpressionParser();

    /** 参数名发现器：依赖 {@code -parameters} 或调试变量名解析形参名。 */
    private final ParameterNameDiscoverer nameDiscoverer = new DefaultParameterNameDiscoverer();

    /** "方法 + 表达式" → 已解析表达式的缓存（解析成本只付一次）。 */
    private final ConcurrentMap<SpelCacheKey, Expression> expressionCache = new ConcurrentHashMap<>();

    /**
     * 构造切面。
     *
     * @param client 已装配的客户端，锁获取/释放经由其异步内核
     */
    public OpenLatchAspect(OpenLatchClient client) {
        this.client = client;
    }

    /**
     * 环绕通知：求值 key → 获取锁 → 执行业务 → 守卫式释放。
     *
     * @param pjp       连接点（业务方法）
     * @param openLatch 方法上的注解实例
     * @return 业务方法返回值
     * @throws Throwable 业务方法自身异常原样传播；获取超时/被拒抛
     *                   {@link LockAcquisitionTimeoutException}；key 求值非法、
     *                   等待被中断抛 {@link OpenLatchException}；
     *                   业务成功但释放真实失败时抛 {@link OpenLatchException}
     */
    @Around("@annotation(openLatch)")
    public Object around(ProceedingJoinPoint pjp, OpenLatch openLatch) throws Throwable {
        Method method = ((MethodSignature) pjp.getSignature()).getMethod();
        String key = evaluateKey(openLatch.key(), method, pjp.getArgs());
        long threadId = Thread.currentThread().threadId();
        LockGrant grant = acquire(key, openLatch, threadId);
        Throwable businessError = null;
        Object result = null;
        try {
            result = pjp.proceed();
        } catch (Throwable t) {
            businessError = t;
        }
        releaseAfterBusiness(key, grant, threadId, businessError);
        if (businessError != null) {
            throw businessError;
        }
        return result;
    }

    /**
     * 获取锁：注解 {@code waitTime} 映射为 {@link AcquireSpec#waitMs()}
     * （{@code <0 → -1} 排队、{@code =0 → 0} 立即、{@code >0 →} 毫秒数），
     * 阻塞等待由本地兜底时限约束（客户端总超时先触发，兜底仅是双保险）。
     *
     * @param key       锁键
     * @param openLatch 注解实例
     * @param threadId  归属线程标识
     * @return 授予结果
     */
    private LockGrant acquire(String key, OpenLatch openLatch, long threadId) {
        long waitTime = openLatch.waitTime();
        long leaseMs = openLatch.leaseTime() > 0
                ? openLatch.timeUnit().toMillis(openLatch.leaseTime()) : 0;
        long specWaitMs;
        long boundMs;
        if (waitTime < 0) {
            specWaitMs = -1;
            boundMs = client.config().defaultWaitTimeout().toMillis()
                    + client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS;
        } else if (waitTime == 0) {
            specWaitMs = 0;
            boundMs = client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS;
        } else {
            specWaitMs = openLatch.timeUnit().toMillis(waitTime);
            boundMs = specWaitMs + client.config().requestTimeout().toMillis()
                    + BLOCK_SLACK_MS;
        }
        AcquireSpec spec = new AcquireSpec(key, openLatch.type(), threadId, leaseMs, specWaitMs);
        try {
            return client.acquireAsync(spec).get(boundMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenLatchException("acquire of '" + key + "' interrupted", e);
        } catch (TimeoutException e) {
            throw new LockAcquisitionTimeoutException(
                    "acquire of '" + key + "' timed out after " + boundMs + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof LockAcquisitionTimeoutException late) {
                throw late;
            }
            if (cause instanceof OpenLatchException ole
                    && ole.status() == StatusCode.DENIED) {
                throw new LockAcquisitionTimeoutException(
                        "acquire of '" + key + "' denied (lock held, waitTime=0)");
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("acquire of '" + key + "' failed", cause);
        }
    }

    /**
     * 业务执行后的守卫式释放（design D3）：锁已丢失（失效状态码
     * {@code INVALID_TOKEN}/{@code NOT_HELD}/{@code SESSION_EXPIRED} 或断连
     * 不可达）时静默跳过——丢失事件已经客户端 {@code LockLostListener}
     * 通道通知，服务端以租约到期兜底；其余真实失败只在业务成功时抛出，
     * 业务已失败时记日志让位业务异常；等待释放被中断时恢复中断标志并记
     * debug 日志、不抛出（锁状态未确认，最坏随租约到期消失）。
     *
     * @param key           锁键
     * @param grant         获取结果
     * @param threadId      归属线程标识
     * @param businessError 业务异常（成功时为 {@code null}）
     */
    private void releaseAfterBusiness(String key, LockGrant grant, long threadId,
                                      Throwable businessError) {
        Duration requestTimeout = client.config().requestTimeout();
        try {
            client.releaseAsync(key, grant.leaseToken(), threadId)
                    .get(requestTimeout.toMillis() + BLOCK_SLACK_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("release of '{}' interrupted after business failure", key);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (isLostLockFailure(cause)) {
                log.debug("release of '{}' skipped: lock already lost ({})", key,
                        cause.toString());
                return;
            }
            reportReleaseFailure(key, cause, businessError);
        } catch (TimeoutException e) {
            reportReleaseFailure(key, e, businessError);
        }
    }

    /**
     * 判定"锁已丢失"类释放失败：租约凭证失效、未持有、会话过期或断连
     * 导致的不可达。这些失败服务端语义上锁已不存在或必然随租约到期
     * 消失，无需上报。注意 {@code SESSION_EXPIRED} 仅计入释放侧丢失路径，
     * 获取侧的会话过期不经本判定、原样传播（见类注释使用约束）。
     *
     * @param cause 释放异常
     * @return 属于丢失路径返回 {@code true}
     */
    private static boolean isLostLockFailure(Throwable cause) {
        if (cause instanceof ServerUnavailableException) {
            return true;
        }
        return cause instanceof OpenLatchException ole
                && (ole.status() == StatusCode.INVALID_TOKEN
                        || ole.status() == StatusCode.NOT_HELD
                        || ole.status() == StatusCode.SESSION_EXPIRED);
    }

    /**
     * 非丢失类释放失败的处置：业务成功时以 {@link OpenLatchException}
     * 抛出（调用方须知道锁状态未确认）；业务已抛异常时记 error 日志并
     * 让位业务异常，不掩盖。
     *
     * @param key           锁键
     * @param cause         释放失败原因
     * @param businessError 业务异常
     */
    private void reportReleaseFailure(String key, Throwable cause, Throwable businessError) {
        if (businessError == null) {
            throw new OpenLatchException("release of '" + key + "' failed", cause);
        }
        log.error("release of '{}' failed after business exception (lock released "
                + "by lease expiry at worst): {}", key, cause.toString());
    }

    /**
     * SpEL 求值锁键：解析结果缓存复用，评测上下文每调用新建
     * （{@link StandardEvaluationContext} 非线程安全）。
     *
     * @param expression 注解中的 SpEL 表达式
     * @param method     被拦截方法（形参名解析来源）
     * @param args       实参数组
     * @return 非空字符串锁键
     */
    private String evaluateKey(String expression, Method method, Object[] args) {
        Expression parsed;
        try {
            parsed = expressionCache.computeIfAbsent(new SpelCacheKey(method, expression),
                    k -> parser.parseExpression(k.expression()));
        } catch (ParseException e) {
            throw new OpenLatchException("invalid @OpenLatch key expression '"
                    + expression + "'", e);
        }
        StandardEvaluationContext context = new StandardEvaluationContext();
        String[] names = nameDiscoverer.getParameterNames(method);
        if (names != null) {
            for (int i = 0; i < names.length && i < args.length; i++) {
                context.setVariable(names[i], args[i]);
            }
        }
        for (int i = 0; i < args.length; i++) {
            context.setVariable("p" + i, args[i]);
            context.setVariable("a" + i, args[i]);
        }
        Object value;
        try {
            value = parsed.getValue(context);
        } catch (EvaluationException e) {
            throw new OpenLatchException("failed to evaluate @OpenLatch key '" + expression
                    + "' on " + method.getName() + " (is the project compiled with "
                    + "-parameters?)", e);
        }
        String key = value == null ? null : value.toString();
        if (key == null || key.isEmpty()) {
            throw new OpenLatchException("@OpenLatch key expression '" + expression
                    + "' must evaluate to a non-empty string, got: " + value);
        }
        return key;
    }

    /**
     * 表达式缓存条目数（包内可见，仅测试观测缓存复用用）。
     *
     * @return 当前缓存条目数
     */
    int cachedExpressionCount() {
        return expressionCache.size();
    }

    /**
     * 表达式缓存键：方法 + 原始表达式串（同方法多注解位不会出现，
     * 不同方法同表达式独立编译）。
     *
     * @param method     被拦截方法
     * @param expression SpEL 原文
     */
    private record SpelCacheKey(Method method, String expression) {
    }
}
