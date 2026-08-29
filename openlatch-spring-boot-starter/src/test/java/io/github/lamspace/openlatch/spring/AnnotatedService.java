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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.lamspace.openlatch.client.LockType;

/**
 * starter 集成测试的被注解服务。探针状态集中于 {@link Probe}（经
 * {@link #probe()} 方法取回——CGLIB 代理的实例字段是未初始化的遮蔽拷贝，
 * 不能直接从代理读字段）。任何时刻并发进入数超过 1 即记违例，
 * 互斥/读写语义的最终断言都落在探针上。
 */
class AnnotatedService {

    /**
     * 并发探针：临界区进入计数、违例计数、非原子读-改-写计数与时刻记录。
     */
    static final class Probe {

        /** 当前在临界区内的线程数。 */
        final AtomicInteger activeInside = new AtomicInteger();
        /** 并发违例次数（应恒为 0，读并发用例除外）。 */
        final AtomicInteger violations = new AtomicInteger();
        /** 非原子读-改-写计数器：终值 == 执行次数当且仅当互斥成立。 */
        int plainCounter;
        /** 写者进入信号。 */
        final CountDownLatch writerEntered = new CountDownLatch(1);
        /** 写者离开时刻（纳秒）。 */
        volatile long writerExitNanos;
        /** 普通读者进入时刻（纳秒）。 */
        volatile long readerEntryNanos;

        /**
         * 复位与单调无关的计数（用例间隔离）。
         */
        void resetCounters() {
            violations.set(0);
            activeInside.set(0);
            plainCounter = 0;
        }
    }

    /** 探针实例（目标对象持有，测试经 probe() 取同一引用）。 */
    private final Probe probe = new Probe();

    /**
     * 取回探针。
     *
     * @return 探针
     */
    public Probe probe() {
        return probe;
    }

    /**
     * 进入临界区探针：并发数超 1 记违例。
     */
    private void enter() {
        if (probe.activeInside.incrementAndGet() > 1) {
            probe.violations.incrementAndGet();
        }
    }

    /**
     * 离开临界区探针。
     */
    private void exit() {
        probe.activeInside.decrementAndGet();
    }

    /**
     * 互斥临界区：非原子自增（读与写之间留窗口），锁失效必然丢失更新。
     *
     * @return 固定值
     * @throws InterruptedException 睡眠被打断
     */
    @OpenLatch(key = "'shared'")
    public String sharedCritical() throws InterruptedException {
        enter();
        int cur = probe.plainCounter;
        Thread.sleep(2);
        probe.plainCounter = cur + 1;
        exit();
        return "ok";
    }

    /**
     * SpEL 按参数取键 + 屏障：同键调用会被串行化（屏障等待超时即证明），
     * 异键调用可并发穿过屏障。
     *
     * @param id   锁键来源
     * @param gate 并发屏障
     * @return 锁键
     * @throws Exception 屏障超时/打断
     */
    @OpenLatch(key = "#id")
    public String keyedWithBarrier(String id, CyclicBarrier gate) throws Exception {
        enter();
        try {
            gate.await(2, TimeUnit.SECONDS);
        } finally {
            exit();
        }
        return id;
    }

    /**
     * 读锁临界区：双读者应能并发穿过屏障。
     *
     * @param gate 并发屏障
     * @return 固定值
     * @throws Exception 屏障异常
     */
    @OpenLatch(key = "'doc.read'", type = LockType.READ)
    public String readWithBarrier(CyclicBarrier gate) throws Exception {
        enter();
        try {
            gate.await(2, TimeUnit.SECONDS);
        } finally {
            exit();
        }
        return "read";
    }

    /**
     * 写锁临界区：持有 300ms 并广播进入/离开时刻。
     *
     * @return 固定值
     * @throws InterruptedException 睡眠被打断
     */
    @OpenLatch(key = "'doc.read'", type = LockType.WRITE)
    public String writeDoc() throws InterruptedException {
        enter();
        probe.writerEntered.countDown();
        Thread.sleep(300);
        probe.writerExitNanos = System.nanoTime();
        exit();
        return "write";
    }

    /**
     * 普通读临界区：记录进入时刻，用于与写者离开时刻比较。
     *
     * @return 固定值
     */
    @OpenLatch(key = "'doc.read'", type = LockType.READ)
    public String readDoc() {
        enter();
        probe.readerEntryNanos = System.nanoTime();
        exit();
        return "read";
    }

    /**
     * 外部持锁场景的注解方法（立即式）：开关关闭时应直接执行，
     * 开关开启且锁被外部持有时应抛超时异常。
     *
     * @return 固定值
     */
    @OpenLatch(key = "'held'", waitTime = 0)
    public String whileExternalHeld() {
        return "ran";
    }
}
