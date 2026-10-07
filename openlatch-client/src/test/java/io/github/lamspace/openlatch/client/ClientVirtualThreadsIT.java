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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.server.OpenLatchServer;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 客户端虚拟线程亲和端到端（单机服务端）：OTopic 扇出下平台线程数恒定绊线、
 * 万级虚拟线程等待者规模见证、钉扎零断言、归属矩阵与中断语义对齐。
 *
 * <p><b>线程计数口径</b>：JDK 21+ 起 {@link ThreadMXBean#getThreadCount()}
 * 只计平台线程（虚拟线程不进入该计数），故"订阅扇出而平台计数不随实体
 * 线性增长"可直接断言；派发线程若回归平台形态，本绊线即红。
 */
@Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
class ClientVirtualThreadsIT {

    /**
     * 扇出形态：500 订阅摊 500 个独立 key（每 key 恰一订阅）。判据——
     * 服务端 {@code TopicRegistry} 按 {@code (session, key)} 唯一登记（幂等覆盖），
     * 同一会话对同一 key 的多次订阅只存续一条路由；跨会话扇出需多客户端，
     * 而本绊线度量的是单客户端进程的资源形态，故取"一 key 一订阅"。每 key
     * 订阅数 1 ≤ {@code DEFAULT_MAX_SUBSCRIBERS_PER_KEY=64}（{@code ClientTestServers.config}
     * 走 11 参 {@code ServerConfig} 构造回落默认），不触默认限额。
     */
    private static final int TOTAL_SUBSCRIPTIONS = 500;

    /** 每 key 发布消息数；每订阅应恰收该数（5 ≤ 缓冲默认 256，零丢弃）。 */
    private static final int MESSAGES_PER_KEY = 5;

    /**
     * 平台线程增量常数区间上限。判据——派发线程虚拟形态下增量仅为虚拟线程
     * 调度器载体线程爬坡与 JUnit/GC 噪声（十位数），100 上界与改动前实测
     * 红值（约 +500）间距一个数量级，既不误报也不放过形态回归。
     */
    private static final int PLATFORM_THREAD_DELTA_CEILING = 100;

    /** 规模见证总等待者数：1 万虚拟线程。 */
    private static final int SCALE_WAITERS = 10_000;

    /**
     * 规模见证 key 摊开数：100（每 key 恰 100 等待者 ≤ 默认
     * {@code maxQueueDepthPerKey=4096}，实测见 evidence/red-tripwire-fanout.md
     * 服务器启动行）。存在性论证：同规模平台线程形态在本机不可启动
     * （1 万 × ~8MB 默认栈 ≫ 堆外内存预算），虚拟线程形态挂起等待者仅 KB 量级/者。
     */
    private static final int SCALE_KEYS = 100;

    /** 规模见证分批启动粒度：每 512 者暂停一次，瞬时在途不触每连接 1024 上限。 */
    private static final int SCALE_RAMP_BATCH = 512;

    /** 规模见证分批暂停（毫秒）。 */
    private static final long SCALE_RAMP_PAUSE_MS = 10;

    /** 规模见证单等待者 join 上限（毫秒）。 */
    private static final long SCALE_JOIN_TIMEOUT_MS = 60_000;

    /** JFR 钉扎事件名。 */
    private static final String PINNED_EVENT = "jdk.VirtualThreadPinned";

    /** JFR 虚拟线程启动事件名（观测窗有效性证据源）。 */
    private static final String VT_START_EVENT = "jdk.VirtualThreadStart";

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 发布端客户端。 */
    private OpenLatchClient publisher;
    /** 订阅端客户端（全部 500 订阅挂在此单连接上）。 */
    private OpenLatchClient subscriber;
    /** 平台线程计数入口。 */
    private ThreadMXBean threadMXBean;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        publisher = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        subscriber = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        publisher.connectAsync().get(5, java.util.concurrent.TimeUnit.SECONDS);
        subscriber.connectAsync().get(5, java.util.concurrent.TimeUnit.SECONDS);
        threadMXBean = ManagementFactory.getThreadMXBean();
    }

    @AfterEach
    void tearDown() {
        publisher.close();
        subscriber.close();
        server.stop();
    }

    /**
     * OTopic 扇出平台线程数绊线（"OTopic 扇出下平台线程数恒定"场景）：
     * 500 订阅（一 key 一订阅，共 500 key）建立并全部收齐交付后，平台线程
     * 计数相对订阅建立前基线的增量必须落在常数区间——派发线程为虚拟线程时
     * 成立；若任何改动使每订阅重新消耗一条平台线程，增量约 +500，本断言
     * 即红。同时钉住交付正确性（每订阅恰收、串行不重入、序号升序、零丢弃）。
     *
     * @throws Exception 订阅/发布链路异常
     */
    @Test
    void topicFanoutKeepsPlatformThreadCountConstant() throws Exception {
        int baseline = threadMXBean.getThreadCount();
        List<SubState> states = new ArrayList<>(TOTAL_SUBSCRIPTIONS);
        List<OTopicSubscription> subs = new ArrayList<>(TOTAL_SUBSCRIPTIONS);
        try {
            for (int k = 0; k < TOTAL_SUBSCRIPTIONS; k++) {
                SubState st = new SubState();
                states.add(st);
                subs.add(subscriber.newTopic("vt:topic:" + k)
                        .subscribe(m -> st.accept(m.topicSeq())));
            }
            for (int k = 0; k < TOTAL_SUBSCRIPTIONS; k++) {
                OTopic topic = publisher.newTopic("vt:topic:" + k);
                for (int i = 0; i < MESSAGES_PER_KEY; i++) {
                    topic.publish(("m" + i).getBytes(StandardCharsets.UTF_8));
                }
            }
            long expectedTotal = (long) TOTAL_SUBSCRIPTIONS * MESSAGES_PER_KEY;
            awaitUntil(() -> {
                long sum = 0;
                for (SubState st : states) {
                    sum += st.received.get();
                }
                return sum == expectedTotal;
            }, 60_000);

            int delta = threadMXBean.getThreadCount() - baseline;

            // 绊线主断言：平台线程增量恒定区间（改动前此处约 +500 即红）。
            assertThat(delta)
                    .as("500 订阅扇出下平台线程增量（派发线程虚拟形态承诺）")
                    .isLessThanOrEqualTo(PLATFORM_THREAD_DELTA_CEILING);
            // 交付正确性钉住（红先要求本组断言先绿，红因只允许来自主断言）。
            assertThat(states).allSatisfy(st -> {
                assertThat(st.received.get()).isEqualTo(MESSAGES_PER_KEY);
                assertThat(st.serialViolation).isFalse();
                assertThat(st.reentrantViolation).isFalse();
            });
            assertThat(subs).allSatisfy(s -> assertThat(s.droppedCount()).isZero());
        } finally {
            for (OTopicSubscription s : subs) {
                s.close();
            }
        }
    }

    /**
     * 万级虚拟线程等待者规模见证（"万级虚拟线程等待者闭环通过"场景）：
     * 1 万条虚拟线程跨 100 key 各执行一次 {@code lock()/unlock()} 单周期。
     * 存在性论证——同规模平台线程形态不可启动（1 万 × 默认栈内存 ≫ 本机预算），
     * 本用例仅虚拟线程形态可在 90s 预算内完成，即"等待者规模不再按线程资源
     * 线性消耗"的可执行证明。分批启动保证瞬时在途不触每连接 1024 默认上限；
     * 等待全程无续租流量（等待不持租约，既有承诺在虚拟形态下依旧成立）。
     *
     * @throws Exception 启动/回收异常
     */
    @Test
    void tenThousandVirtualThreadWaitersCompleteUnderContention() throws Exception {
        long startMs = System.currentTimeMillis();
        AtomicInteger done = new AtomicInteger();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        List<Thread> workers = new ArrayList<>(SCALE_WAITERS);
        for (int i = 0; i < SCALE_WAITERS; i++) {
            String key = "vt:scale:" + (i % SCALE_KEYS);
            Thread vt = Thread.ofVirtual().unstarted(() -> {
                try {
                    OLock lock = subscriber.newReentrantLock(key);
                    lock.lock();
                    lock.unlock();
                    done.incrementAndGet();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                }
            });
            workers.add(vt);
            vt.start();
            if ((i + 1) % SCALE_RAMP_BATCH == 0) {
                Thread.sleep(SCALE_RAMP_PAUSE_MS);
            }
        }
        for (Thread t : workers) {
            t.join(SCALE_JOIN_TIMEOUT_MS);
        }
        assertThat(firstError.get()).isNull();
        assertThat(done.get()).isEqualTo(SCALE_WAITERS);
        // 规模负载后功能健全性：新的立即式获取不受损。
        OLock probe = subscriber.newReentrantLock("vt:scale:probe");
        assertThat(probe.tryLock()).isTrue();
        probe.unlock();
        // 预算守卫（任务 3.1 的 <90s 判据，超出即红并触发 D4 退让条款评估）。
        assertThat(System.currentTimeMillis() - startMs).isLessThan(90_000L);
    }

    /**
     * 钉扎零断言（"钉扎事件零断言"场景）：JFR 观测窗内以虚拟线程执行锁争用
     * 等待、信号量阻塞获取、屏障合拢、倒计数等待、条件限时等待、队列阻塞
     * 消费与原子 CAS 循环，钉扎事件必须为 0；同窗虚拟线程启动事件非零以证明
     * 观测在收（防"零事件源于观测失效"的假绿）。
     *
     * <p><b>预热轮的必要性</b>：JDK 上虚拟线程执行首次类初始化（{@code <clinit>}
     * 在栈上）期间不可卸载，会如实产生 {@code jdk.VirtualThreadPinned}——这是
     * JVM 级冷启动伪影而非 SDK 桥接属性。故观测窗前先以同一负载预热一轮
     * （键前缀 {@code warm}）完成类初始化，窗口轮（前缀 {@code run}）只测稳态；
     * 若稳态仍有钉扎事件，断言失败并附事件栈原文供定位。
     *
     * @throws Exception JFR/线程操作异常
     */
    @Test
    void noVirtualThreadPinningAcrossBridgePaths() throws Exception {
        // 预热轮：类加载/初始化在观测窗外完成（失败同样视为红，非静默跳过）。
        exerciseBridgePathsOnVirtualThreads("warm");

        Path dump = Files.createTempFile("vt-pinning-", ".jfr");
        Recording rec = new Recording();
        rec.enable(PINNED_EVENT).withoutThreshold();
        rec.enable(VT_START_EVENT);
        rec.setToDisk(true);
        rec.setDestination(dump);
        rec.start();
        long pinned;
        long started;
        try {
            exerciseBridgePathsOnVirtualThreads("run");
        } finally {
            rec.stop();
            rec.close();
        }
        StringBuilder pinDetail = new StringBuilder();
        try {
            List<RecordedEvent> events = RecordingFile.readAllEvents(dump);
            pinned = events.stream()
                    .filter(e -> PINNED_EVENT.equals(e.getEventType().getName())).count();
            started = events.stream()
                    .filter(e -> VT_START_EVENT.equals(e.getEventType().getName())).count();
            for (RecordedEvent e : events) {
                if (PINNED_EVENT.equals(e.getEventType().getName())) {
                    pinDetail.append("\n--- pinned event: ").append(e);
                }
            }
        } finally {
            Files.deleteIfExists(dump);
        }
        assertThat(started).as("观测窗内虚拟线程启动事件数（观测有效性证据）").isPositive();
        assertThat(pinned).as("钉扎事件数%s", pinDetail).isZero();
    }

    /**
     * 归属矩阵（"虚拟线程归属矩阵"场景）：虚拟线程重入两次后两次释放、
     * 他线程归属查询为假、释放清零后他者即刻可取（服务端重入计数仅见归零
     * 时的恰一次释放生效，未归零前他者不可得）、非持锁线程解锁拒绝——
     * 全部与平台线程形态判例同构。
     *
     * @throws Exception 线程编排异常
     */
    @Test
    void reentrantOwnershipMatrixOnVirtualThreads() throws Exception {
        OLock lock = subscriber.newReentrantLock("vt:own:lock");
        AtomicBoolean selfHeld = new AtomicBoolean();
        AtomicBoolean otherHeld = new AtomicBoolean(true);
        AtomicReference<Throwable> workerFail = new AtomicReference<>();
        CompletableFuture<Void> releaseGate = new CompletableFuture<>();
        Thread holder1 = Thread.ofVirtual().unstarted(() -> {
            try {
                lock.lock();
                lock.lock();
                selfHeld.set(lock.isHeldByCurrentThread());
                releaseGate.get(30, TimeUnit.SECONDS);
                lock.unlock();
                lock.unlock();
            } catch (Throwable t) {
                workerFail.set(t);
            }
        });
        holder1.start();
        awaitUntil(selfHeld::get, 10_000);
        Thread checker = Thread.ofVirtual().unstarted(
                () -> otherHeld.set(lock.isHeldByCurrentThread()));
        checker.start();
        checker.join(10_000);
        assertThat(workerFail.get()).isNull();
        assertThat(selfHeld).isTrue();
        assertThat(otherHeld).isFalse();

        releaseGate.complete(null);
        holder1.join(30_000);
        assertThat(workerFail.get()).isNull();

        // 计数归零释放生效：另一虚拟线程限时获取立即成功。
        AtomicBoolean acquired = new AtomicBoolean();
        Thread next = Thread.ofVirtual().unstarted(() -> {
            try {
                acquired.set(lock.tryLock(5, TimeUnit.SECONDS));
                if (acquired.get()) {
                    lock.unlock();
                }
            } catch (Throwable t) {
                workerFail.set(t);
            }
        });
        next.start();
        next.join(20_000);
        assertThat(workerFail.get()).isNull();
        assertThat(acquired).isTrue();

        // 非持锁线程解锁拒绝（本地归属判定先于任何释放请求）。
        AtomicReference<Throwable> illegal = new AtomicReference<>();
        Thread offender = Thread.ofVirtual().unstarted(() -> {
            try {
                lock.unlock();
            } catch (Throwable t) {
                illegal.set(t);
            }
        });
        offender.start();
        offender.join(10_000);
        assertThat(illegal.get()).isInstanceOf(IllegalMonitorStateException.class);
    }

    /**
     * 中断语义一致性（"虚拟线程中断与平台线程判例一致"场景）：锁限时等待、
     * 队列阻塞消费、信号量阻塞获取三类桥接，分别以平台线程与虚拟线程执行并
     * 在等待中被中断——两形态抛出异常类逐项相同（{@code InterruptedException}），
     * 且中断后资源可正常复用（等待项经补偿归还摘除，后续获取不受损）。
     *
     * @throws Exception 线程编排异常
     */
    @Test
    void interruptSemanticsIdenticalAcrossThreadFlavors() throws Exception {
        for (boolean virtual : new boolean[]{false, true}) {
            OLock holder = subscriber.newReentrantLock("vt:int:lock");
            holder.lock();
            // 体取即还：若中断窗竞速使等待者仍获授，锁不被遗留（断言只关心中断形态）。
            Throwable lockThrown = startAndInterrupt(virtual, () -> {
                holder.lock();
                holder.unlock();
            });
            holder.unlock();
            assertThat(lockThrown)
                    .as("锁等待中断异常形态(virtual=%s)", virtual)
                    .isInstanceOf(InterruptedException.class);

            OBlockingQueue queue = subscriber.newBlockingQueue("vt:int:q:" + (virtual ? "v" : "p"), 4);
            Throwable takeThrown = startAndInterrupt(virtual, queue::take);
            assertThat(takeThrown)
                    .as("队列 take 中断异常形态(virtual=%s)", virtual)
                    .isInstanceOf(InterruptedException.class);

            OSemaphore permits = subscriber.newSemaphore("vt:int:sem", 1);
            permits.acquire();
            // 取即还（同锁体理由）：中断形态断言不受竞速获授干扰。
            Throwable acqThrown = startAndInterrupt(virtual, () -> {
                permits.acquire();
                permits.release();
            });
            permits.release();
            assertThat(acqThrown)
                    .as("许可 acquire 中断异常形态(virtual=%s)", virtual)
                    .isInstanceOf(InterruptedException.class);
        }
        // 中断风暴后功能健全性：新等待者可正常获取三类资源。
        OLock probe = subscriber.newReentrantLock("vt:int:lock");
        assertThat(probe.tryLock(5, TimeUnit.SECONDS)).isTrue();
        probe.unlock();
        assertThat(subscriber.newSemaphore("vt:int:sem", 1).tryAcquire(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * 钉扎观测窗的桥接面工作负载：七个等待/桥接形态各以虚拟线程走一遍，
     * 任一环节异常即记入 {@code workerFail} 并在末尾断言为 null（保证钉扎
     * 断言的零值来自"负载完成且无钉扎"，而非"负载中断未跑全"）。
     *
     * @param phase 键前缀相位名（预热轮 {@code warm} / 观测轮 {@code run}），
     *              两轮互不共享服务端条目状态
     * @throws Exception 线程回收异常
     */
    private void exerciseBridgePathsOnVirtualThreads(String phase) throws Exception {
        AtomicReference<Throwable> workerFail = new AtomicReference<>();
        List<Thread> pool = new ArrayList<>();
        String p = "vt:pin:" + phase + ":";

        // 1) 锁争用等待：主线程持有，虚拟线程排队至释放后获授。
        OLock lock = subscriber.newReentrantLock(p + "lock");
        lock.lock();
        pool.add(startVirtual(() -> {
            try {
                lock.lock();
                lock.unlock();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));
        Thread.sleep(100);
        lock.unlock();

        // 2) 信号量阻塞获取：主线程占满许可，虚拟线程等待归还。
        OSemaphore sem = subscriber.newSemaphore(p + "sem", 1);
        sem.acquire();
        pool.add(startVirtual(() -> {
            try {
                sem.acquire();
                sem.release();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));
        Thread.sleep(100);
        sem.release();

        // 3) 屏障合拢：两个虚拟线程（不同客户端会话）同世代到场。两侧句柄
        // 均携一致 parties 定型主张（纯加入句柄对未定型 barrier 按契约拒绝，
        // 双主张形态消次序依赖；纯加入语义由 ClientBarrierIT 承载）。
        OBarrier barP = publisher.newBarrier(p + "bar", 2);
        OBarrier barS = subscriber.newBarrier(p + "bar", 2);
        pool.add(startVirtual(() -> {
            try {
                barP.await();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));
        pool.add(startVirtual(() -> {
            try {
                barS.await();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));

        // 4) 倒计数等待：虚拟线程 await，主线程放行。
        OCountDownLatch latch = subscriber.newCountDownLatch(p + "latch", 1);
        pool.add(startVirtual(() -> {
            try {
                latch.await();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));
        Thread.sleep(100);
        latch.countDown();

        // 5) 条件限时等待：持有者虚拟线程 await 超时返回（仍持锁）并空 signal 后释放。
        pool.add(startVirtual(() -> {
            try {
                OLock condLock = subscriber.newReentrantLock(p + "cond");
                condLock.lock();
                OCondition cond = condLock.newCondition("held");
                boolean signaled = cond.await(300, TimeUnit.MILLISECONDS);
                if (!signaled) {
                    cond.signal(); // 空等待集 signal：无操作合法（持有所在性满足）
                }
                condLock.unlock();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));

        // 6) 队列阻塞消费：虚拟线程 take，主线程投放。
        OBlockingQueue queue = subscriber.newBlockingQueue(p + "q", 8);
        pool.add(startVirtual(() -> {
            try {
                queue.take();
            } catch (Throwable t) {
                workerFail.compareAndSet(null, t);
            }
        }));
        Thread.sleep(100);
        assertThat(queue.offer("ping")).isTrue();

        // 7) 原子 CAS 循环：两条虚拟线程并发 accumulateAndGet 触发版本戳 CAS 重试。
        // 并发档位刻意保守：单调用 64 次 CAS 上限为契约内拒绝线（紧循环高争用会
        // 触发有界重试异常，属承诺而非缺陷），本工作负载意在覆盖重试路径而非压线。
        OAtomicLong counter = subscriber.newAtomicLong(p + "atom");
        for (int i = 0; i < 2; i++) {
            pool.add(startVirtual(() -> {
                try {
                    for (int j = 0; j < 20; j++) {
                        counter.accumulateAndGet(1L, Long::sum);
                    }
                } catch (Throwable t) {
                    workerFail.compareAndSet(null, t);
                }
            }));
        }

        for (Thread t : pool) {
            t.join(30_000);
        }
        assertThat(workerFail.get()).isNull();
        assertThat(counter.get()).isEqualTo(40L);
    }

    /**
     * 以指定线程形态启动阻塞体，驻留后中断，返回其抛出异常（未抛出为 {@code null}）。
     *
     * @param virtual   虚拟线程形态为 {@code true}，否则平台线程
     * @param blockingOp 预期阻塞的调用体
     * @return 阻塞体抛出物
     * @throws InterruptedException join 被中断
     */
    private Throwable startAndInterrupt(boolean virtual, BlockingOp blockingOp)
            throws InterruptedException {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Runnable body = () -> {
            try {
                blockingOp.run();
            } catch (Throwable t) {
                thrown.set(t);
            }
        };
        Thread worker = virtual ? Thread.ofVirtual().unstarted(body) : new Thread(body);
        worker.start();
        Thread.sleep(150);
        worker.interrupt();
        worker.join(10_000);
        return thrown.get();
    }

    /**
     * 启动一条虚拟线程执行体（钉扎工作负载的编排糖）。
     *
     * @param body 执行体
     * @return 已启动线程
     */
    private static Thread startVirtual(Runnable body) {
        Thread t = Thread.ofVirtual().unstarted(body);
        t.start();
        return t;
    }

    /**
     * 可抛受检异常的阻塞调用体。
     */
    @FunctionalInterface
    private interface BlockingOp {

        /**
         * 执行预期阻塞的调用。
         *
         * @throws Exception 任意调用异常（含 InterruptedException）
         */
        void run() throws Exception;
    }

    /**
     * 单订阅交付状态：恰收计数、串行重入甄别、序号升序校验。
     */
    private static final class SubState {

        /** 已收消息数。 */
        final AtomicInteger received = new AtomicInteger();
        /** 回调重入违例（同订阅并行进入）。 */
        volatile boolean reentrantViolation;
        /** 序号非严格升序违例。 */
        volatile boolean serialViolation;
        /** 串行门闩（回调内 CAS 占用，重入即违例）。 */
        private final AtomicBoolean inside = new AtomicBoolean();
        /** 上一条交付序号（0=未置基线）。 */
        private long lastSeq;

        /**
         * 回调入口：先做重入甄别与计数，再校验同订阅内升序。
         *
         * @param seq 交付序号
         */
        void accept(long seq) {
            if (!inside.compareAndSet(false, true)) {
                reentrantViolation = true;
                return;
            }
            try {
                if (lastSeq != 0 && seq <= lastSeq) {
                    serialViolation = true;
                }
                lastSeq = seq;
                received.incrementAndGet();
            } finally {
                inside.set(false);
            }
        }
    }

    /**
     * 轮询等待条件成立（同 ClientTopicIT 惯用法）。
     *
     * @param condition  条件
     * @param timeoutMs  超时（毫秒）
     * @throws InterruptedException 等待被中断
     */
    private static void awaitUntil(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("条件超时未达成");
    }
}
