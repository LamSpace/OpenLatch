# 03 · Client SDK

## Lifecycle and construction

`OpenLatchClient` is `AutoCloseable`: one instance = one connection (+ a reroute lane in
cluster mode) with its own EventLoop, watchdog and reconnect logic. It is **thread-safe and
meant to be shared process-wide**; `close()` (or try-with-resources) on shutdown.

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("node1:9410", "node2:9410", "node3:9410")   // cluster: all seeds; single node may use .address("host:port")
        .requestTimeout(Duration.ofSeconds(5))             // per-request response timeout
        .defaultWaitTimeout(Duration.ofSeconds(30))        // lock() total budget
        .connectTimeout(Duration.ofSeconds(3))
        .reconnectInitialBackoff(Duration.ofMillis(200))
        .reconnectMaxBackoff(Duration.ofSeconds(10))
        .workerThreads(1)
        .build();
client.connectAsync().join();   // optional eager connect; other calls connect lazily anyway
```

### Security & token (optional, default off)

| builder | meaning |
|---|---|
| `tlsEnabled(true)` | TLS toward the business port |
| `tlsTrustStore(path)` | PEM CA trust anchor |
| `tlsClientCert / tlsClientKey` | mTLS client cert/key (paired) |
| `authToken(token)` | business token carried in HELLO (required when server auth is on) |

Once configured, TLS and the token apply to first connect, reconnects and seed-discovery
probes — every connection construction point. Details: [06 Security](06-security.md).

## Choosing a lock type

| Need | Use |
|---|---|
| Reentrant mutex | `newReentrantLock(key)` |
| Lightest mutex (same thread is NOT reentrant — re-acquire queues against itself until lease expiry) | `newSimpleLock(key)` |
| Reader/writer split | `newReadWriteLock(key)` → `.readLock()` / `.writeLock()` |
| Strict arrival-order mutex (explicit fairness) | `newFairLock(key)` |
| N-concurrency resource pool | `newSemaphore(key, permits)` |
| One-shot gate ("wait for N things") | `newCountDownLatch(key, total)` |

Keys live in one global namespace (same key = same lock); prefer `domain:entity:id` naming.

## Synchronous usage (default recommendation)

```java
OLock lock = client.newReentrantLock("order:123");
lock.lock();                              // unbounded wait? no — defaultWaitTimeout caps it
try {
    // critical section
} finally {
    lock.unlock();                        // silent when already lost — the callback is the truth
}

if (lock.tryLock(2, TimeUnit.SECONDS)) {  // bounded acquisition
    try { ... } finally { lock.unlock(); }
}
```

- Timeouts surface as `LockAcquisitionTimeoutException` (wait budget) or
  `OpenLatchTimeoutException` (per-request silence); `InterruptedException` passes through.
- `lock.isHeldByCurrentThread()` and `lock.key()` are local reads — no network.

## Lock-lost handling: mandatory when writing critical state

```java
client.addLockLostListener((key, cause) -> { /* global: abort/degrade the commit, alert */ });
lock.onLockLost((key, cause) -> { /* per-lock handler */ });
```

Triggers: lease expiry, session closure on disconnect, failover rollback. Callbacks run on a
dedicated single thread — **never block inside them** (hand off to your executor).

## Semaphore and latch

```java
OSemaphore sem = client.newSemaphore("pool:http", 8);
sem.acquire();                    // or tryAcquire(n, timeout, unit)
try { work(); } finally { sem.release(); }   // release without holding throws IllegalMonitorStateException

OCountDownLatch latch = client.newCountDownLatch("boot:gates", 3);
latch.init();                     // fixes total (idempotent: repeating with the same total is legal)
latch.countDown();                // → remaining count
latch.await(60, TimeUnit.SECONDS);   // wait for zero; one-shot — the entry then lives until node restart
```

Barrier note: entries are not reclaimed when participants disperse — namespace keys per
round (e.g. `deploy:2026-09-12`).

## Atomic variables (the OAtomicLong family, v4)

```java
OAtomicLong seq = client.newAtomicLong("ids:order");          // starts at 0
OAtomicLong one = client.newAtomicLong("ids:user", 1);        // non-zero initial claim: first create starts at 1
long v = seq.incrementAndGet();                               // one server-side round-trip (not a local CAS loop)

// ABA-free read-modify-write (the recommended form):
OAtomicLong.Stamped cur = seq.getStamped();
boolean ok = seq.compareAndSetStamped(cur.value(), cur.version(), cur.value() + 10);

OAtomicBoolean ready = client.newAtomicBoolean("boot:ready");
if (ready.compareAndSet(false, true)) { doInit(); }           // cross-process first-writer-wins

OAtomicInteger hits = client.newAtomicInteger("counter:hits");
hits.accumulateAndGet(5, Integer::sum);                       // client-side stamped CAS loop (bounded retries)
```

A key's **form (long/integer/boolean) is settled by its first creating request** and the
three forms are mutually exclusive — the same one-type-per-key discipline already enforced
for locks/semaphores/latches across families.

Semantic boundaries (details in [01 Concepts §7](01-concepts.md)):

1. one round-trip per operation; writes and reads are ordered through the quorum
   (linearizable) — the cost is RTT;
2. the value is **not bound to a session** — no client death rolls it back (the opposite of
   lock/semaphore lock-loss semantics); no lease, no watchdog, no `LockLostListener`;
3. when a timeout is thrown (`OpenLatchTimeoutException`) a write **may already be applied**:
   the SDK retried with the same sequence under dedup protection; on the abandoned case
   (session switch) re-check via `getStamped()` before deciding anything;
4. the JDK-shaped `compareAndSet` has ABA risk; use `compareAndSetStamped` when value plus
   history must both be judged;
5. entries are permanent: no delete API — namespace keys per round/tenant and watch the
   key cardinality;
6. the initial value in `newAtomic*(key, initial)` is an **assertion**, not an assignment:
   a mismatching claim on an existing entry makes the handle's first operation throw
   `OpenLatchException` (same rule as the latch total).

## Cyclic barrier (OBarrier, v5)

```java
OBarrier gate = client.newBarrier("phase:ingest", 3);          // creator handle: fixes parties=3
gate.await();                                                  // blocks until its generation trips
boolean met = gate.await(10, TimeUnit.SECONDS);                // timed: expiry = you leave AND break -> false

OBarrier worker = client.newBarrier("phase:ingest");           // join-only handle (no parties claim)
OBarrier withAction = client.newBarrier("phase:ingest", 3, () -> flushBuffers());
// whoever completes the generation runs flushBuffers() inside their own await() call;
// nobody else is released until the action reports done.

OBarrier any = client.newBarrier("phase:ingest", 3);
any.breakBarrier();                                            // break the current generation (idempotent; no reset)
if (any.isBroken()) { ... }                                    // handle-local last-seen verdict (no network)
```

Semantic boundaries (details in [01 Core concepts §8](01-concepts.md)):

1. meeting, action release and breaking are majority-replicated decisions — each
   arrival/report costs a round trip;
2. **leaving breaks the generation** (stronger than the JDK): any arrived party's
   timeout/interruption/death/explicit break instantly breaks its generation and
   all its waiters throw `OBrokenBarrierException`;
3. **breakage is generation-local, not sticky**: next arrivals open a fresh
   generation and meet normally; there is deliberately **no `reset()`**, and
   `isBroken()` is a handle-local last-seen reading, not a live cross-process query;
4. the action runs in the last arriver's process; if it throws, that await fails
   with the action's exception and the generation breaks for everyone;
5. `await(timeout)` expiry breaks the barrier for the whole generation — over flaky
   networks prefer `await()` with external supervision, or raise the budget;
6. an in-flight arrival is never replayed across a session switch (arrivals are
   stateful) — that await throws `OpenLatchException` while the old generation has
   already broken via session cleanup; awaits hold no lease and emit no renewals.

## Async usage

```java
client.acquireAsync(new AcquireSpec(...))
      .thenAccept(grant -> { ... });     // ⚠️ completes on the network thread: keep it non-blocking
client.releaseAsync(key, leaseToken, threadId, permits);   // credentials from the grant
lock.lockAsync();
lock.tryLockAsync(2, TimeUnit.SECONDS);
```

One rule: **no blocking in chained callbacks** (futures complete on the network thread);
lock-lost callbacks follow the same rule on their dedicated thread.

## Client metrics

Inject a host `MeterRegistry` (Micrometer, compile-optional dependency) to export the four
client metrics (requests, duration, reconnects, locks-lost) — see [08](08-observability.md).

## Pitfall quick-reference

| Symptom | Cause & fix |
|---|---|
| Second same-thread `lock()` on a SIMPLE lock hangs to timeout | by design — use REENTRANT or remove the reentry path |
| Read→write under `OReadWriteLock` self-deadlocks | no upgrade special case — release first, or use a reentrant lock |
| Lock-lost callback mid-critical-section | renewal lost the race (GC/pause) — abort the commit; lengthen lease or shorten the section |
| `unlock()` returns fine but the lock was gone | intended: the callback is the only truth |
| Sporadic `NOT_LEADER` | normal reroute signal; persistent → [09](09-troubleshooting.md) |

## Next

Spring without boilerplate → [04 Spring Boot Starter](04-spring-boot-starter.md)
