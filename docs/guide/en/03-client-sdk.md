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

## Atomic reference (OAtomicReference, v6)

The payload form of the ATOMIC family: one opaque byte payload (capped at 4KB by default)
plus a version stamp, driven through get/set/stamped-CAS.

```java
OAtomicReference cfg = client.newAtomicReference("config:switch"); // initial null state
cfg.set("on".getBytes(StandardCharsets.UTF_8));                    // store a byte payload
cfg.setString("v2");                                               // String convenience (UTF-8)

// cross-process "config generation flip" (ABA-free):
OAtomicReference.Stamped cur = cfg.getStamped();
boolean ok = cfg.compareAndSetStamped(cur.value(), cur.version(), s("v3"));

// null and the empty byte string are two distinguishable values:
OAtomicReference flag = client.newAtomicReference("boot:ready");
if (flag.compareAndSet(null, s("init"))) { doInit(); }             // expect-null establishes the state
```

The form of a key is **settled by the first create request** (one of long/integer/boolean/
reference; mutually exclusive within the family) — a reference operation on a scalar key or
a scalar operation on a reference key is rejected with `INVALID_REQUEST`, zero perturbation.

Semantic boundaries (know these; details in [01 core concepts §7.1](01-concepts.md)):

1. payloads are **opaque**: the server stores and compares bytes and never deserializes —
   object encoding is the application's responsibility;
2. **overflow has zero effect**: a payload above the server's `max-value-bytes` (default
   4KB) throws `OpenLatchException` (rejection semantics); the SDK neither truncates nor
   retries, and the entry's value and version stay untouched; lowering the cap never
   retro-affects stored values (existing larger payloads remain readable);
3. same rules as the scalar atomics: one round-trip per operation, value unbound from the
   session, the timeout indeterminate window with same-sequence retry, ABA eliminated only
   by `*Stamped`, entry and payload never reclaimed;
4. **no arithmetic**: no `increment`/`accumulate`, and no `weakCompareAndSet` /
   `compareAndExchange` family (the JDK `AtomicReference` has no arithmetic either);
5. requires the **v6 handshake**: against a server whose capped protocol version is below
   6, a reference-form request fails with an explicit `OpenLatchException`
   (`INVALID_REQUEST`) — no retry, no silent downgrade.

## Bounded and delay queues (OBlockingQueue / ODelayQueue, v7)

Cross-process element shipping: a bounded FIFO queue and a delay queue (elements
carry an expiry instant). Elements are opaque bytes (subject to the same
per-element `max-value-bytes` clamp, 4KB default); capacity is formed by the
handle that first writes.

```java
OBlockingQueue jobs = client.newBlockingQueue("job:queue", 64);      // capacity claim 64
jobs.put(s("task-A"));                                                // blocking put (interruptible)
if (jobs.offer(s("task-B"))) { ... }                                 // immediate: false when full
byte[] item = jobs.take();                                           // blocking take (interruptible)
byte[] maybe = jobs.poll(2, TimeUnit.SECONDS);                       // budgeted take (local clock)
List<byte[]> batch = new ArrayList<>();
int n = jobs.drainTo(batch, 32);                                     // batched drain (RTT amortizer)
int depth = jobs.size();                                             // residency view (delay: incl. unexpired)

ODelayQueue alarm = client.newDelayQueue("alarm:queue", 16);
alarm.offerDelayed(s("remind me"), 5, TimeUnit.MINUTES);             // invisible to consumers until due
String due = alarm.takeAsString();                                   // earliest expiry first, ties by arrival
```

Semantic boundaries (details in [01 §9](01-concepts.md)):

1. **Elements belong to the key, not the session**: producer death never swallows
   them (stronger than the JDK's same-process heap); elements survive until
   consumed and nothing reclaims them server-side — a forgotten key is permanent
   residency (see the console's queue observations);
2. **The two "fulls" split**: element-full → `offer` false / `put` parks;
   waiter-queue-full → `OpenLatchException` (`OVERLOADED`, the lock depth-guard
   mapping). Server-side parking has no expiry; a client timeout/interruption is
   terminal locally while the server-side waiter is retired by events and the
   timeout sweep — meanwhile its queue slot still holds the position (an
   abandoned waiter never blocks others' progress semantics, only their ranks);
3. **Per-session dedup slots make resends safe**: `put` never double-inserts,
   `take`/`drainTo` replays deliver **the same bytes**; a mid-flight session
   switch aborts (exception) — re-check with `size()`/`peek()`, never blindly
   revalue;
4. **Elements are never null** (JDK `BlockingQueue` rejectNull precedent — the
   deliberate opposite of `OAtomicReference`; a zero-length array is a legal
   element); no `iterator`/`contains`/`remove(Object)` surface;
5. **Delay form**: `offerDelayed(e, delay, unit)` is explicitly named (JDK
   DelayQueue's `offer(e, timeout, unit)` delay-injection signature collides
   in shape with the wait-budget meaning elsewhere); absolute expiry is folded
   at the apply point and replicated (leader changes cannot re-judge); wake
   precision is the server ready tick (`ready-tick-ms`, default 200ms) while
   correctness never depends on it;
6. Requires a **v7 handshake** — older servers answer `QUEUE_OP` with an
   explicit `OpenLatchException` (`INVALID_REQUEST`), no retry, no downgrade;
   `drainTo`'s actual count is bounded by the server's `max-drain-bytes` budget
   and is authoritative in the return value.

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

## Broadcast pub/sub (`OTopic`, v8)

A cross-process broadcast channel: one key, one channel; each message reaches
all subscribers registered at that moment. Message bodies are opaque bytes
(per-message `max-value-bytes` clamp, default 4KB).

```java
OTopic news = client.newTopic("evt:orders");

news.publish(s("order paid"));                                // sync: accepted (NOT delivered)
long seq = news.publish("accepted".getBytes(StandardCharsets.UTF_8));
news.publishAsync(s("fire and forget"));                      // async single shot, no auto resend

OTopicSubscription watch = news.subscribe(m -> {
    handle(m.payload(), m.topicSeq(), m.publisherSessionId()); // ascending seq per subscription
});
long lost = watch.droppedCount();                              // loss estimate (see 2 below)
watch.close();                                                 // == news.unsubscribe() (idempotent)
```

Semantic boundaries (details in [01 Concepts §10](01-concepts.md)):

1. **At-most-once**: a successful `publish` means accepted-and-enqueued for
   fan-out, never received. Disconnects, buffer overflow and the leader-change
   window lose messages with no server-side retransmission. Use
   `OBlockingQueue` when delivery must survive;
2. **Weak backpressure = drop-newest, two buffer tiers**: the server-side
   per-subscription buffer (`max-subscription-buffer`, default 256) drops the
   newest message and counts it when full; the SDK's local tier does the same.
   Slow subscribers never backpressure publishers and are never disconnected
   (deliberately unlike JDK `SubmissionPublisher`'s overflow-close — closing
   would kill the session and release its locks). Losses surface only via
   `droppedCount()` (same-term `topic_seq` gap inference + local overflow,
   baseline reset across terms);
3. **Ordering**: `topic_seq` ascends strictly within one subscription during
   one Leader term; no global order across publishers/subscriptions; after a
   leader change seqs restart and the gap is never replayed — the SDK
   re-subscribes automatically and delivery resumes;
4. **Retry dedup is same-Leader only**: same-`op_seq` resends hit the dedup
   slot and never double-fan-out; retries across a leader change (including
   manual ones) **may double-deliver** — consumer idempotence is required;
5. **Listener threading**: callbacks are serialized per subscription (the JDK
   `Flow.Subscriber.onNext` no-reentrancy promise) on an SDK dispatcher
   thread, never on the network EventLoop; handler exceptions are swallowed
   and logged without breaking delivery. Hand off heavy work to your own
   executor;
6. **Subscriptions bind to the session**: subscriber process death
   unsubscribes (the deliberate inverse of the queue's "death never swallows
   elements" — queues hold resident data, topics hold delivery events).
   Long-lived subscribers should `unsubscribe()`; registrations have a
   standing registry + buffer cost;
7. Requires a **v8 handshake** (upgrade servers first, then clients):
   v≤7 sessions sending `TOPIC_OP` get an `INVALID_REQUEST` message-level
   rejection without disconnect. Message bodies are mandatory and non-null
   (queue-element precedent — the inverse of the reference form's null), with
   zero length as a legal empty message;
8. **Colliding keys**: subscribing/publishing on a key occupied by another
   family is ingress-rejected (`INVALID_REQUEST`, best-effort, no race
   guarantee) — "one key, one form" is an application contract for topics.

## Condition variables (`OCondition`, v9)

The wait/notify channel paired with an `OLock`, the counterpart of JDK
`Condition`. Condition identity is (lock key, condition name): handles are
stateless, need no closing, and same-name handles from different processes
bind the same server-side wait set. The standard idiom (the guard loop is the
caller's obligation):

```java
OLock lock = client.newReentrantLock("job:dispatch");
OCondition ready = lock.newCondition("ready");          // named addressing, re-creatable

lock.lock();
try {
    while (!hasWork()) {        // always wrap await in a predicate check — spurious wake-ups are allowed
        ready.await();          // parks with a full release; returns holding the lock, 1 reentrancy level
    }
    takeWork();
} finally {
    lock.unlock();
}

lock.lock();                    // producer: signal requires holding this lock
try {
    enqueueWork();
    ready.signal();             // wakes the head waiter (signalAll carries everyone)
} finally {
    lock.unlock();
}
```

Semantic boundaries (details in [01 Concepts §11](01-concepts.md)):

1. **The guard loop is the caller's obligation**: spurious wake-ups are
   allowed and never promised away (the JDK contract) — wake sources include
   head-timeout sweep promotions, predicates whose signal was lost in a
   leader-change re-registration window, and LEAVE/SIGNAL race convergence.
   A return is not proof the predicate holds; a bare `await()` is a defect;
2. **Returns holding the lock, and the 1-level reentrancy arithmetic**:
   `await()`/`await(timeout, unit)` — woken, timed out or interrupted alike —
   return (or throw) only after re-acquiring the lock: a `false` from
   `await(timeout, unit)` still means you hold it (safely re-check the
   predicate), and interruption throws `InterruptedException` only after the
   lock is re-held (JDK fidelity). The N reentrancy levels held before the
   await are zeroed in one step; on return the count starts at 1 — write the
   unlocking for "holds 1 level", and the extra N-1 `unlock()` calls would
   throw `IllegalMonitorStateException`. When timeout and wake-up settle
   simultaneously the return value reports whichever settled first
   (best-effort — the predicate, not the boolean, is the truth);
3. **Two-layer permission exceptions**: calling `signal`/`signalAll` from a
   thread that does not hold the lock throws `IllegalMonitorStateException`
   from two sources with one shape — the local pre-check (not held: zero
   requests sent) and the authoritative server re-check (ownership already
   lost, e.g. signaling after lock loss; line code `NOT_HELD`). **await
   permission is checked locally only**: the server never verifies that an
   await registrant holds the lock (required for re-registration across
   leader changes); ghosts from misuse are bounded by the merged guardrail
   and reclaimed on three paths (session death / LEAVE / leader change) —
   an explicit downgrade surface. Signaling an empty set, an unknown name or
   a missing key is a no-op returning normally (JDK-aligned);
4. **Cost model**: a folded await's full cycle costs at least 2 network
   round-trips (park acceptance + post-wake resend; a resend not granted
   immediately keeps rotating the wait–notify–resend loop, each rotation
   adding one more). `signal`/`signalAll` are 1 round-trip with an immediate
   reply. While parked, the server holds just one registration — no
   dedicated connection resources;
5. **Holder death never signals for you (sleep forever with nobody
   signaling)**: lease-expiry/process-death sweeps wake the **wait-queue
   entrants only**; condition waiters are untouched, and waiters themselves
   hold no lease and have no renewal duty. **Production code should prefer
   `await(timeout, unit)` as self-rescue** — an unbounded `await()` in a
   topology where nobody signals simply sleeps forever;
6. **Leader-change layering: waiting is a promise, signal is an event**:
   waiters **re-register automatically** with the acquisition-lane migration
   (same request id, idempotent, invisible to the application; wake-ups
   arrive normally on the new Leader); signals emitted inside the
   leader-change window are never replayed or compensated (a bounded
   window). The same rule topics follow: the promise/registration side
   survives via replay and re-registration, the event side (signals,
   messages) is lost once lost;
7. **Support surface and what is not provided**: only the REENTRANT/FAIR/
   SIMPLE exclusive forms host conditions; calling `newCondition` on a lock
   handle from `OReadWriteLock` throws `UnsupportedOperationException` (local
   verdict, zero requests). **No `awaitNanos`/`awaitUntil`/
   `awaitUninterruptibly`, and no async counterparts.** FAIR ordering:
   carried waiters join the FIFO at carry time — being signaled does not
   prioritize you over elders already queued;
8. **Merged guardrail and version gate**: condition waiters and the wait
   queue share `max-queue-depth-per-key` under one "waiters on this key"
   count — **v9 adds zero configuration keys**; over the limit an await
   fails with `OVERLOADED` (the thread then does NOT hold the lock —
   lock-lost/retry conventions apply). Requires a **v9 handshake** (upgrade
   servers first, then clients): v≤8 sessions sending `CONDITION_OP` or an
   ACQUIRE carrying the `condition` field get an `INVALID_REQUEST`
   message-level rejection without disconnect.

## Phaser (`OPhaser`, v10)

The generalized multi-party phase rendezvous, counterpart to JDK `Phaser`.
Handles are stateless and need no close; handles on the same key (including
across processes) bind one server-side ledger. Worker-loop idiom:

```java
OPhaser ph = client.newPhaser("pipeline");   // construction is network-free
ph.register();                                // this participant (returns current phase)
// ...produce...
long arrivalPhase = ph.arriveAndAwaitAdvance(); // arrive and wait for this phase's trip
// returns once the phase is (or is about to be) complete; phases advance monotonically
ph.arriveAndDeregister();                     // depart: removes one of THIS session's quotas
```

Bystanders (no participation needed):

```java
long seen = ph.getPhase();                     // one QUERY round trip (advisory read)
ph.awaitAdvanceInterruptibly(seen, 5, TimeUnit.SECONDS); // wait until phase advances beyond seen
```

Semantic boundaries (see [01 Core Concepts §12](01-concepts.md)):

1. **Quotas are session-attributed; departure only removes your own** —
   `arriveAndDeregister` on a quota-less session throws `OpenLatchException`
   (`INVALID_REQUEST`); a dead session's outstanding quotas are removed
   implicitly (the obligation may trip on the spot — no stall) while
   **counted arrivals are never rolled back**;
2. **Quotas persist across phases; no one-shot typying** — unlike Barrier's
   parties assertion, `register` is always legal, `bulkRegister(n)` simply
   adds; when everyone departs the ledger **idles** (phase kept, quota 0) and
   later registration revives it; **no termination surface** (no
   `isTerminated`/`forceTerminated`; deliberately inverted from JDK);
3. **No `onAdvance` hook** — discriminate "last arrival of the phase" from
   the returned arrival phase (optionally cross-checking
   `getArrivedParties()`) and run the action locally; **no ordering versus
   other wake-ups** is promised (the barrier's two-phase action is
   deliberately not adopted);
4. **Return values and width** — phases are `long` (JDK `int` wraps at 2^31);
   `awaitAdvance(long)` takes the phase **you have seen** and returns the
   strictly-greater current phase at resolution;
5. **Cost model** — every call is at least one RTT (mutations/reads 1; the
   `arriveAndAwaitAdvance` journey = arrival 1 + post-wake reissue 1); the
   wait uses chunked re-sends carrying keep-alive semantics — a lost wake
   (e.g. leader change) is settled by the next re-send immediately:
   **no loss window** (contrast the condition's uncompensated signal window;
   here the predicate lives in the replicated ledger, self-heal is
   structural). High-frequency `getPhase()` is high-frequency network —
   throttle at the application;
6. **Timeout discipline** — `awaitAdvance(long)` is bounded by the wait
   budget (default 30 s, `OpenLatchTimeoutException`); loop to re-enter for
   JDK's unbounded wait. `awaitAdvanceInterruptibly(phase, timeout, unit)`
   throws `TimeoutException` after best-effort CANCEL (lost cancels are
   reclaimed by the depth guardrail and three-way cleanup); interruption
   follows the same path preserving the interrupt flag;
7. **Not provided** — parent/child tiering (no `parent` ctor), termination,
   `bulkArriveAndDeregister` (loop `arriveAndDeregister` instead); reads are
   Leader-local zero-log and **not linearized** — the deliberate divergence
   from v4's GET-commits precedent;
8. **Guardrails and version gate** — `max-parties-per-phaser` (default 1024,
   [1, 65536]) caps per-key registrations (overflow `OVERLOADED`, existing
   quotas untouched); suspended waits share `max-queue-depth-per-key` (pure
   `awaitAdvance` overflow `OVERLOADED`; `arriveAndAwaitAdvance`'s wait half
   always registers). Requires a v10 handshake (server first): v≤9 sessions
   sending `PHASER_OP` get an `INVALID_REQUEST` message-level rejection
   without disconnect.

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
