# 01 · Core Concepts

Six mechanisms explain everything OpenLatch does. After this chapter you should be able to
answer: when can my lock be lost? How does the client find out? Why is queuing fair?

## 1. Lease: every grant expires

The server stamps every grant with a **lease**: default 30s (clamped to a configured
1s–1h range; client-requested values are clamped into it). An unrenewed lease expires, the
lock is reclaimed by the expiry scanner and becomes grantable again.

**Corollary: no lock is held forever.** Partitions, hung processes and long GC pauses can all
mean "you think you still hold it" while the lease is actually gone. Before writing critical
state, ask: what happens if someone else takes the lock while I'm mid-critical-section?
That answer is your lock-lost handler.

## 2. Watchdog: renewal on your behalf

For each held lock the client renews in the background every `lease/3` (30s lease → 10s
cadence). Failed renewals retry; if failures persist past the lease deadline the lock is
reclaimed and lock loss fires.

- Annotation locks (`@OpenLatch`) and SDK locks are protected identically, including custom
  `leaseTime` — the lease is only the "unrenewed → reclaimed" yardstick, not a switch that
  turns protection off at expiry.

## 3. Wait–notify–resend: how fair queuing works

When acquisition fails you are not left hanging on the connection: the server enqueues you and
answers **`QUEUED` immediately**. When you reach the head and the lock frees, the server
**pushes a notification** and the client **resends the same request** to claim the grant. Benefits:

- the server holds no dangling responses — network jitter cannot corrupt queue state;
- only the head is notified — **no thundering herd**;
- the connection stays reusable while you wait.

A head waiter that never replies (process died) keeps its seat for at most one
**head-reply timeout** (default 5s) and is then skipped — abandoning a wait does not
proactively cancel the seat; this timeout reclaims it.

## 4. FIFO fairness: strict arrival order

Each key has one queue; grants follow **strict arrival order**, read-write included (readers
do not overtake writers). The `FAIR` type makes the promise explicit. This is **strict FIFO
within one leader term**: on leader change the queue resets to "re-queue against the new
leader" (positions may change — the cluster fairness boundary, see the consistency
declaration in [05](05-cluster-deployment.md)).

## 5. Lock loss: a required answer, not an option

Loss fires on **lease expiry** or **session closure** (disconnect, server-side cleanup).
Subscribe via `LockLostListener`:

```java
client.addLockLostListener((key, cause) -> {
    // global: fires for any lock of this client
    // correct move: abort/degrade the in-flight commit + alert
});

OLock lock = client.newReentrantLock("order:123");
lock.onLockLost((key, cause) -> { /* per-lock handler, same semantics */ });
lock.lock();
try {
    doCriticalWork();
} finally {
    lock.unlock();   // silent if already lost — the callback is the source of truth
}
```

The cluster adds a subtle path: **failover rollback**. A grant the old Leader confirmed but
never replicated to a quorum does not exist on the new Leader — renewal/unlock fail with
`INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED`, and the same handler must cope. Design every
acquisition as "may be invalidated by failover".

## 6. Timeout discipline: nothing blocks unboundedly

| Layer | Default | Semantics |
|---|---|---|
| `requestTimeout` | 5s | per-request response window → `OpenLatchTimeoutException` |
| `defaultWaitTimeout` | 30s | `lock()` total budget → `LockAcquisitionTimeoutException` |
| `tryLock(waitTime, unit)` | yours | bounded acquisition, boolean result |
| connect timeout | 3s | TCP + handshake |

Synchronous APIs never block forever; async APIs (`lockAsync`/`tryLockAsync`/`acquireAsync`/
`releaseAsync`) express the same failures as exceptionally-completed futures.

## 7. Atomic variables and version stamps

`OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean` (protocol v4) are the library's only
**non-lock** coordination primitives: a shared mutable cell inside the replicated state.
The semantic differences from locks must be known item by item:

- **The value has no owner** — it does not belong to any `(session, thread)`. Creator death
  or session timeout never rolls it back or zeroes it; the value lives as long as the key.
- **Every operation is a network round-trip** (reads included). Reads are not free memory
  semantics; write replies carry `(value, version)` so read-modify-write should be one
  stamped CAS instead of a read plus a write.
- **Version stamp**: every successful value-changing write bumps it by exactly 1 (GET and
  missed CAS do not). It is the sole basis for timeout re-judgement and ABA elimination —
  `compareAndSetStamped(expectedValue, expectedVersion, update)` is the ABA-free form;
  the JDK-shaped `compareAndSet(value, value)` still suffers value rebound.
- **A timeout means an indeterminate outcome**: the SDK automatically retries with the
  **same op sequence** and the server's per-key dedup slot guarantees no double apply; if
  the session switched mid-retry the SDK aborts instead — re-check with `getStamped()`,
  never blindly retry a different value.
- **Entries are never reclaimed**: there is no delete; govern key cardinality yourself.
- Overflow matches the JDK (wrap within long/int32 domains; boolean restricted to {0,1}).

## Primitive cheat sheet

| Primitive | Reentrant | Key semantics |
|---|---|---|
| Reentrant lock | ✅ | per-thread count; mutex across threads |
| Simple lock | ❌ | re-acquire queues **against itself** until lease expiry — by design, not a bug |
| Read-write lock | write ✅ | no upgrade/downgrade special cases: both directions queue generically (self-deadlock to lease if careless) |
| Fair lock | ✅ | explicit fairness promise, otherwise equal to reentrant |
| Semaphore | — | N-permit gate; releasing more than held throws `IllegalMonitorStateException` |
| Count-down latch | — | one-shot: `init` fixes total, `countDown` to zero releases all `await`s; entry lives until node restart |
| Atomic variables | — | value has no owner (death never rolls back); every write bumps the version stamp by 1; ABA only eliminated by `*Stamped` forms; entries never reclaimed (v4) |

## Next

Hands-on → [02 Quick Start](02-quickstart.md)
