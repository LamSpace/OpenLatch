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

## 7. Atomic variables, version stamps and atomic references

`OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean` (protocol v4) and `OAtomicReference`
(protocol v6) form the ATOMIC family — the library's only
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

### 7.1 Atomic references and small payloads (v6)

`OAtomicReference` extends the family above the scalar forms with **one opaque byte
payload** (the coordination-level counterpart of the JDK `AtomicReference`) — the first
primitive opening the roadmap's small-payload tier:

- **Payloads are opaque** — the server stores and compares bytes and **never deserializes
  or interprets** them. Object forms are the application's own encode/decode business
  (UTF-8 strings ride the `String` convenience methods). This is a deliberate boundary,
  not a limitation: content-addressed retrieval belongs in a KV store, not here.
- **Size is authoritatively clamped server-side** — the per-key payload cap is
  `openlatch.server.limit.max-value-bytes` (default 4KB). Over-limit writes are rejected
  **at ingress** (`OpenLatchException`, rejection semantics) with zero effect — oversized
  commands never enter the replication log, so the cluster pays no replication or snapshot
  cost for them. The clamp runs only at the ingress (leader/standalone entry point), so
  per-node config drift cannot fork replicas; **lowering the cap never retro-affects
  stored values** — existing larger payloads remain readable, subsequent writes obey the
  new limit.
- **null and the empty byte string are two distinct values** — `set(null)` clears,
  `compareAndSet(null, x)` establishes from the empty state; a CAS expecting null misses
  an empty-string entry.
- Version stamps, dedup slots, same-op-seq retries and timeout re-judgement are
  **item-for-item isomorphic** with the scalar forms (one shared decision table); value
  comparison is byte-content equality and `compareAndSetStamped` is the ABA-free form.
- **No arithmetic**: no `incrementAndGet`/`accumulateAndGet` counterparts (bytes have no
  addition; the JDK `AtomicReference` has none either).
- **Residency is made visible**: entry-plus-payload residency is a real memory/snapshot
  cost. The console and admin protocol show each payload's **size and a truncated
  preview** (full bytes never appear in admin responses); snapshot size governance
  lives in [05 §2/§4.3](05-cluster-deployment.md).

## 8. Cyclic barrier and generations

`OBarrier` (protocol v5) mirrors the JDK `CyclicBarrier`: N parties meet, and the
barrier rewraps for the next round. Three contract deltas you must know:

- **Leaving breaks the generation (stronger than the JDK)** — in the JDK a dead
  thread simply never arrives and the barrier waits forever. Here any *arrived*
  participant leaving — `await(timeout)` expiry, interruption, explicit
  `breakBarrier()`, an action that throws, or **process death / session expiry**
  (adjudicated at the replicated `SESSION_CLOSE`) — instantly breaks the current
  generation; every waiter in it gets `OBrokenBarrierException`.
- **Breakage is generation-local (weaker than the JDK)** — the JDK keeps a broken
  barrier until `reset()`; OpenLatch's next arrivals simply open a fresh
  generation, so there is **no `reset()`**.
- **barrierAction runs inside the last arriver's own await** — no one is released
  until the action reports done (faithful to the JDK's "closed until opened");
  an action failure breaks the generation for everyone.

Generation numbers increase monotonically per key; responses echo them so a
resend settles against its own generation. Arrival is a stateful request: on a
session switch an in-flight await is abandoned, never replayed.

## 9. Bounded queues and delay visibility

`OBlockingQueue`/`ODelayQueue` (protocol v7) mirror the JDK `BlockingQueue`/
`DelayQueue` as coordination-plane primitives carrying **small payload
elements** (each element subject to the same `max-value-bytes` clamp, 4KB
default). Elements live in the replicated log, owned by no session:

- **Elements are bound to the key, not to a session (stronger than the JDK)** —
  a producer's process death **never swallows elements**: where a JDK queue dies
  with its heap, here elements persist until consumed. Conversely, nothing
  consumes them by itself — entries and elements are never reclaimed server-side,
  so a forgotten key is real memory/snapshot cost (the console shows depth and
  total resident bytes).
- **Capacity is always declared (no unbounded form)** — the first write's
  `capacity` claim forms the entry (server ceiling `max-queue-capacity`,
  default 1024); a handle claiming a different capacity is rejected. The two
  "fulls" are distinct: **element-full** makes `offer` return false and `put`
  park; **waiter-queue-full** (the `max-queue-depth-per-key` guard) rejects a
  parking request with `OVERLOADED`.
- **`put/take` ride the wait-notify-resend loop** — same machinery as Latch/
  Barrier awaits; an apply-point race (precheck said yes, commit lost the race)
  bounces back into waiting transparently. `offer/poll(timeout)` budgets are
  client-side clocks; parked waits have no server-side expiry (contract).
- **Per-session dedup slots make resends safe**: a replayed `put` never
  double-inserts; a replayed `take`/`drainTo` delivers **the exact same bytes**.
- **Elements are never null** — the JDK `BlockingQueue` rejectNull contract
  (deliberately the *opposite* of `OAtomicReference`, where null is a first-class
  value); a zero-length byte array is a legal element.
- **Delay form** — injection carries a relative delay; the **absolute expiry is
  folded at the apply point from the entry timestamp** and replicated
  deterministically (leader changes cannot re-judge). Unexpired elements are
  invisible to every consumption path but count in `size()`; dequeue order is
  (earliest expiry, then arrival) — the per-tie FIFO is an enhancement over the
  JDK. The delayed-injection method is named `offerDelayed(e, delay, unit)`,
  **not** JDK's `offer(e, timeout, unit)`: that signature means "delay" on a
  DelayQueue but "wait budget" on a BlockingQueue — renaming avoids the trap.
  Wake-up granularity is the server-side ready tick (`ready-tick-ms`, default
  200ms); correctness never depends on it.
- **No `iterator`/`contains`/`remove(Object)` surface**; batch consumption rides
  `drainTo` (subject to the `max-drain-bytes` reply budget).

## 10. Broadcast publish/subscribe

`OTopic` (protocol v8) is the coordination-plane counterpart of JDK
`Flow.Publisher`/`SubmissionPublisher`: one key is one broadcast channel and
every published message is delivered, best-effort, to all registered
subscribers. It closes the tier-2 payload family and is the **only primitive
that never touches the replication log** — topics have no replicated state;
the subscription registry is Leader memory that lives and dies with a term.

- **At-most-once (an explicit weakening)** — a `publish` return means the
  server **accepted the message and enqueued it for fan-out**, never that any
  subscriber received it. Buffer overflow, disconnects and the leader-change
  window all lose messages; the server never retransmits or reports individual
  losses. Where delivery must survive, use `OBlockingQueue`, not a topic.
- **Weak backpressure = drop-newest with two buffer tiers** — each
  subscription's server-side in-flight buffer (`max-subscription-buffer`,
  default 256) drops **the newest message** and counts it when full, with the
  already-queued messages still delivered; the SDK adds a local tier with the
  same policy. A slow subscriber never backpressures publishers and is never
  disconnected (deliberately unlike JDK `SubmissionPublisher`'s
  overflow-close: closing a connection here would kill the session and release
  every lock it holds). Losses surface only via `droppedCount()` (same-term
  `topic_seq` gaps plus local overflow counts).
- **Ordering is per-subscription within one Leader term** — a given
  subscriber sees strictly ascending `topic_seq`; no global order across
  publishers or subscriptions; `topic_seq` restarts after a leader change and
  is not comparable across terms (missed messages are never replayed — the
  SDK automatically re-subscribes and delivery resumes).
- **Retry dedup is same-Leader only** — a same-`op_seq` resend hits the
  per-session dedup slot and never double-fans-out; a retry crossing a leader
  change (including manual application-level retries) **may double-deliver**:
  consumer idempotence is the application's obligation.
- **Subscriptions bind to the session (the deliberate inverse of "death
  never swallows elements")** — when a subscriber's process dies it is
  unsubscribed (registry entry, buffer and dedup slot reclaimed on all three
  paths). Queues carry resident data that must survive; topics carry delivery
  events that need not. Long-lived subscribers should `unsubscribe()`
  explicitly; registrations and buffers have a standing server-side cost.
- **Payload and version discipline follow the tier-2 rules** — opaque
  non-null message bodies (zero length is a legal empty message, queue-element
  precedent) clamped per message by `max-value-bytes`; TOPIC messages require
  a v8 handshake; a topic key colliding with another family's key is
  ingress-rejected on a best-effort read-only probe — "one key, one form" is
  an **application-side contract** for topics, not mechanical exclusion.

## 11. Condition variables and wait sets

`OLock.newCondition(name)` (protocol v9) is the coordination-plane counterpart
of JDK `Condition`: a wait/notify channel under a lock key. The wait set is
**Leader process-volatile state** — never in the replication log, never in a
snapshot, destroyed at leader change and repaired by automatic client
re-registration. It is the first primitive since tier 2 that opens no new
infrastructure: awaits fold into the existing wait–notify–resend acquisition
loop, and the signal family is pure Leader-local queue surgery.

- **await = full release folded + re-acquire on wake** — accepting an await
  atomically performs "reentrancy count zeroed in one step, lease cleared,
  watchdog stopped + registration in the wait set" (registration becomes
  visible before the release, so the critical section has no lost-wakeup
  window); after a wake the client re-acquires under the wait–notify–resend
  discipline and **returns holding the lock with exactly 1 reentrancy
  level** (JDK-identical arithmetic: the N levels held before await were
  zeroed by the await itself).
- **Spurious wake-ups are allowed and never promised away (the JDK contract —
  a fidelity, not a downgrade)** — wake sources include head-timeout sweep
  promotions, predicate changes whose signal was lost in a leader-change
  re-registration window, and LEAVE/SIGNAL race convergence. **The guard
  loop is the caller's obligation**: always wrap await as
  `while (!predicate) condition.await();` — a bare await is a defect.
- **Named addressing replaces JDK handle identity (an adaptation, not a
  loss)** — condition identity is (lock key, condition name): repeated
  `newCondition("x")` calls in one process and same-name handles created by
  different processes all bind to the **same server-side wait set**;
  different names are isolated. JDK handle identity cannot cross processes;
  named addressing buys cross-process equivalence instead.
- **signal requires ownership (two-layer, split-track permissions)** —
  `signal`/`signalAll` require the caller's (session, thread) to be exactly
  the current holder: the SDK checks locally first (misuse throws
  `IllegalMonitorStateException` with zero requests sent), and the server
  re-checks authoritatively (line code `NOT_HELD` maps to the same
  exception). **await permission is checked locally only** — the server does
  not verify the registrant holds the lock (cross-leader-change
  re-registration must pass: the old session's holding was already released
  at session close); misused/malicious registrations become ghost waiters
  bounded by the merged depth guardrail (shared `max-queue-depth-per-key`
  with the wait queue) and reclaimed on three paths (LEAVE / session death /
  leader change) — and ghosts are indistinguishable from predicate races,
  which is JDK concurrency common sense.
- **"Waiting is a promise, signal is an event" (the leader-change layering)**
  — an await rides the replicated log: its release half replays
  deterministically (the promise survives), and the emptied wait set is
  refilled by the SDK **automatically re-registering** waiters; but
  SIGNAL/SIGNAL_ALL are never logged and never compensated — **signals sent
  inside a leader-change window are lost**, and a waiter at best re-registers
  and waits for the next signal or its local timeout (timed await is the
  application's self-rescue surface). Same precedent as the topic's
  at-most-once/no-replay rule: the server does not custody event-shaped
  state for the application.
- **Waiters hold no lease; holder death never signals for you** — when the
  holder's process dies or its lease expires, the server sweep releases the
  lock and wakes the **wait-queue entrants only**; condition waiters are
  untouched — with nobody signaling, they sleep forever (JDK-aligned;
  **`await(timeout, unit)` is the recommended form**). Waiters themselves
  have no lease and no renewal duty.
- **Narrowed support surface** — only the REENTRANT/FAIR/SIMPLE exclusive
  forms host conditions; `newCondition` on a read/write lock handle throws
  `UnsupportedOperationException` (local verdict). Fair-lock extension:
  carried waiters enter the FIFO **at carry time** — a signaled thread is
  not prioritized over elders already queued (the JDK non-fair effect; the
  fairness promise constrains enqueue order only).

## 12. Phaser and phase rendezvous

`OClient.newPhaser(key[, initialParties])` (protocol v10) is the distributed
counterpart of JDK `Phaser`: multi-party rendezvous **by phase** with dynamic
membership — participants register and deregister freely, arrival and waiting
are decoupled, and phase numbers advance monotonically without reuse. Against
the cyclic barrier (v5) the three hardening points become: fixed parties →
**dynamic per-session quotas**; arrive-equals-gate → **arrive can be
non-blocking** (`arrive` vs `arriveAndAwaitAdvance`); leave-breaks →
**death removes quota without exploding the group**. The ledger (quotas,
arrival counts, phase) is replicated state — every mutation is a log entry and
survives failover and restarts; wait bookkeeping is Leader-local (v9's dual
topology precedent).

- **Quotas are session-attributed** — `register`/`bulkRegister(n)` credit the
  calling session; `arriveAndDeregister` can only decrement its own session's
  quota (overdraft rejected — JDK's anonymous-party undefined behavior made
  explicit, so death cleanup knows exactly what to remove).
- **Arrival is a state transition (all mutations logged)** — register/arrive/
  deregister each commit a `PHASER_OP_ENTRY` (v5 Barrier doctrine; Latch's
  "counts in, waits out" family). Trip rule: `arrived > 0 ∧ arrived ≥
  registered`; after a trip counts reset but **registrations persist across
  phases** (only departure removes).
- **Two-layer replay idempotence** — arrival dedup is keyed
  (session, request) valid within the current phase; a rolled **previous-
  generation window** keeps the last trip's arrivals for one cycle so a lost-
  reply resend terminates with its arrival phase instead of double counting
  (declared race beyond the window, same bound style as Barrier's completed
  record). Registration carries a per-session last-(request, phase) slot.
- **Death: implicit quota removal, arrival facts stand, no stall** — a dead
  session's outstanding quota is subtracted immediately (the obligation can
  trip on the spot — one dead member never starves the group), while already
  counted arrivals are NOT rolled back (a fact once replicated stays, the
  queue's "death doesn't swallow elements" counterpart). Contrast with
  Barrier's break-on-death: dynamic membership lets removal replace breaking —
  the generalization pays off exactly here.
- **Zero registered = idle, never terminated** — unlike JDK (zero-party
  termination is sticky), this library provides no termination surface: after
  everyone deregisters the key idles (phase kept, quota 0) and later
  `register` resumes from the current phase. Same reasoning as Barrier's
  refusal of sticky breakage.
- **Three Non-Goals** — no `onAdvance` hook (the server runs no user code;
  discriminate "I was the last arrival" from the returned arrival phase and
  run your action locally — no ordering versus others' wake-ups); no
  termination; no parent/child tiering (a local-counter scalability
  optimization whose benefit evaporates behind per-arrival RTT). Also:
  phase numbers are `long` (JDK `int` wraps at 2^31).
- **Waits self-heal across leader change (stronger than v9's contrast)** —
  the wake predicate "current phase > observed phase" lives in the replicated
  ledger, so lost wake pushes are repaired by the client's bounded re-sends
  immediately — no loss window, no compensation needed (contrast the
  condition's "signal is an event" loss window). `awaitAdvance(int)` is
  bounded by the wait budget (default 30 s then `OpenLatchTimeoutException`;
  loop to re-enter for JDK's unbounded semantics);
  `awaitAdvanceInterruptibly(phase, timeout, unit)` best-effort CANCELs before
  throwing `TimeoutException`.
- **Reads are RTT-priced, Leader-local, zero-log** — every getter is one
  QUERY round trip; readings are advisory, **not linearized** — deliberately
  diverging from v4's "GET also commits" precedent (a phase reading is stale
  by language fact).
- **Two guardrails, separate lines** — `max-parties-per-phaser` (default
  1024) caps per-key registration (overflow `OVERLOADED`, existing quotas
  untouched, checked only at admission); suspended waits share
  `max-queue-depth-per-key` (pure `awaitAdvance` overflow `OVERLOADED`;
  `arriveAndAwaitAdvance`'s wait half is always lenient — an arrival already
  in the ledger is never stranded by depth).

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
| Atomic reference | — | opaque payload ≤ `maxValueBytes` (default 4KB, clamped authoritatively at ingress, zero effect on overflow); null and empty-string are distinct; version stamp/dedup isomorphic with scalars; entry and payload never reclaimed (v6) |
| Cyclic barrier | — | N-party rendezvous, reusable generations; **leaving breaks the current generation** (death/timeout/interrupt/break — stronger than the JDK); no sticky broken state, no `reset()`; the last arriver runs the action (v5) |
| Bounded queue | — | elements bound to the key, not the session (**producer death never swallows them** — stronger than the JDK); declared capacity, two "fulls" split (element-full = false/park, waiter-full = OVERLOADED); dedup slots: no double-insert, identical replay; elements never null; delay form folds expiry at the apply point, per-tie FIFO (v7) |
| Broadcast topic | — | at-most-once; weak backpressure = drop-newest across two buffer tiers (never backpressures, never disconnects); per-subscription ascending seq within one term, rebased at leader change; dedup only same-Leader (cross-term retries may double-deliver — consumer idempotence required); **death unsubscribes** (the queue's inverse); "one key, one form" is an application contract (v8) |
| Condition variable | host lock's | await folds a full release and re-acquires — **returns holding the lock** with reentrancy counting from 1; spurious wake-ups allowed, **the guard loop is the caller's obligation**; signal requires ownership (authoritative server check + local pre-check, two layers), await permission is local-only (an explicit downgrade); named addressing (key, name) is cross-process equivalent; **waiting is a promise, signal is an event** (auto re-registration across leader change, in-window signals uncompensated); waiters hold no lease and holder death never signals for you (prefer timed await); read/write forms unsupported; waiter count merges into the key's waiting guardrail (v9) |
| Phaser | — | dynamic quotas attributed per session (`arriveAndDeregister` overdraft rejected); registrations/arrivals/departures each logged, **quotas persist across phases (only departure removes)**; trip = arrivals ≥ quota with immediate wake; **death removes quota without stalling and never rolls back counted arrivals** (contrast Barrier's break-on-death); zero quota = idle, no termination (re-register revives); no hook / no tiering / long phases (three Non-Goals); wait predicate lives in the ledger — **self-heals across leader change** (contrast the condition's signal loss window); every read is one RTT, advisory, non-linearized; `awaitAdvance` bounded by the wait budget (v10) |

## Next

Hands-on → [02 Quick Start](02-quickstart.md)
