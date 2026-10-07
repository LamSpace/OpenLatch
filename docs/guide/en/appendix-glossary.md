# Appendix · Glossary

| Term | Definition |
|---|---|
| lease | the bounded validity of a grant (default 30s, clamped); unrenewed ⇒ reclaimed |
| watchdog | client-side background renewal at `lease/3` keeping held locks alive |
| grant | a successful acquisition result: credentials (`leaseToken`) + expiry |
| renewal | in-term lease extension request |
| session | server-side identity of a handshaked connection; disconnect closes it and releases its locks |
| waiter | a registration in a key's queue after a failed acquisition |
| wait–notify–resend | the queuing mechanism: immediate `QUEUED`, push notify at the head, client resends the same request to claim |
| head-reply timeout | how long a notified-but-silent head waiter keeps its seat (default 5s) |
| FIFO fairness | strict arrival-order grants per key; queues reshuffle on leader change |
| thundering herd | mass-wake anti-pattern OpenLatch avoids by notifying only the head |
| lock loss | revocation via lease expiry, session closure or failover rollback; surfaced through `LockLostListener` |
| leader hint | leader address carried in HELLO / `NOT_LEADER` responses driving client reroute |
| seed discovery | concurrent `CLUSTER_VIEW` probing of the seed list to locate the leader |
| forwarding lane | Follower-to-Leader internal path serving RELEASE/RENEW arriving at non-leaders |
| reroute | migrating a client connection from a Follower to the Leader |
| quorum | surviving majority (> N/2) required for commits and elections |
| replicated state machine | Raft's model: replicated log + deterministic apply ⇒ identical replicas |
| snapshot | periodic full state dump compressing the log and bootstrapping lagging nodes |
| log truncation | dropping Raft log entries before the snapshot index |
| digest | SHA-256 cross-replica state summary used to verify convergence |
| double-award | two authoritative holders of one key simultaneously — the invariant OpenLatch guarantees never happens |
| replication stall | frozen commit progress in a leader term; watchdog detects and self-heals (abdicate → escalate) |
| election storm | prolonged leaderlessness; the watchdog has nothing to abdicate from — backed-off restart restores |
| admin token | per-message read-only observation credential (`ADMIN_*`), independent from business tokens |
| business token | handshake-scoped connection credential (`auth_token`); failure disconnects |
| weakly consistent read | observation surfaces served from one replica's local state, may lag quorum commits briefly |
| generation | one rendezvous cycle of a cyclic barrier; monotonically increasing per key, rewound on trip or break |
| leave-breaks | contract where any arrived participant's departure (timeout/interruption/death/explicit break) instantly breaks its current generation |
| version stamp | per-key monotone counter of the atomic cell: every successful write adds exactly 1; the basis for timeout re-judgement and ABA elimination |
| dedup slot | per-atomic-entry record of the last applied write (session, op-sequence, reply quad) that makes same-sequence retries apply only once |
| initial claim | a handle's non-zero initial-value assertion: applies on first create, rejects on mismatch with the settled value (latch-total precedent); the reference form reworks it as a presence claim (absent = no claim, empty = empty-string claim) |
| atomic reference | the payload form of the ATOMIC family (v6): one opaque byte payload plus a version stamp, get/set/stamped-CAS, the server never deserializes |
| payload clamp | the per-key reference-payload byte cap (`max-value-bytes`, default 4KB): enforced only at ingress, over-limit commands never enter the log and have zero effect, lowering the cap never retro-affects stored values |
| truncated preview | the admin-plane rendering of a reference payload: a constant-length (≤64B) escaped prefix plus the true byte size; full payload bytes never appear in an admin response |
| declared capacity | the capacity claim that forms a queue entry at first write (v7, clamped by `max-queue-capacity`); later non-zero mismatching claims are rejected |
| dequeue predicate | whether the head element may be consumed: always true for QUEUE; for DELAY the head's absolute expiry must not exceed the judging instant (no skipping an unexpired head) |
| expiry folding | a delay element's absolute expiry is computed at the apply point from the entry-carried timestamp (the lease `expires_at` precedent); byte-identical across replica replays |
| dual-track waiting | the two queue waiter identities (waiting-for-capacity / waiting-for-elements), each with its own ranks and wake path (v7) |
| apply-point bounce | a committed-but-raced queue op returns DENIED at the apply point and is rewritten to QUEUED at its track position — invisible to the caller (v7) |
| head gate | a non-head parked waiter's resend may not overtake elders in its track (core entry and Leader precheck enforce it alike), pinning wake-grant order to park order (v7) |
| broadcast term | the lifetime of one Leader's authority for broadcasts: `topic_seq` is ordered only within a term and restarts at leader change — cross-term sequence numbers are not comparable (v8) |
| topic_seq | the accepted sequence number of a broadcast message within a term (Leader memory, never logged); strictly ascending per subscription, and its gaps are the subscriber's loss-estimate basis (v8) |
| weak backpressure | the explicit broadcast backpressure contract: per-subscription buffers on two tiers, drop-newest when full — no backpressure to publishers, no disconnects, drops counted but never announced (v8) |
| drop-newest | the overflow policy: discard the incoming (newest) message and count it, delivering the already-queued ones (deliberately unlike JDK `SubmissionPublisher`'s close-on-overflow) (v8) |
| gap inference | the subscriber-side loss estimate: same-term `topic_seq` jumps plus local buffer overflows, exposed as `droppedCount()` (v8) |
| death unsubscribes | a topic subscription binds to its live session — process death reclaims registry, buffer and dedup slot (the deliberate inverse of the queue's "death never swallows elements") (v8) |
| condition wait set | the arrival-ordered waiter registry grouped by condition name under a lock key (v9): Leader process-volatile, never logged or snapshotted, reclaimed on three paths (LEAVE / session death / leader change) and counted together with the wait queue under the one `max-queue-depth-per-key` guardrail |
| named addressing | condition identity is (lock key, condition name) rather than handle identity (v9): same-name handles — re-created in one process or created by different processes — bind the same server-side wait set; the distributed adaptation of the JDK's handle identity, cross-process equivalence is the capability side |
| release folding | await's two half-steps ("full release + wait-set registration") ride ONE existing ACQUIRE entry atomically (v9): registration becomes visible before the release, leaving no lost-wakeup window inside the critical section |
| carry | the movement of condition waiters, in arrival order, from the wait set into the lock's wait queue by SIGNAL/SIGNAL_ALL (v9): carried waiters then follow the existing head-grant discipline and leave the condition-set readouts |
| signal-as-event | the signal family is an event, not a state (v9): never logged, never compensated, never replayed — signals inside a leader-change window are lost, and waiters self-rescue with timed awaits |
| returns holding the lock | the JDK-faithful await contract (v9): woken, timed out or interrupted alike, an await returns (or throws) only after re-acquiring the lock, with the reentrancy count starting at 1 |
| guard-loop obligation | spurious wake-ups are allowed and callers MUST wrap await in a predicate re-check loop (v9): a wake is not proof the predicate holds — the wake-source list is contract-visible |
| wait-as-promise, signal-as-event | the leader-change layering of conditions (v9): the waiting promise survives via log replay plus automatic client re-registration; signals are fire-and-forget events lost in the window (the same rule topics follow for messages) |
| arrival phase | arrival phase | the phase recorded when an arrive-family op fires (v10): the return-value semantics of `arrive`/`arriveAndAwaitAdvance`; a trip-triggering arrival still echoes its own arrival phase |
| expected party set | expected party set | the registration total that must arrive for the current phase to complete (v10): quotas persist across phases, only departure removes; trip = arrivals > 0 AND arrivals ≥ quota |
| registration quota | registration quota | a session's outstanding registered participant count (v10): `arriveAndDeregister` only spends its own session (overdraft rejected); a dead session's whole row is removed implicitly — JDK anonymous-party semantics made explicit so cleanup has a determinate ownership domain |
| implicit quota removal | implicit quota removal | on session death the server subtracts the dead quotas immediately (the phase may trip on the spot — no stall) while counted arrivals are never rolled back (v10) — the deliberate contrast to Barrier's break-on-death: one death never detonates the group |
| idle-then-revive | idle-then-revive | zero registrations is NOT a terminal state (v10): the ledger idles (phase kept, quota 0) and later registrations resume from the current phase — inverted from JDK's sticky zero-party termination, same reasoning as refusing sticky barrier breakage |
| previous-generation window | previous-generation window | the rolled slot set + arrival phase kept for one cycle after each trip (v10): lets a cross-trip replay of an arrival terminate idempotently without double counting; rolls out on the next trip, late arrivals past the window count fresh (declared race, same bound style as Barrier's completed record) |
| arm & clock-clear | arm & clock clear | each timer `schedule` mints a new generation and clears the previous sticky mark (v11): round semantics ride on "sticky + clearing at re-arm", and pending waits follow the newest generation |
| derived expiry predicate | derived expiry predicate | `marked = armed ∧ verdict-clock ≥ fire_at_ms` (v11) — expiry is a pure function of replicated data and a clock, never a transition: arms/withdrawals log while the clock crossing appends zero entries, the two ends against lease-expiry-logs |
| shared mark | shared mark | after the fire instant the mark is visible to every arriving observer and never consumed (v11) — opposed to the queue's one-element-one-consumer handoff |
| generation terminal | generation terminal | `disarm` makes the current generation permanently un-markable until the next arm (v11): parked waiters settle immediately as DENIED/exception (cancel wakes ride on the event, not the tick) |
| wake side vs fire side | wake side vs fire side | `timer.fired.total` counts only sweeps that found ≥1 waiter to wake (v11) — a silent expiry leaves no line on the wire; read it as the wake surface, never as an expiry total |
| clock-free projection | clock-free projection | timer admin reads show raw `{generation, armed, fire_at_ms}` without converting marked (v11): byte-equal across nodes; marked folds only on Leader reply lines, skew ≤ clock offset is contractual |
| two-speed projection | two-speed projection | phaser Follower observability shape (v10): ledger counters and quota rows are replicated and readable everywhere while the waiter list is Leader-local and honestly empty on Followers — the two halves advance at different speeds and MUST NOT be conflated |
