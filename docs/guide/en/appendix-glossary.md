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
