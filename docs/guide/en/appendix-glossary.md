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
