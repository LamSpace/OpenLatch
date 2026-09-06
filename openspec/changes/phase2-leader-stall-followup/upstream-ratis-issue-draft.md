# Apache Ratis 上游提报草稿（任务 3.1 — **定夺：不投递，成稿备查**）

> 2026-09-06 用户定夺：提报不改变实现效果与形态 B 实际风险等级，不启动对外发布。
> 本稿与取证包完整保留——任一后续时点（如升级评估受阻、需要社区佐证时）即可即用。
> 若未来投递：把编号回填本文件末节与缺陷档案状态块。

- 目标：apache/ratis（GitHub issue，或 issues.apache.org JIRA RATIS 项目）。
- 附件素材（已入库本 change 目录）：`evidence-r5-jstack/`（停摆现场双 jstack×3 节点、
  三节点全量日志、配置映射）、`evidence-r6-control-1/` `-2/`（同形风暴自然收敛对照包，
  证明"风暴为前件非充分条件"），本目录
  `observations-leader-stall-rootcause.md`（机理全文），
  `docs/rolling-restart-drill-2026-09-06.md`（频率矩阵）。

---

## Title

Leader-side per-follower appender livelocks after rolling restart: INCONSISTENCY
nextIndex rewinds are negated by the `request==null` reset path, startup (conf)
entry never commits → leader permanently "not ready" (3.3.0)

## Body

**Environment**
- Ratis 3.3.0 (release artifacts, tag `ratis-3.3.0`), Java 25.0.3 LTS, Linux amd64, 8 CPUs
- 3-node group, gRPC transport, single division; steady client writes + leader NOOP
  probes during the whole run; log appender retry policy default
  (`1ms,10, 1s,20, 5s,1000`), `raft.server.log.appender.install.snapshot.enabled=true`
  (default)
- Reproduction probability observed: 3/7 leader-side-restart rounds (rolling restart,
  one node at a time under load); also occurred on one followers-first round
  (2/14 overall). Never self-heals (>200 s observed; killed at drill end).

**Symptom (two shapes of the same livelock, both observed)**
- **Shape A (frozen leader):** a successfully elected leader (role LEADER,
  heartbeats acked, `checkLeadership` keeps passing) never completes its startup
  entry: `LeaderStateImpl.isReady()` stays false (hours-scale observed: >200 s
  until manual intervention); every client write fails fast with
  `LeaderNotReadyException`; the leader refuses to step down
  (`reject PRE_VOTE …: this server is the leader and still has leadership`).
- **Shape B (election storm):** with the same preconditions, no node ever
  stabilizes as leader — all members cycle `FOLLOWER→CANDIDATE→REJECTED` with
  monotonically climbing terms while the group's commit is frozen (no
  `LeaderNotReadyException` at all in logs; client-visible as sustained
  NOT_LEADER). Eventually self-resolves (observed ≤~100 s) — bounded but with a
  ~1.5-minute write outage.

**Sequence that triggers it**
1. 3-node cluster under continuous writes; leader L1 (term T).
2. Rolling restart (SIGTERM-graceful close of each node, data dirs preserved).
   Each restarted node reopens its log at **its own state-machine snapshot index**
   (we observed `snapshotIndexFromStateMachine = 244` on one node and `398` on
   another — stop-triggered snapshots at their last applied index), i.e. its
   available log can be far below the group's committed prefix.
3. A new leader L2 is (re)elected in term T+1. Its fresh `LeaderStateImpl`
   initializes `FollowerInfo` for both followers with `nextIndex = L2.nextIndex`
   (high) and `matchIndex = -1`, and appends its startup conf entry above the
   followers' snapshot boundaries.
4. From then on both appenders cycle livelocked (log excerpts below): each
   INCONSISTENCY reply rewinds `nextIndex` to the follower's boundary
   (`setNextIndex … updateUnconditionally 553 -> 399`), but the *next* request
   built by the daemon again starts above the boundary
   (`entries=(t:4,i:401)...(t:4,i:552)` → `INCONSISTENCY nextIndex=399` again),
   interleaved with
   `GrpcLogAppender: Follower failed (request=null, errorCount=N); keep nextIndex
   (399/400/401) unchanged and retry.` (errorCount monotonically climbing,
   17→18→…, each round gated by a `sleepForErrors()` backoff which by design
   `won't be waked up by signal`).
5. Neither follower's `matchIndex` ever leaves the boundary →
   `updateCommit`'s majority median never covers the startup entry →
   `isReady()` never flips. No error is ever logged beyond the WARN lines above.

**Leader log excerpt (captured; full logs + double jstack available)**
```
n2 …->n3-AppendLogResponseHandler: received INCONSISTENCY reply with nextIndex 399,
    errorCount=17, request=AppendEntriesRequest:cid=4925:HEARTBEAT
n2 …->n3: setNextIndex nextIndex: updateUnconditionally 553 -> 399
n2 …->n3-GrpcLogAppender: Follower failed (request=null, errorCount=2);
    keep nextIndex (399) unchanged and retry.
n2 …->n3-AppendLogResponseHandler: received INCONSISTENCY reply with nextIndex 399,
    errorCount=18, request=AppendEntriesRequest:cid=4926:152 entries=(t:4,i:401)...(t:4,i:552)
[on the follower side, repeatedly:]
n3 …: Failed appendEntries, previous log entry (t:4, i:400) not found
n3 …: appendEntries* reply n2<-n3#4926:FAIL-t4,INCONSISTENCY,nextIndex=399,
      followerCommit=398,matchIndex=-1
```
jstack 10 s apart: both appender daemons alive, CPU advancing — threads park in the
retry/sleep cycle; nothing is deadlocked or dead. It is a state-machine-level
livelock between the reply-processing threads (`resetClient` /
`getNextIndexForInconsistency`) and the daemon's request construction.

**Suspects (from source reading of 3.3.0; pointing, not asserting)**
- `GrpcLogAppender.resetClient(…, request==null)` deliberately keeps `nextIndex`
  unchanged — while INCONSISTENCY handlers concurrently `updateUnconditionally`
  rewind it; the `request=null` error replies (stream teardown after the follower
  restarts) appear to re-pin `nextIndex` above the follower's boundary, undoing
  the rewind before the next request is built.
- `mayWait()/sleepForErrors()` is not signal-wakeable and its final policy stage
  sleeps 5 s *per iteration* once `errorCount ≥ 31`, so each negated rewind costs
  a full sleep; combined with `errorCountToDelay` never decaying on this path,
  progress per wall-clock second ≈ 0.
- `LeaderStateImpl.checkHealth()` has no appender-rescue/restart hook, and
  `LogAppenderDaemon` only restarts on `EXCEPTION` — a livelocked (non-throwing)
  daemon is never reset. Leadership heartbeat lease stays healthy so no re-election
  happens either; the only escape we found is restarting the leader process.

**Expected**
Either the nextIndex rewind must be authoritative against concurrent `keep
nextIndex unchanged` resets (e.g. track the follower-reported boundary), or a
persistent no-progress condition must be detected and acted on (reinstall-snapshot
path, appender restart, or leadership step-down).

**How to reproduce**
3-node cluster, steady write load (≥10 writes/s) plus the usual commit/status
probes, rolling-restart one node at a time (graceful stop, same data dir, restart
within ~3 s) while load continues; repeat 4–6 passes. Snapshot/stop-trigger
configuration matters (our client sets
`raft.server.log.purge.upto.snapshot.index=true`; `snapshot.trigger-when-stop` is
library default `true`). We have a deterministic in-JVM *harness* but it converges
28/28 — the failure needs the real JVM start/stop timing race. Happy to share the
captured logs/jstacks and refine a smaller reproducer with guidance.

---

## 跟踪编号（投递后回填）

- 编号/链接：（待回填）
