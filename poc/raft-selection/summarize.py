#!/usr/bin/env python3
"""P2-04 门槛汇总（spec「报告证据可追溯」）：读 results/*.json，
按候选 × 实验聚合 3 轮中位数，对照详设 §2.4 输出判定表（markdown）。
用法：python3 summarize.py [results目录]
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "results")
GATE_P99_MS = 20.0
GATE_KILL_MS = 10_000
GATE_SNAP_MS = 30_000

rounds = defaultdict(lambda: defaultdict(list))  # (cand, exp) -> metric -> [values]
viol = defaultdict(list)

for f in sorted(RESULTS.glob("*.json")):
    d = json.loads(f.read_text())
    cand, exp, rnd = d["meta"]["candidate"], d["meta"]["exp"], d["meta"]["round"]
    if rnd == 0:
        continue  # round-0 为开发验证轮，不入正式判定（D7：仅 1..3 为正式轮）
    if exp == "bench":
        for k in ("grantLatencyP99Ms", "grantLatencyP50Ms", "grantLatencyMeanMs",
                  "throughputPairsPerS"):
            rounds[(cand, "bench")][k].append(d[k])
    elif exp == "kill":
        for k in ("tElectMs", "tServeMs", "recoveryTotalMs"):
            rounds[(cand, "kill")][k].append(d[k])
        rounds[(cand, "kill")]["lockSurvived"].append(bool(d["lockSurvived"]))
    elif exp == "snapshot":
        for k in ("bulkMs", "snapshotBytes", "snapshotWriteMs", "catchupMs",
                  "victimRebuilds", "victimRebuildFailures"):
            rounds[(cand, "snapshot")][k].append(d[k])
        rounds[(cand, "snapshot")]["digestEqual"].append(bool(d["digestEqual"]))
    if d.get("invariantViolated"):
        viol[(cand, exp)].append(rnd)


def med(vals):
    return statistics.median([float(v) for v in vals])


def fmt(v):
    if isinstance(v, float):
        return f"{v:.3f}" if v < 100 else f"{v:.0f}"
    return str(v)


print("# 门槛判定表（3 轮中位数，rounds 按 D7 A/B 交替）\n")
print("| 候选 | 指标 | 轮值 | 中位 | 门槛 | 判定 |")
print("|---|---|---|---|---|---|")
for cand in ("ratis", "jraft"):
    b = rounds.get((cand, "bench"), {})
    k = rounds.get((cand, "kill"), {})
    s = rounds.get((cand, "snapshot"), {})
    if b.get("grantLatencyP99Ms"):
        v = med(b["grantLatencyP99Ms"])
        print(f"| {cand} | 授予延迟 P99 (ms) | {[fmt(x) for x in b['grantLatencyP99Ms']]} "
              f"| {fmt(v)} | < {GATE_P99_MS} | {'通过' if v < GATE_P99_MS else '不通过'} |")
    if k.get("recoveryTotalMs"):
        v = med(k["recoveryTotalMs"])
        surv = all(k.get("lockSurvived", [False]))
        print(f"| {cand} | 杀主恢复 (ms) | {[fmt(x) for x in k['recoveryTotalMs']]} "
              f"| {fmt(v)} | < {GATE_KILL_MS} | {'通过' if v < GATE_KILL_MS else '不通过'} |")
        print(f"| {cand} | 杀主锁保留 | {k.get('lockSurvived')} | 全真={surv} | — | "
              f"{'通过' if surv else '不通过'} |")
    if s.get("catchupMs"):
        v = med(s["catchupMs"])
        eq = all(s.get("digestEqual", [False]))
        rb = med(s.get("victimRebuilds", [0]))
        rf = sum(s.get("victimRebuildFailures", [0]))
        print(f"| {cand} | 10 万条目快照恢复+追赶 (ms) | {[fmt(x) for x in s['catchupMs']]} "
              f"| {fmt(v)} | < {GATE_SNAP_MS} | {'通过' if v < GATE_SNAP_MS else '不通过'} |")
        print(f"| {cand} | digest 全量一致 / rebuild 证据 | eq={s.get('digestEqual')} rb={s.get('victimRebuilds')} fail={rf} | "
              f"全真且走快照路径={eq and rb == 1 and rf == 0} | — | {'通过' if eq and rb == 1 and rf == 0 else '不通过'} |")
    if s.get("snapshotBytes"):
        print(f"| {cand} | 快照字节数 | {s['snapshotBytes']} | 参考值 | — | — |")
    if b.get("throughputPairsPerS"):
        print(f"| {cand} | bench 吞吐 (对/s) | {[fmt(x) for x in b['throughputPairsPerS']]} "
              f"| {fmt(med(b['throughputPairsPerS']))} | 参考值 | — |")

fb = rounds.get(("noraft", "bench"), {})
if fb.get("grantLatencyP99Ms"):
    print(f"\nfloor(no-raft, 同脚本同机): P50={fmt(med(fb['grantLatencyP50Ms']))}ms "
          f"P99={fmt(med(fb['grantLatencyP99Ms']))}ms "
          f"mean={fmt(med(fb['grantLatencyMeanMs']))}ms "
          f"吞吐={fmt(med(fb['throughputPairsPerS']))}对/s")
for cand in ("ratis", "jraft"):
    b = rounds.get((cand, "bench"), {})
    f = fb
    if b.get("grantLatencyP50Ms") and f.get("grantLatencyP50Ms"):
        print(f"delta({cand} - floor): P50=+{med(b['grantLatencyP50Ms']) - med(f['grantLatencyP50Ms']):.3f}ms "
              f"P99=+{med(b['grantLatencyP99Ms']) - med(f['grantLatencyP99Ms']):.3f}ms")

print(f"\n不变式违例轮次: {dict(viol) if viol else '无（全部轮次）'}")
