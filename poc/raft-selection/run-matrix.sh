#!/usr/bin/env bash
# P2-04 正式实验矩阵（D7 公平协议）：3 轮 A/B 交替 × {bench 300s, kill, snapshot 100k}
# + no-raft floor bench 3 轮。输出到 results/，全部完成后由 summarize.py 汇总。
set -uo pipefail
cd "$(dirname "$0")"

RC=0
for r in 1 2 3; do
  if (( r % 2 == 1 )); then ORDER=(ratis jraft); else ORDER=(jraft ratis); fi
  echo "==== round $r order: ${ORDER[*]} ===="
  echo "---- floor bench round $r ----"
  ./poc-run.sh noraft bench "$r" --duration 300 || RC=1
  for c in "${ORDER[@]}"; do
    echo "---- $c bench round $r ----"
    ./poc-run.sh "$c" bench "$r" --duration 300 || RC=1
    echo "---- $c kill round $r ----"
    ./poc-run.sh "$c" kill "$r" || RC=1
    echo "---- $c snapshot round $r ----"
    ./poc-run.sh "$c" snapshot "$r" --keys 100000 || RC=1
  done
done
echo "MATRIX_DONE rc=$RC"
exit $RC
