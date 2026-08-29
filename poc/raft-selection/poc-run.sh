#!/usr/bin/env bash
# OpenLatch Phase 2 / S1 PoC 一键实验脚本（P2-01）。
# 用法: ./poc-run.sh <candidate: ratis|jraft|noraft> <exp: smoke|bench|kill|snapshot> [round] [extra driver args...]
# 例:   ./poc-run.sh ratis bench 1 --duration 300
set -euo pipefail

MVN_SETTINGS="-s /home/lam/repo/settings.xml"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # poc/raft-selection
CANDIDATE="${1:?candidate required: ratis|jraft|noraft}"
EXP="${2:?exp required: smoke|bench|kill|snapshot}"
ROUND="${3:-0}"
shift 3 || true

case "$CANDIDATE" in
  ratis)  JAR="$ROOT/poc-ratis/target/poc-ratis-1.0-SNAPSHOT-all.jar" ;;
  jraft)  JAR="$ROOT/poc-jraft/target/poc-jraft-1.0-SNAPSHOT-all.jar" ;;
  noraft) JAR="$ROOT/poc-harness/target/poc-harness-1.0-SNAPSHOT-all.jar" ;;
  *) echo "unknown candidate $CANDIDATE" >&2; exit 2 ;;
esac

if [[ ! -f "$JAR" ]]; then
  echo "building poc reactor..." >&2
  # shellcheck disable=SC2086
  mvn $MVN_SETTINGS -q -f "$ROOT/pom.xml" -DskipTests package
fi

mkdir -p "$ROOT/results"
exec java -jar "$JAR" driver "$EXP" --candidate "$CANDIDATE" --round "$ROUND" \
  --workdir "$ROOT/results" --jar "$JAR" "$@"
