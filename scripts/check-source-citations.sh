#!/usr/bin/env bash
#
# check-source-citations.sh — 防回潮门禁（spec: source-comment-discipline）
#
# 扫描范围（模式集的单一事实源：本地提交前自查与 CI 门禁共用本脚本）：
#   1. 七个发布模块 openlatch-*/src/main/**/*.java 的全部行
#      （Javadoc、行内注释与字符串字面量一律按对外面处理——
#       maven-javadoc-plugin 配置 show=private，私有成员注释同样外发）；
#   2. openlatch-*/pom.xml 的 <description> 元素区
#      （pom 其余区域如构建注释不在发布元数据面内，不扫描）。
#
# 模式集（ERE，逐行匹配；假阳性预演结论记于各条目后）：
#   设计说明书|概要设计|详细设计|实施计划|验收报告|验收标准|详设
#       —— 内部过程文档名与简称；"验收标准 N" 是验收报告判据编号，
#          "说明书" 不单独入模式（全部实际用法已被 设计说明书 覆盖）。
#   退出门 —— 内部收口判据术语（"装配阶段/业务阶段" 等中文技术词
#             属现状陈述，不入库，故 "阶段" 不入模式）。
#   openspec|\bphase[0-9]-|Phase ?[123]
#       —— openspec 根路径、小写 change 名（phase1-audit-remediation
#          等）、阶段代号；Phase 大小写敏感，"phases of GC" 类小写
#          普通词不误伤。
#   P[123]-[0-9]{2} —— 阶段任务号（P1-NN/P2-NN/P3-NN）。
#   \bT[1-4]\b|\bS[0-9]\b|\bM[1-4]\b
#       —— 任务家族代号（T/S 族）与里程碑代号（M1–M4）；词边界确保
#          AES256/TLS1.2/RT-2 类标识不误伤（T-1、小写 t2 亦不入内）。
#   §[0-9] —— 文档节号；"第 4.9 节" 等无 § 表述不属内部方言，不拦。
#   \bD[0-9]{1,2}\b|design(\.md)? ?D[0-9]{1,2}
#       —— 设计决策编号，含裸 "（D5）" 引用与两位数形态 "（D12）"
#          （清理中实际发现，单位数模式曾漏网；全仓用法均为决策号）。
#   spec[ ]?[a-zA-Z0-9_-]*["“”]|规格["“”]
#       —— delta spec 引用方言：spec"XX" / spec 模块名"XX" 与中文
#          "规格"XX""变体，直引号与弯引号并收；specific/specification/
#          `spec = x`/`spec + "x"` 等无紧邻引号的形态不误伤。
#
# 用法：
#   bash scripts/check-source-citations.sh              # 全量扫描
#   bash scripts/check-source-citations.sh --selftest   # 夹具自检（模式集回归）
#
# 退出码：0 = 零命中；1 = 有命中或自检失败。
set -u

PATTERN='设计说明书|概要设计|详细设计|实施计划|验收报告|验收标准|详设|退出门|openspec|\bphase[0-9]-|Phase ?[123]|P[123]-[0-9]{2}|\bT[1-4]\b|\bS[0-9]\b|\bM[1-4]\b|§[0-9]|\bD[0-9]{1,2}\b|design(\.md)? ?D[0-9]{1,2}|spec[ ]?[a-zA-Z0-9_-]*["“”]|规格["“”]'

MODULES=(
  openlatch-protocol
  openlatch-core
  openlatch-server
  openlatch-client
  openlatch-spring-boot-starter
  openlatch-console
  openlatch-examples
)

# scan_poms <pom-file>... —— 仅取各 pom 的 <description> 区逐行匹配，
# 输出 file:line:<整行>（awk 自带行号，grep 只做模式过滤）。
scan_poms() {
  local pom
  for pom in "$@"; do
    [ -f "$pom" ] || continue
    awk '/<description>/ {f=1}
         f {printf "%s:%d:%s\n", FILENAME, FNR, $0}
         /<\/description>/ {f=0}' "$pom"
  done | grep -E "$PATTERN" || true
}

# selftest ——脏夹具逐行必须命中（模式集不缺漏），净夹具逐行必须零命中（不误伤）
selftest() {
  local tmp fails=0 line
  tmp=$(mktemp -d) || return 1
  trap 'rm -rf "$tmp"' RETURN

  cat > "$tmp/dirty.java" <<'EOF'
// 设计说明书 §4.9 与概要设计 §4.3 标准 3
// Phase 3 详设 §2.4 / P3-05 交付
// （Phase 3 T2，spec"只读统计观察面"）
// 见 design.md D4 与（D5）的裁决
// 验收报告 / 详细设计 / 实施计划 / 验收标准 4 / 退出门
// phase2-leader-stall-followup 的 openspec 观察件
// M4 定案、S3 多车道、T2 重连计数
// spec admin-console"部署形态与配置"与 spec"客户端 TLS 与认证消费"
// 违 D12 进度保护与 规格"消息合法性校验"
EOF

  cat > "$tmp/clean.java" <<'EOF'
// GC 的 phases 不干扰续租；装配阶段按拓扑序，业务阶段处理矩阵不变
// 协议版本 v1/v2/v3 兼容回显；第 4.9 节所述结构与此无关
// String spec = null 表示未配置；拼接形态 spec + "x" 与 spec = "x"
// 报告落盘路径 docs/benchmark-baseline-2026-09-01.md 属功能事实
// specific value、specification 与 AES256/TLS1.2/T-1/t2 不误伤
// 快照比对与锁语义裁决不依赖任何过程文档即可理解
EOF

  cat > "$tmp/dirty-pom.xml" <<'EOF'
<project>
  <description>OpenLatch 管理控制台（Phase 3 详设 §4 T3）</description>
</project>
EOF

  cat > "$tmp/clean-pom.xml" <<'EOF'
<project>
  <!-- 概要设计 §5.2 出现在构建注释里：不属 description 扫描区 -->
  <description>OpenLatch 纯 Java 锁语义核心（无外部运行依赖）</description>
</project>
EOF

  while IFS= read -r line; do
    if ! printf '%s' "$line" | grep -qE "$PATTERN"; then
      echo "selftest FAIL: 脏夹具行未命中: $line" >&2
      fails=$((fails + 1))
    fi
  done < "$tmp/dirty.java"

  if clean_out=$(grep -nE "$PATTERN" "$tmp/clean.java"); then
    echo "selftest FAIL: 净夹具误命中" >&2
    printf '%s\n' "$clean_out" >&2
    fails=$((fails + 1))
  fi

  if [ -z "$(scan_poms "$tmp/dirty-pom.xml")" ]; then
    echo "selftest FAIL: 脏 pom description 未命中" >&2
    fails=$((fails + 1))
  fi

  if [ -n "$(scan_poms "$tmp/clean-pom.xml")" ]; then
    echo "selftest FAIL: 净 pom description 误命中（或注释区越界）" >&2
    scan_poms "$tmp/clean-pom.xml" >&2
    fails=$((fails + 1))
  fi

  if [ "$fails" -ne 0 ]; then
    echo "selftest: ${fails} 项失败" >&2
    return 1
  fi
  echo "selftest: OK（脏夹具全命中、净夹具零命中、pom 作用区正确）"
  return 0
}

if [ "${1:-}" = "--selftest" ]; then
  selftest
  exit $?
fi

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) \
  || REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT" || exit 1

main_dirs=()
pom_files=()
for m in "${MODULES[@]}"; do
  main_dirs+=("$m/src/main")
  pom_files+=("$m/pom.xml")
done
java_hits=$(find "${main_dirs[@]}" -name '*.java' -print0 2>/dev/null \
  | xargs -0 -r grep -Hn -E "$PATTERN" 2>/dev/null || true)
pom_hits=$(scan_poms "${pom_files[@]}")

if [ -z "$java_hits" ] && [ -z "$pom_hits" ]; then
  echo "== 零命中：对外源码与发布元数据无内部过程文档引用 =="
  exit 0
fi
[ -n "$java_hits" ] && printf '%s\n' "$java_hits"
[ -n "$pom_hits" ] && printf '%s\n' "$pom_hits"
all_hits=$( { [ -n "$java_hits" ] && printf '%s\n' "$java_hits"; [ -n "$pom_hits" ] && printf '%s\n' "$pom_hits"; } )
total=$(printf '%s\n' "$all_hits" | wc -l | tr -d ' ')
files=$(printf '%s\n' "$all_hits" | cut -d: -f1 | sort -u | wc -l | tr -d ' ')
echo "== 命中 ${total} 处，涉及 ${files} 个文件：对外源码/发布元数据禁止内部过程文档引用（见 CLAUDE.md §5）=="
exit 1
