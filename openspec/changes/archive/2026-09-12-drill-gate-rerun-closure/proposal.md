# Proposal: drill-gate-rerun-closure

## Why

Phase 2 标准 5 自收口期定判 ⚠️ 后，其"CI/运维环境复跑"承载项在 Phase 3 验收中记录为**未执行项**（归因"无 TTY 沙箱不可靠"）；2026-09-12 评审人依 runbook 桌面复跑，连带暴露并当日修复两层夹具缺陷（`drill-metrics-port-collision-fix`：默认管理端口 9412 单机三节点互撞，T2 后必挂；`drill-port-allocation-race-fix`：探针-重绑 TOCTOU 竞态，端口被自家出站连接抢占），同时证伪"沙箱不可靠"旧归因——它是夹具回归，与执行环境无关。修复后于绑定修订 `b35f549` 完成正式全量实录（单环境、全 reactor、五轨执行 Skipped=0、15:33 min 全绿），标准 5 复核与"未执行项" closure 的数据自此齐备。

## What Changes

- Phase 2 验收报告追加"标准 5 复核记录（第二轮，2026-09-12 修复后全量复跑）"：实录数据（failover 恢复窗 1671ms、rolling 两序末窗+45s 预算后残留=0 且全程 0.08%/1.65% 仅报告、partition 全判据绿、stall 采样 K=8 STALL=0/RECOVERED=8/ABORTED=0）+ 复核改判：**维持 ⚠️**（形态 B 概率残余不因单轮零命中消除，跟踪口径不变），**"未执行"状态清除**（复跑承诺由本实录兑现，常态回归转 `ci-github-actions` drill job）。
- Phase 3 验收报告"未执行项"条目追加带日期关闭注记（原文保留，不改写历史），并勘正"无 TTY 沙箱不可靠"归因。
- 原始 dated 报告不入库（遵"过时报告不入库"定夺），证据副本留档 `/tmp/drill-evidence-2026-09-12/formal-b35f549/`；常态证据通道自 CI drill job 起以 run artifacts 承载。
- 无规格 delta（纯验收记录承载，行为与夹具变更已由两个前置 change 的规格承担）。

## Impact

- 文件：`docs/Phase2-验收报告.md`、`docs/Phase3-验收报告.md` 各追加一节/一条（仅追加，历史文字不动）。
- 关联 change：本变更归档时一并归档 `drill-metrics-port-collision-fix`、`drill-port-allocation-race-fix`（其规格增量 `fault-drill-harness`、`metrics-observability` 端口隔离要求同步主规格）。
- 签署位：复核改判定夺仍留评审人签署栏（沿第一轮格式）。
