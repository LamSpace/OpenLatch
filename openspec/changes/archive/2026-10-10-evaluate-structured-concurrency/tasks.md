# Tasks

## 1. 治理登记

- [x] 1.1 `WATCHLIST.md` 新增 **W16** 行：现状与结论=库内无结构化并发落点（R1 空集）+ `StructuredTaskScope` 预览轨迹 + `--enable-preview` 禁令无例外；触发 → 动作=`StructuredTaskScope` 转正（JEP final 且 API 连续 ≥2 发布稳定）→ 另立 change 重勘察 R1 落点、评估 release 25→27 兼容代价、按第三项模式落地，且落地须对账第三项 `EntryClock` 边界矩阵绊线；记录位置指向本 change `design.md` 与 ROADMAP 决策行——**验证**：行落盘，列结构与既有 W 行一致，"只增删行、不写长文"规约满足
- [x] 1.2 `ROADMAP.md` "决策记录"节补 **2026-10-10** 行（工程改进第四项评估裁决：R1 空集、触发器、基线代价、禁令无例外、与第三项耦合）——**验证**：决策行落盘于"决策记录"节且措辞与 proposal/design 一致；**本条不动工程改进表格行**（其物理退役归后续独立治理 change）

## 2. 证据留档

- [x] 2.1 落 `evidence/jdk-preview-status.md`：记录本机 JDK 25.0.3 `src.zip` 中 `java.util.concurrent.StructuredTaskScope` 仍标注 `@PreviewFeature` 的核验命令与片段，并附预览轨迹（JEP 505 第五预览 / JEP 525 第六预览）出处——**验证**：文件含可复现命令与来源链接，供未来触发时复核

## 3. 收口自查

- [x] 3.1 运行 `bash scripts/check-source-citations.sh`——**验证**：退出 0 / 零命中（本 change 不触对外源码与发布元数据，按落地纪律第 6 条自查）
- [x] 3.2 三处口径交叉核对：W16 触发器措辞 ↔ design D2/D5 ↔ ROADMAP 决策行——**验证**：proposal / design / 治理文件三处表述无冲突
