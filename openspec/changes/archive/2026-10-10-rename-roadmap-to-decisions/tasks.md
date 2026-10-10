# Tasks

## 1. 文件改名与内容重定位

- [x] 1.1 `git mv ROADMAP.md DECISIONS.md`——**验证**：`git status` 显示 `renamed: ROADMAP.md -> DECISIONS.md`（历史保留，非删除+新建）
- [x] 1.2 编辑 `DECISIONS.md` 文件头：标题 → `# 决策账本(Decision Ledger)`；导语重写（账本定位 + 与 `WATCHLIST.md` 分工）；"最后更新" → 2026-10-10——**验证**：文件头不再自称"演进路线/本表"
- [x] 1.3 删除"状态图例"节与"一档/二档/三档/工程改进"四表；保留决策记录、不做清单、每原语落地纪律——**验证**：`grep -nE "^## (状态图例|一档|二档|三档|工程改进)" DECISIONS.md` 零命中
- [x] 1.4 重写"维护规约"节为账本维护规约（追加式 / 定位边界变更先补决策行 / 与 WATCHLIST 分工 / 不写长文）——**验证**：无"更新本表 / 再动表格 / 本表 = 主动方向"等随表移除而失效的措辞

## 2. 活引用改写

- [x] 2.1 `WATCHLIST.md` W5 两处（现状列 + 记录位置列）："ROADMAP 决策记录 2026-10-01" → "DECISIONS.md 决策记录 2026-10-01"——**验证**：`grep -c ROADMAP WATCHLIST.md` 归零
- [x] 2.2 确认归档 change 的历史引用保持不动——**验证**：`git status` 未列出任何 `openspec/changes/archive/**` 文件

## 3. 规格 delta 与收口自查

- [x] 3.1 delta `specs/replicated-state-machine/spec.md` 含四条 MODIFIED 守卫的**全量**文本（编号项 + 全部 `#### Scenario` 逐字保留），仅把引用目标"ROADMAP 决策记录"更名"DECISIONS.md 决策记录"——**验证**：四条 `### Requirement:` 头与主规格逐字一致，场景数与主规格相等
- [x] 3.2 `openspec validate rename-roadmap-to-decisions --strict` 通过——**验证**：输出 valid（主规格同步在**归档时**由 `openspec archive` 执行，含 `sync --strict` + delta 段头残留 grep 双查）
- [x] 3.3 `bash scripts/check-source-citations.sh`——**验证**：exit 0 零命中
