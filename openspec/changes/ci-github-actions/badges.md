# Badge 契约（README 写入由 docs-restructure-and-report-retirement 承载，或本变更在其后落地时直接补行）

workflow 定名即 URL 契约：`build.yml` / `drill.yml` / `benchmark.yml`（仓库 `LamSpace/OpenLatch`）。

```markdown
[![build](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml)
[![drill](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml?query=branch%3Amaster)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-25-orange)](README.md)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-green)](README.md)
[![Protocol](https://img.shields.io/badge/protocol-v3-lightgrey)](docs/)
```

注：
- build badge 反映 PR 门禁状态；drill badge 反映 master 最近一次 nightly/手动 run 结论，run 红不阻断合并、仅供运维观察（与规格一致）。
- 仓库为 public，badge 对所有人可见，无需授权。
- Maven Central 版本 badge 待发布时补（发布不在本变更范围）。
