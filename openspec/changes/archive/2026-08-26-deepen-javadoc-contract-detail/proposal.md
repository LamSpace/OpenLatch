# Proposal: deepen-javadoc-contract-detail

## Why

上次变更（`2026-08-26-normalize-javadoc-and-license`，已归档）完成了注释的**覆盖度**：全部 48 个文件的 `@param`/`@return`/`@throws` 标签、Apache 协议头与 javadoc 构建校验均已就位。但以 JDK `java.util.concurrent.locks.Lock` 接口的契约级注释（约 150 行：内存语义、调度、重入、中断/超时获取契约、示例）为基准重新逐一审核 30 个主源文件后发现**深度**缺口，且深度与契约分量不成正比：

- 4 个契约类不达标：`CoreEngine`（本项目门面，类级注释仅 2 行，线程模型、校验顺序、惰性到期契约缺失）、`LockEntry`（`acquire` 把规则集外包给"设计说明书 §4.3"，重入刷新/读锁共享凭证/队首幂等重发只在内联注释）、`LockType`（兼容性与重入语义未写）、`ServerSessionHandler`（连接生命周期状态机与处理矩阵只在内联注释）
- 3 个部分达标：`Outcome`/`ReleaseStatus`（常量缺"何时、以什么优先级返回"）、`OpenLatchServer`（缺线程模型）
- **17 个 private 方法与 1 个 private 构造器完全没有 Javadoc**（`clampLease`、`handleHandshake`、`notifyHeadIfPossible`、`validate` 等），承载夹取、握手门闩、队首通知、配置校验等关键行为
- 构建防回归的 javadoc 插件未设 `<show>`，默认 `-protected`，private 成员注释完全在校验范围之外

## What Changes

- 全部 48 个 Java 文件（main 30 + test 18）**逐一**按契约分量分级复核：契约类按 JDK Lock 接口级深化，简单类确认维持 JDK 简洁档位并记录判定理由
- 深度重写 4 个契约类：`CoreEngine`、`LockEntry`、`LockType`、`ServerSessionHandler`（类级：职责/线程模型/状态机/契约边界；方法级：分支语义、判定顺序、幂等性、调用者义务）
- 补充 3 个部分达标文件的契约段落：`Outcome`、`ReleaseStatus`、`OpenLatchServer`
- 为全部 17 个 private 方法与 1 个 private 构造器补写详细 Javadoc（行为意图、参数/返回语义、副作用、线程与锁上下文）
- 根 `pom.xml` 的 maven-javadoc-plugin 增加 `<show>private</show>`，private 成员注释纳入构建期 doclint 校验
- **契约自足**：规则本身写进 Javadoc，读懂注释即懂契约，不再依赖设计说明书；设计文档引用降级为"详见 §x.y"深入指引
- **不改变任何运行时行为**：纯注释与一处构建配置变更，字节码语义、协议、API 均不变
- 明确推翻上次变更的非目标"不重写措辞正确的类级注释"（仅限本次深化的 7 个文件）

## Capabilities

### New Capabilities

无。

### Modified Capabilities

无。本变更为纯文档与构建配置整改，不涉及规格级行为变化（已在 `.openspec.yaml` 声明 `skip_specs: true`）。

## Impact

- **代码**：7 个主源文件实质性深化（`CoreEngine`、`LockEntry`、`LockType`、`ServerSessionHandler`、`Outcome`、`ReleaseStatus`、`OpenLatchServer`）；10 个文件补 private 成员注释（含前述重叠）；**实施修正**：`doclint=all` 对 `show=private` 范围强制要求注释存在，private 字段与常量亦须一句话注释（见 design D3）；其余 23 个主源文件仅逐一确认并记录判定；18 个测试文件维持上轮标准不动
- **构建**：根 `pom.xml` 增加 `<show>private</show>` 一行配置
- **验证**：`mvn -s /home/lam/repo/settings.xml clean verify` 全程全绿（测试通过 + javadoc 零告警）
