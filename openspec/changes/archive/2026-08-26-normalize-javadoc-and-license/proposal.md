# Proposal: normalize-javadoc-and-license

## Why

对全部 48 个 Java 文件的逐一审核发现：类级注释完整且质量高，但方法级文档严重缺失——主源码中 `@param` 仅 6 处、`@return` 与 `@throws` 为 0，而带参/带返回值的 public 方法有 60+ 个；record 组件与枚举常量文档不完整；核心门面方法（`CoreEngine.acquire/release/renew`）甚至完全没有 Javadoc。此外，所有 Java 源文件均缺少 Apache License 2.0 开源协议头，构建中也没有任何 Javadoc 校验机制（无 maven-javadoc-plugin / checkstyle），缺口会持续扩大。

## What Changes

- 补齐 `openlatch-core` 主源码（20 个文件）的 Javadoc：方法 `@param`/`@return`/`@throws`、record 组件 `@param`、枚举常量、public 常量、缺失的类级注释
- 补齐 `openlatch-server` 主源码（10 个文件）的 Javadoc，同上
- 测试代码（18 个文件）只要求类级注释；公共测试夹具（`TestSupport`、`TestServers`、`TestProtocolClient`）额外补齐方法标签
- 为全部手写 Java 源文件（main + test，不含 protobuf 生成代码）添加 Apache License 2.0 协议头
- 根 `pom.xml` 引入 `maven-javadoc-plugin`：绑定 `verify` 阶段、`failOnWarnings` 失败构建、排除 protobuf 生成代码，防止文档规范回归
- **不改变任何运行时行为**：纯注释与构建工具变更，字节码语义、协议、API 均不变

## Capabilities

### New Capabilities

无。

### Modified Capabilities

无。本变更为纯文档与构建工具整改，不涉及规格级行为变化（已在 `.openspec.yaml` 声明 `skip_specs: true`）。

## Impact

- **代码**：约 48 个 Java 文件（`openlatch-core` 27 个、`openlatch-server` 21 个、`openlatch-protocol` 测试 1 个）；`openlatch-client` / `openlatch-spring-boot-starter` / `openlatch-examples` 模块当前无 Java 文件，不涉及
- **构建**：根 `pom.xml` 的 `pluginManagement` 与 build 配置（新增 javadoc 插件）
- **生成代码**：protobuf 生成的 `io.github.lamspace.openlatch.protocol.*` 类不在整改范围，插件需排除
- **验证**：`mvn -s /home/lam/repo/settings.xml clean verify` 需在 javadoc 校验开启后全绿
