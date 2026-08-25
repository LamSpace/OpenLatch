# Design: normalize-javadoc-and-license

## Context

根 `pom.xml` 已声明 Apache 2.0 许可（根目录存在 `LICENSE` 文件），但 48 个手写 Java 文件均无协议头；注释现状与问题分类见 proposal.md。关键约束：

- JDK 25（`maven.compiler.release=25`），Maven 构建，统一使用 `-s /home/lam/repo/settings.xml`
- `openlatch-protocol` 模块的主源码由 `protobuf-maven-plugin` 从 `src/main/proto/openlatch.proto` 生成，不可手改
- 现有注释语言为中文，`{@code}`/`{@link}`/`<p>` 用法正确，无需纠正格式，只需补齐覆盖度
- 测试类均为包私有，测试方法名自描述

## Goals / Non-Goals

**Goals:**

- 全部手写 Java 文件带 Apache License 2.0 协议头
- `src/main` 下 public API 文档完整：类、方法 `@param`/`@return`/`@throws`、record 组件、枚举常量、public 常量
- 构建期强制校验：`mvn verify` 时 javadoc 告警即失败
- 零行为变化：不改动任何代码逻辑

**Non-Goals:**

- 不引入 checkstyle、license-maven-plugin 等其他风格工具（单一机制，够用即止）
- 不整改 protobuf 生成代码
- 不重写现有措辞正确的类级注释（只补缺，不润色）
- 不强制测试方法的 `@param`/`@return`（决策点 2 已确认）

## Decisions

### D1｜协议头格式：Apache 2.0 短式样板，置于 package 语句之前

```java
/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

- 用 `/*`（非 `/**`）开头，避免被 javadoc 工具误解析为文档注释
- 版权人采用 Apache 附录标准样板 `the original author or authors`（Spring 等主流项目同款）；如需改为具体名称，全局替换一行即可
- 替代方案（否决）：每个文件写全量 200 行 Apache 文本——冗余且无人这样做，短式样板 + 根 `LICENSE` 文件是标准做法

### D2｜文档标准分级：main 严格 / test 宽松 / 生成代码豁免

| 层级 | 要求 |
|---|---|
| `src/main` public 类型 | 必须有类级 Javadoc |
| `src/main` public/protected 方法 | Javadoc + 完整 `@param`/`@return`/`@throws` |
| `src/main` record 组件 / 枚举常量 / public 常量 | 逐组件 `@param` / 逐常量注释 |
| `src/main` 包私有成员 | 不强制，保留现有单行注释风格 |
| `@Override` 方法 | 不强制补写（继承父类/接口文档） |
| 测试类 | 类级注释 + 公共夹具（`TestSupport`/`TestServers`/`TestProtocolClient`）的方法标签 |

依据：javadoc 插件只扫描 `src/main`，测试标准靠本约定与评审维持；对包私有与 `@Override` 强加标签只产生噪音。

### D3｜注释语言与措辞：保持中文，先读懂代码再写标签

- 与存量注释保持一致，全部中文描述
- `@param` 说明语义与约束（如"会话必须已登记"），不复述参数名；`@return` 说明各分支含义（对齐 `Outcome`/`ReleaseStatus` 枚举）
- 替代方案（否决）：英文注释——与存量割裂，翻译成本高且无收益

### D4｜防回归机制：maven-javadoc-plugin 绑定 verify

根 `pom.xml` 配置（`pluginManagement` 管版本与参数，`build.plugins` 启用）：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-javadoc-plugin</artifactId>
    <version>3.11.2</version>  <!-- 如与 JDK 25 不兼容则上调至最新稳定版 -->
    <configuration>
        <doclint>all</doclint>
        <failOnWarnings>true</failOnWarnings>
        <quiet>true</quiet>
        <encoding>UTF-8</encoding>
        <charset>UTF-8</charset>
        <docencoding>UTF-8</docencoding>
    </configuration>
    <executions>
        <execution>
            <goals><goal>javadoc</goal></goals>  <!-- 只校验，不产 jar -->
        </execution>
    </executions>
</plugin>
```

- 目标用 `javadoc`（校验+报告）而非 `jar`，不改变打包产物
- `openlatch-protocol` 模块在其 `pom.xml` 中对该插件设 `<skip>true</skip>`：主源码是生成代码，无校验价值
- 替代方案（否决）：checkstyle 的 Javadoc 检查——规则表达力弱于 doclint，且多引一个工具

### D5｜实施顺序：先协议头 → 再注释 → 最后启用插件

1. 全部文件补协议头（机械操作，先行可让后续 diff 干净）
2. core 主源码注释 → 3. server 主源码注释 → 4. 测试类注释
5. 最后一步才引入并启用 javadoc 插件（避免中途构建被卡），以 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿收尾

## Risks / Trade-offs

- [注释写错语义：整改者对方法行为理解偏差导致 `@param`/`@return` 描述失真] → 写标签前必须读方法实现；对 `Outcome`/`ReleaseStatus` 分支逐条核对；评审时以代码语义为准
- [doclint 对存量注释报出未预料的告警（如 HTML 片段）] → 插件启用前先单独跑 `javadoc:javadoc` 摸底，存量问题与补齐工作一并修掉
- [插件版本与 JDK 25 不兼容] → 3.11.2 起步，不兼容即上调最新稳定版；protocol 模块已 skip，爆炸半径仅限文档构建
- [约 150 处注释的单次变更量大，评审困难] → tasks 按模块拆分提交，每个模块独立 `verify`，可分次合入
- [中文注释在某些外部工具链（如翻译管线）中不便] → 项目既定风格为中文，维持一致性优先
