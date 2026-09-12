# OpenLatch 用户指南（简体中文）

面向使用者与运维者的完整学习路径。英文版见 [../en/index.md](../en/index.md)。

| 篇章 | 内容 |
|---|---|
| [00 简介与架构](00-introduction.md) | OpenLatch 是什么、组件全景、端口地图 |
| [01 核心概念](01-concepts.md) | 租约、看门狗、失锁、等待-通知-重发、FIFO 公平、超时纪律 |
| [02 快速上手](02-quickstart.md) | 十分钟跑通单机与三节点集群 |
| [03 客户端 SDK](03-client-sdk.md) | 各类锁的 API、语义边界、失锁处理、异步用法 |
| [04 Spring Boot Starter](04-spring-boot-starter.md) | `@OpenLatch` 声明式锁、SpEL、事务边界 |
| [05 集群部署与运维](05-cluster-deployment.md) | 拓扑、配置键、Leader 发现、扩缩容、重启与 supervisor |
| [06 安全](06-security.md) | TLS / mTLS、业务令牌与轮换、边界声明 |
| [07 管理控制台](07-admin-console.md) | 只读观察面的部署与配置 |
| [08 可观测性](08-observability.md) | 指标清单、/metrics 与 /healthz、客户端指标 |
| [09 故障排查与 FAQ](09-troubleshooting.md) | 错误码语义、恢复窗口、停摆自愈日志、常见误配 |
| [10 兼容性与协议版本](10-compatibility.md) | Java / Spring Boot / 协议支持矩阵与升级回滚 |
| [附录 · 术语表](appendix-glossary.md) | 中英术语对照 |

> 阅读建议：第一次接触按 00 → 01 → 02 顺序；已有单机经验要上集群直接看 05；
> 排障从 [09](09-troubleshooting.md) 的错误码表进入。
