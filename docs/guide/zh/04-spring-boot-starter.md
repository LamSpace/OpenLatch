# 04 · Spring Boot Starter

## 定位与版本

`openlatch-spring-boot-starter` 为 Spring Boot **4.x** 应用提供自动装配的 `OpenLatchClient`
与声明式注解 `@OpenLatch`。Boot 3.x 不兼容（依赖 Boot 4 独有构件），Boot 3 工程请直接用
[03 客户端 SDK](03-client-sdk.md) 手动装配。

## 接入三步

**① 依赖**

```xml
<dependency>
    <groupId>io.github.lamspace</groupId>
    <artifactId>openlatch-spring-boot-starter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

**② 构建开启 `-parameters`（必做）**——`#orderId` 这类 SpEL 依赖方法参数名：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <parameters>true</parameters>
    </configuration>
</plugin>
```

**③ 注解**

```java
@Service
public class OrderService {

    @OpenLatch(key = "#orderId")
    public void createOrder(String orderId) { ... }

    @OpenLatch(key = "report", waitTime = 3, leaseTime = 60)
    @Transactional
    public Order settle(long report) { ... }
}
```

## 注解属性

| 属性 | 默认 | 语义 |
|---|---|---|
| `key` | 必填 | SpEL 表达式（可引用方法参数 `#p0`/`#参数名`、`#root` 上下文）求值为锁键 |
| `type` | `REENTRANT` | 锁类型：`REENTRANT` / `SIMPLE` / `READ_WRITE` / `FAIR` |
| `waitTime` | `-1` | `<0` 排队等待（受总超时兜底）；`=0` 立即式（拿不到直接失败）；`>0` 限时等待 |
| `leaseTime` | `0` | `0` 用服务端默认租约并接受看门狗续租；`>0` 按请求值（服务端钳制区间内），同样受看门狗保护 |
| `timeUnit` | 秒 | `waitTime`/`leaseTime` 的单位 |

获取失败抛 `OpenLatchException`（含协议状态码），与 SDK 的失败语义一一对应。

## 配置键（`application.yaml`）

| 属性 | 默认 | 说明 |
|---|---|---|
| `openlatch.enabled` | `true` | `false` 时注解完全失效（切面不注册；客户端 bean 仍创建） |
| `openlatch.server-host` / `server-port` | `127.0.0.1` / `9410` | 服务端地址 |
| `openlatch.request-timeout` | `5s` | 单请求超时（Duration） |
| `openlatch.default-wait-timeout` | `30s` | `waitTime<0` 时的总兜底 |
| `openlatch.reconnect-initial-backoff` / `reconnect-max-backoff` | `200ms` / `10s` | 重连退避 |
| `openlatch.tls-enabled` | `false` | 连接启用 TLS |
| `openlatch.tls-trust-store` | — | PEM CA 信任锚 |
| `openlatch.tls-client-cert` / `tls-client-key` | — | mTLS 客户端证书/私钥（成对） |
| `openlatch.auth-token` | — | 握手业务令牌（服务端开启认证时必填） |

## 行为语义（重要，共 4 条）

1. **锁在事务外层**：`@OpenLatch` + `@Transactional` 同方法时，切面先于事务通知执行——
   锁获取 → 事务开启 → 提交/回滚 → 锁释放。不会出现"锁已放、事务未提交"的窗口。
2. **自调用不加锁**：`this.method()` 不走代理，与 `@Transactional` 同款 AOP 局限；
   需要时对注入的自我引用（`@Lazy` self）或拆 bean。
3. **优雅停机**：Spring 容器关闭时客户端 bean 尽力归还所持锁后关闭（best-effort，
   不保证跨进程崩溃场景——那走租约兜底）。
4. **等待失败即异常**：注解方法拿不到锁按上表语义抛异常，方法体不执行；
   需要"拿不到就走别的分支"时请直接用 SDK 的 `tryLock` 形态。

## 排错入口

- 注解不生效：九成是没开 `-parameters`（SpEL 求值失败）或自调用——见 [09](09-troubleshooting.md)；
- 启动即连不上：客户端 bean 创建不阻塞启动，首次调用触发连接，失败为请求级异常。
