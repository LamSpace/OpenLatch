# 04 · Spring Boot Starter

## Scope and version

`openlatch-spring-boot-starter` targets Spring Boot **4.x**: auto-configured
`OpenLatchClient` plus the `@OpenLatch` declarative lock. Boot 3.x is not compatible
(the starter depends on Boot-4-only artifacts); Boot 3 apps should wire
[03 the client SDK](03-client-sdk.md) manually.

## Three steps

**① Dependency**

```xml
<dependency>
    <groupId>io.github.lamspace</groupId>
    <artifactId>openlatch-spring-boot-starter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

**② Enable `-parameters` in the build (required)** — SpEL keys like `#orderId` need
method-parameter names:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <parameters>true</parameters>
    </configuration>
</plugin>
```

**③ Annotate**

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

## Annotation attributes

| Attribute | Default | Semantics |
|---|---|---|
| `key` | required | SpEL evaluated against method args (`#paramName` / `#p0`) into the lock key |
| `type` | `REENTRANT` | `REENTRANT` / `SIMPLE` / `READ_WRITE` / `FAIR` |
| `waitTime` | `-1` | `<0` queue (capped by the total budget); `=0` immediate; `>0` bounded wait |
| `leaseTime` | `0` | `0` server-default lease with watchdog; `>0` requested lease (server-clamped), also watchdog-protected |
| `timeUnit` | seconds | unit for `waitTime`/`leaseTime` |

Acquisition failure throws `OpenLatchException` (carrying the protocol status), mirroring
SDK failure semantics.

## Configuration keys (`application.yaml`)

| Property | Default | Notes |
|---|---|---|
| `openlatch.enabled` | `true` | `false` makes the annotation inert (client bean still created) |
| `openlatch.server-host` / `server-port` | `127.0.0.1` / `9410` | server address |
| `openlatch.request-timeout` | `5s` | Duration |
| `openlatch.default-wait-timeout` | `30s` | budget for `waitTime<0` |
| `openlatch.reconnect-initial-backoff` / `reconnect-max-backoff` | `200ms` / `10s` | backoff |
| `openlatch.tls-enabled` | `false` | TLS |
| `openlatch.tls-trust-store` | — | PEM CA |
| `openlatch.tls-client-cert` / `tls-client-key` | — | mTLS pair |
| `openlatch.auth-token` | — | business token (required when server auth is on) |

## Behavioral semantics (four, important)

1. **Lock outside the transaction**: with `@OpenLatch` + `@Transactional` on one method the
   aspect runs first — acquire → begin tx → commit/rollback → release. No window where the
   lock is gone but the transaction is uncommitted.
2. **Self-invocation bypasses the proxy**: `this.method()` is not locked — the standard AOP
   limitation, same as `@Transactional`; call through the injected proxy or split beans.
3. **Graceful shutdown**: on context close the client best-effort releases held locks and
   shuts down (process-crash cases fall back to leases).
4. **Failed acquisition throws**: the annotated method body does not run; for
   "do something else if not acquired" use the SDK `tryLock` form directly.

## Debugging entry points

- Annotation inert: nine times out of ten `-parameters` missing or self-invocation — [09](09-troubleshooting.md);
- No server at startup: bean creation does not block startup; connection failures surface on first call.
