## ADDED Requirements

### Requirement: 注解 type 新增 FAIR 取值

`@OpenLatch` 注解的 `type` SHALL 新增 `FAIR` 取值：映射为公平可重入互斥语义（与 `REENTRANT` 行为等价并携带服务端公平承诺），并发互斥与排队行为遵循服务端语义。`SEMAPHORE`/`LATCH` 无声明式方法拦截语义，MUST NOT 引入为注解取值。

#### Scenario: FAIR 注解方法互斥执行

- **WHEN** 两个线程同时进入标注 `type = FAIR` 同 key 的方法
- **THEN** 串行执行且放行顺序等于进入等待的顺序，行为与 `REENTRANT` 标注一致
