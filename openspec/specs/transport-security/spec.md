## Purpose

定义 OpenLatch 客户端接入通道的传输安全：服务端锁端口的可选 TLS/mTLS 接入与明文拒绝、业务令牌的 HELLO 认证（默认关闭 + 兼容守卫、多令牌轮换、常量时间比较、失败不泄露、单机/集群统一门闩、与管理令牌独立），以及客户端对 TLS 与认证的消费行为。加密范围仅客户端接入端口（业务与管理消息共用）；Raft 节点间通道与指标/控制台 HTTP 不在本能力范围。

## Requirements

### Requirement: 服务端 TLS 传输层

服务端客户端接入端口 MUST 支持可选的 TLS 传输层：`openlatch.server.tls.enabled` 默认 `false`，关闭时服务端行为与明文形态逐字节一致（无 TLS 状态机、无额外握手超时）。开启后，服务端 MUST 在该端口监听 pipeline 的首位置执行 TLS 握手，仅放行握手成功的连接进入既有帧编解码与会话路径；服务端证书以 PEM 文件（cert/key）提供。`require-client-cert` 默认 `false`；为 `true` 时（mTLS）MUST 要求客户端在握手期提交证书并以配置的 trust-store 校验，未提交或校验失败的客户端 MUST 被拒。TLS 开启后 MUST 拒绝明文连接：既非 TLS 握手的入站数据（明文协议字节）MUST 导致连接被断开；TLS 握手未在超时（默认 5s）内完成（含静默不发握手）的连接 MUST 被断开，不得悬挂至读空闲时限。TLS 握手/解码失败 MUST 不产生任何会话、注册表登记或业务副作用。TLS 配置非法（证书/密钥文件不可读或解析失败、`require-client-cert=true` 而未配 trust-store）MUST 在启动时快速失败并给出明确错误信息，不进入半启动（对齐端口占用启动失败语义）。证书更新以重启生效，MUST NOT 提供热加载。

#### Scenario: 默认关闭明文照常

- **WHEN** 未配置 `tls.enabled` 启动服务端，明文客户端正常 HELLO 并完成获取/释放
- **THEN** 行为与不引入本能力时逐字节一致，无握手超时、无 TLS 解码路径被触发

#### Scenario: 开启后明文连接被拒

- **WHEN** `tls.enabled=true` 启动服务端，明文协议客户端直接发送 HELLO 帧
- **THEN** 连接被断开，未收到任何握手应答，服务端无会话分配

#### Scenario: 正确证书通过

- **WHEN** TLS 客户端以服务端可验证的信任配置建立连接并完成握手
- **THEN** 握手成功，后续 HELLO 与业务请求在加密通道上正常完成

#### Scenario: 握手超时断开

- **WHEN** `tls.enabled=true`，客户端建立 TCP 连接后不发任何 TLS 握手字节
- **THEN** 连接在 5s 握手超时内被断开，未悬挂至 60s 读空闲时限

#### Scenario: mTLS 缺客户端证书被拒

- **WHEN** `require-client-cert=true` 且服务端已配置 trust-store，客户端不提交任何证书发起连接
- **THEN** TLS 握手失败，连接被断开，无会话副作用

#### Scenario: 坏证书配置启动失败

- **WHEN** `tls.enabled=true` 但 cert/key 指向不存在或不可解析的文件
- **THEN** 服务端启动失败并输出可定位的错误信息，已绑定资源被回收，不进入半启动

### Requirement: 业务令牌认证与默认兼容守卫

服务端 MUST 支持基于 `HelloRequest.auth_token` 的业务令牌认证，认证开关 `openlatch.server.auth.enabled` 默认 `false`。**关闭（默认）时维持 Phase 1 兼容守卫**：HELLO 携带非空 `auth_token` MUST 回以 `INVALID_REQUEST` 并断连；不携带令牌的合法客户端行为不受影响（既有一致性 §8-6）。**开启时**服务端 MUST 持 ≥1 个配置令牌（多令牌列表）；HELLO 的 `auth_token` 命中任一配置令牌即放行进入会话建立，否则（缺失、为空、不符，或开关开启而未配置任何令牌）MUST 统一回以 `INVALID_REQUEST` 并断连。认证失败应答 MUST NOT 携带任何可区分原因（不得区分"未配置/空/错"，防探测枚举），MUST 以常量时间比较完成校验（防时序侧信道），且 MUST 发生在任何会话分配、注册表登记或集群 `SESSION_OPEN` 复制**之前**——单机与集群装配共用同一握手门闩，未通过认证的 HELLO 不得产生任何可观察状态副作用（含复制日志条目与影子镜像变更）。会话生命周期内 MUST NOT 做逐请求鉴权（连接即身份，认证一次完成于握手）。业务令牌与 `ADMIN_*` 的逐消息管理令牌 MUST 相互独立：两者不得互替、不得借道，任一通道的令牌存在与否不得影响另一通道的判定。

#### Scenario: 默认关非空令牌仍被拒（兼容回归）

- **WHEN** 认证未开启，HELLO 携带非空 `auth_token`
- **THEN** 回以 `INVALID_REQUEST` 并断连，行为与 Phase 1 规则逐字节一致

#### Scenario: 默认关无令牌连接照常

- **WHEN** 认证未开启，v1/v2/v3 客户端以空 `auth_token` 完成 HELLO
- **THEN** 握手成功，既有客户端零改动

#### Scenario: 开启后正确令牌放行

- **WHEN** 认证开启，HELLO 携带与配置令牌之一完全一致的 `auth_token`
- **THEN** 握手成功并获得会话，后续业务照常

#### Scenario: 空令牌与错令牌同形拒绝

- **WHEN** 认证开启，分别以空 `auth_token` 与错误 `auth_token` 发起 HELLO
- **THEN** 两情形均回 `INVALID_REQUEST` 并断连，应答形态完全一致、无原因性字段，无法区分"空/错/未配置"

#### Scenario: 轮换期多令牌双活

- **WHEN** 认证开启且配置两个令牌（旧 + 新），分别以两令牌发起 HELLO
- **THEN** 两者均握手成功；服务端摘除旧令牌后，旧令牌 HELLO 被拒、新令牌照常

#### Scenario: 未认证 HELLO 零状态副作用

- **WHEN** 认证开启，集群档下以错误令牌发起 HELLO 并随即观测集群状态
- **THEN** 连接在 `INVALID_REQUEST` 后断开，无 `SESSION_OPEN` 复制条目、无注册表登记、影子镜像无变化

#### Scenario: 认证开启下管理令牌不可借道

- **WHEN** 认证开启，连接仅携带（合法的）逐消息管理令牌发起 HELLO（业务令牌缺失）
- **THEN** 按业务令牌判定拒绝并断连，管理令牌不构成 HELLO 放行依据

### Requirement: 客户端 TLS 与认证消费

`openlatch-client` MUST 支持可选的 TLS 与业务令牌配置，默认全部关闭（关闭时连接行为与明文无令牌形态逐字节一致）。配置开启后，客户端对**每一次**连接尝试（主连接建立、种子发现探针、断连重连）MUST 按同一配置执行 TLS 握手（mTLS 时提交客户端证书）并在 HELLO 携带配置的业务令牌；MUST NOT 存在任何绕过 TLS/认证的降级明文重试路径。TLS 握手失败（如 trust-store 无法验证服务端证书）或认证被拒（服务端对 HELLO 回 `INVALID_REQUEST`）时，本次连接 MUST 以失败收场：挂起的获取/释放/续租操作按"服务不可达"快速失败，重连按既有指数退避语义继续、MUST NOT 忙转；客户端 MAY 将"认证被拒"与普通网络断线区分呈现（诊断），但 MUST NOT 把认证被拒当作已建立会话处理。客户端对业务令牌 MUST NOT 打印或记录明文值。

#### Scenario: 默认关明文无令牌

- **WHEN** 未配置 TLS/令牌构建客户端并连接明文服务端
- **THEN** 连接与握手行为与现状一致，无 TLS 状态机、HELLO 令牌为空

#### Scenario: 正确 TLS 与令牌放行

- **WHEN** 客户端配置 TLS（trust-store 信任服务端）与正确的业务令牌，连接开启 TLS 的服务端
- **THEN** 握手成功，业务在加密通道完成，服务端认证放行

#### Scenario: 错误 trust-store 清晰失败

- **WHEN** 客户端 trust-store 不含服务端签发 CA，连接 TLS 服务端
- **THEN** TLS 握手失败，挂起操作以"服务不可达"快速失败，重连受既有退避约束、无忙转

#### Scenario: 种子发现探针附令牌

- **WHEN** 认证开启，客户端以配置令牌进行种子发现探针 HELLO
- **THEN** 探针携带令牌通过认证（不被当作未认证连接断开），种子发现正常完成

#### Scenario: 认证被拒不当作已建会话

- **WHEN** 客户端携带错误令牌连接认证开启的服务端，HELLO 被回 `INVALID_REQUEST`
- **THEN** 该连接不进入已建会话状态，相关操作以"服务不可达"失败，令牌明文不落日志

### Requirement: 常量时间比较

业务令牌校验 MUST 使用常量时间比较（时序与输入内容及长度无关）。对长度不等的输入，MUST 先补齐至等长再比较，MUST NOT 因长度不等提前返回（防令牌长度侧信道）。比较实现须单一收口，管理令牌（`admin-token`）校验与业务令牌校验 MUST 共用同一常量时间原语，保证两处语义永久一致。

#### Scenario: 长度不等仍常量时间

- **WHEN** 以任意长度的候选令牌对配置令牌做校验
- **THEN** 校验实现无长度不等早退分支（经代码评审断言，非运行时计时断言），行为与等长输入一致
