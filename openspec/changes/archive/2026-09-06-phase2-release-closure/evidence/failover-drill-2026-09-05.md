# 杀 Leader 演练报告（s3 P2-14）

- 生成：3 节点本机 shaded jar，election-timeout 800ms

## 场景 B：kill -9 单 Follower（节点1，初始 Leader：节点2）

| 指标 | 值 | 判定 |
|---|---|---|
| 杀 Follower 后 20 轮获取+释放 | 236 ms 全成功 | 多数派满足、无感 ✅ |

## 场景 A：kill -9 当值 Leader（初始 Leader：节点2）

| 指标 | 值 | 判定 |
|---|---|---|
| kill → 首次成功授予 | 1587 ms | < 10000 ms ✅ |
| 死主会话失锁回调 | 触发 | 触发 ✅ |
| 停载后同键全新授予 | 成功 | 无泄漏 ✅ |

