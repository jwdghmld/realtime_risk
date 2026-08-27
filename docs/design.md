# 实时风控代码级设计

本文只补充 README 未展开的实现约束。总体架构、规则口径、结果截图和仓库目录见仓库首页。

## 类职责

| 类 | 职责 |
|---|---|
| `RealtimeConfig` | 统一读取 Kafka、MySQL 和 Checkpoint 环境变量，并为两个作业生成独立配置 |
| `RiskEvent` | 统一承载订单、明细和支付解析后的业务字段 |
| `EventParsers` | 将 Flink SQL Row 严格转换为 `RiskEvent`，校验事件类型、日期、时间和金额 |
| `TimeUtils` | 统一 `Asia/Shanghai` 时间解析、日期转换和格式化 |
| `OrderRiskJob` | 维护订单键状态，等待跨 Topic 事件并执行四条订单规则 |
| `UserRiskJob` | 维护用户键状态，计算两个滑动窗口规则和冷却时间 |

## API 分工

Flink SQL 只负责声明 Kafka Source 与 JDBC Sink，使字段合同和连接器参数集中可读。Table 转为 DataStream 后再完成 Watermark、`keyBy`、Keyed State、Timer 和纯规则函数；告警最终转回 Row 并通过临时视图写入 JDBC Sink。

该边界避免用 SQL Join 直接处理跨 Topic 长时间乱序，也保留了 Table Connector 的声明式配置能力。

## 订单状态机

`OrderRiskProcessFunction` 按 `order_id` 保存一个订单快照、多个明细、多个支付尝试和已处理事件集合。只有成功支付注册 `payment_time + 2min` 的 Timer；失败支付保留在输入合同中，但不会触发订单金额判断。

Timer 触发时按业务键找到对应成功支付，累计全部明细成交金额，调用无状态 `evaluate` 函数生成零到多条违规结果。`evaluate` 不访问 Flink Runtime，便于直接单元测试。完成判断后清除订单、明细和支付状态，去重状态由 TTL 回收。

## 用户窗口实现

`UserRiskProcessFunction` 没有使用窗口算子，而是显式计算事件所属的全部窗口结束点并注册 Timer：

```text
窗口结束点 = floor(event_time / slide) * slide + slide + n * slide
窗口数量   = window_size / slide
```

未支付窗口为 60 秒，窗口结束后延迟 120 秒判断；失败支付窗口为 180 秒，直接在窗口结束点判断。两类窗口都以 10 秒滑动，并分别保存已评估窗口与冷却结束时间，防止重复 Timer 和相邻窗口告警风暴。

## 去重与主键

输入去重键为合同中的稳定 `event_id`。输出 `alert_id` 由风险等级前缀、业务日期和稳定业务键的 SHA-256 摘要生成 12 位编号：

```text
订单告警：level + yyyyMMdd + hash(order_id, payment_id, rule_code)
用户告警：H     + yyyyMMdd + hash(user_id, window_end, rule_code)
```

主键只依赖业务事实，不依赖 TaskManager、并行子任务或处理时间。重复消息与恢复重放因此会命中相同 MySQL 主键。

## 状态生命周期

- 订单状态 TTL 为 2 小时，用户状态 TTL 为 10 分钟。
- TTL 在创建和写入时刷新，过期状态不可见。
- 业务计算完成后优先主动清理，TTL 作为异常路径和残留状态的兜底。
- 迟于当前 Watermark 的事件增加 `late_events` 指标并记录业务标识。

## 测试边界

单元测试覆盖订单四类违规判断、用户窗口归属和计数逻辑，以及时区和日期转换。Kafka Connector、Checkpoint 恢复与 MySQL JDBC 行为属于集成验证范围，应在与目标集群版本一致的环境中执行。

## 一致性声明

Checkpoint 对 Kafka offset、Flink 状态和 Timer 提供一致恢复点。MySQL 使用稳定主键 Upsert 收敛重复输出，但 JDBC Sink 未启用 XA，因此故障窗口内允许同一结果重复写入，不允许宣传为严格端到端 Exactly-Once。
