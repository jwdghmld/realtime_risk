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

## 精确一次状态恢复

两个作业均以 `CheckpointingMode.EXACTLY_ONCE` 每 60 秒生成恢复点。Kafka Source 的消费位置、Keyed State、去重集合和 EventTime Timer 属于同一个 Checkpoint；只有完整成功的 Checkpoint 才成为新的恢复基线。

```text
Checkpoint N
├── Kafka Offset
├── 订单或用户业务状态
├── processed-events 去重状态
├── 窗口与冷却标记
└── 尚未触发的 EventTime Timer
```

发生故障时，作业不会分别恢复这些对象，而是从同一个成功 Checkpoint 整体恢复。这保证消费位置与规则上下文同步回退，避免 Offset 已确认但业务状态丢失。

## 输出提交边界

JDBC Sink 不参与 Flink Checkpoint 的 XA 两阶段提交。存在以下故障窗口：MySQL 已成功写入告警，但对应 Checkpoint 尚未成功，随后作业失败。恢复后相同输入可能再次触发相同告警。

项目通过三层机制收敛该重复：

1. `event_id` 在 Flink 状态中控制同一恢复周期内的输入重复。
2. `alert_id` 由稳定业务键生成，恢复重放不会改变结果主键。
3. MySQL 以 `alert_id` 为主键执行 Upsert，重复提交更新原记录。

因此准确语义为：Kafka 到 Flink 状态是 **Exactly-Once State Recovery**，Flink 到 MySQL 是 **Idempotent Effectively-Once**。除非将 Sink 替换为支持两阶段提交的事务型实现，否则不应宣传为 Kafka 到 MySQL 的严格端到端 Exactly-Once。

## 故障窗口

| 故障点 | Offset | 状态与 Timer | MySQL 结果 |
|---|---|---|---|
| Checkpoint 前失败 | 回退 | 回退 | 未写入或由稳定主键收敛 |
| Checkpoint 完成后失败 | 从新一致点恢复 | 从新一致点恢复 | 已确认结果保持 |
| JDBC 成功、Checkpoint 失败 | 回退后重放 | 回退后重算 | 可能重复 Upsert，但不新增重复主键 |
| 取消作业 | 取决于恢复入口 | 外部化 Checkpoint 保留 | 历史结果不删除 |
