# Kafka 交易事件合同

合同版本：`1.0.0`

Kafka 只作为 Flink 风控输入，只接收每日事实造数器在 MySQL 事务提交成功后发布的有限批次交易事件。项目不使用 CDC，也不发送行为、评分、维表变更、交易宽表或脏数据消息。

## Topic 配置

| Topic | Key | 分区 | 副本 | 保留 | `retention.ms` | 业务粒度 |
|---|---|---:|---:|---:|---:|---|
| `ods_order_info` | `order_id` | 3 | 2 | 7 天 | 604800000 | 一次订单创建 |
| `ods_order_detail` | `order_id` | 3 | 2 | 7 天 | 604800000 | 一条订单明细创建 |
| `ods_payment_info` | `order_id` | 3 | 2 | 7 天 | 604800000 | 一次支付尝试 |

三个 Topic 都使用 `cleanup.policy=delete`。Key 为十进制字符串形式的 `order_id`，Value 为 UTF-8 JSON。同一订单在单个 Topic 内稳定进入同一分区；Kafka 不保证同一订单跨 Topic 的到达顺序。

## 公共字段

每条消息均先包含以下公共字段：

| 字段 | JSON 类型 | 允许为空 | 说明 |
|---|---|---|---|
| `event_id` | string | 否 | 确定性且不可变；相同 `ds + base_seed + 冻结维度` 重跑时保持不变 |
| `event_type` | string | 否 | `ORDER_CREATED`、`ORDER_DETAIL`、`PAYMENT_SUCCESS`、`PAYMENT_FAILED` |
| `event_time` | string | 否 | EventTime 和 Watermark 的唯一时间依据，允许秒或毫秒精度 |
| `business_date` | string | 否 | `event_time` 对应的 `yyyyMMdd`，仅用于校验 |

时间字段统一使用 `yyyy-MM-dd HH:mm:ss[.SSS]` 并按 `Asia/Shanghai` 解释。金额使用 JSON number，由 Flink 精确映射为 `DECIMAL(20,2)`；禁止先转换为 Double。

字段顺序为四个公共字段在前、Topic 业务字段在后。JSON 对象语义不依赖字段顺序，但生产端和样例保持该顺序以便审阅及校验。

## `ods_order_info`

- `event_type` 固定为 `ORDER_CREATED`。
- `event_time` 等于 `create_time`。
- 一条消息代表一次订单创建，不发送后续订单状态版本。

| 业务字段 | JSON 类型 | 允许为空 | 说明 |
|---|---|---|---|
| `order_id` | integer | 否 | 订单唯一标识，同时作为 Kafka Key |
| `user_id` | integer | 否 | 下单用户标识 |
| `shop_id` | integer | 否 | 订单所属店铺 |
| `order_amount` | number | 否 | 订单金额，等于明细 `final_amount` 合计的正常值 |
| `create_time` | string | 否 | 订单创建业务时间 |

```json
{"event_id":"20260825:ORDER_CREATED:100001","event_type":"ORDER_CREATED","event_time":"2026-08-25 09:15:20.123","business_date":"20260825","order_id":100001,"user_id":20001,"shop_id":301,"order_amount":299.00,"create_time":"2026-08-25 09:15:20.123"}
```

## `ods_order_detail`

- `event_type` 固定为 `ORDER_DETAIL`。
- `event_time` 等于 `create_time`。
- 一条消息代表一条不可变订单明细，以 `order_detail_id` 作为业务去重键。

| 业务字段 | JSON 类型 | 允许为空 | 说明 |
|---|---|---|---|
| `order_detail_id` | integer | 否 | 订单明细唯一标识 |
| `order_id` | integer | 否 | 所属订单，同时作为 Kafka Key |
| `sku_id` | integer | 否 | 成交 SKU 标识 |
| `sku_num` | integer | 否 | 购买数量，大于 0 |
| `original_amount` | number | 否 | 优惠前明细金额 |
| `final_amount` | number | 否 | 优惠后的明细成交金额 |
| `create_time` | string | 否 | 明细创建业务时间 |

```json
{"event_id":"20260825:ORDER_DETAIL:500001","event_type":"ORDER_DETAIL","event_time":"2026-08-25 09:15:20.223","business_date":"20260825","order_detail_id":500001,"order_id":100001,"sku_id":8001,"sku_num":1,"original_amount":319.00,"final_amount":299.00,"create_time":"2026-08-25 09:15:20.223"}
```

## `ods_payment_info`

- 支付成功时 `event_type=PAYMENT_SUCCESS`，失败时 `event_type=PAYMENT_FAILED`。
- `event_time` 等于 `payment_time`。
- 一条消息代表一次支付尝试，同一订单允许多条失败尝试，但最多一条有效成功支付。

| 业务字段 | JSON 类型 | 允许为空 | 说明 |
|---|---|---|---|
| `payment_id` | integer | 否 | 支付尝试唯一标识 |
| `order_id` | integer | 否 | 支付对应订单，同时作为 Kafka Key |
| `user_id` | integer | 否 | 支付用户标识 |
| `payment_status` | string | 否 | `SUCCESS` 或 `FAILED`，必须与 `event_type` 一致 |
| `payment_amount` | number | 否 | 本次支付金额 |
| `payment_time` | string | 否 | 支付尝试业务时间 |

```json
{"event_id":"20260825:PAYMENT_SUCCESS:700001","event_type":"PAYMENT_SUCCESS","event_time":"2026-08-25 09:18:04.456","business_date":"20260825","payment_id":700001,"order_id":100001,"user_id":20001,"payment_status":"SUCCESS","payment_amount":299.00,"payment_time":"2026-08-25 09:18:04.456"}
```

## 发布约束

1. 只有 `2_generate_daily_facts.py --publish` 生产消息；MySQL 事务必须先完整提交。
2. 发布前按 `event_time` 全局归并；相同时间点按订单创建、订单明细、支付尝试排序。
3. 允许受控注入不超过 2 分钟的少量跨 Topic 乱序，不故意遗漏同批订单创建事件。
4. Producer 设置 `enable.idempotence=true`、`acks=all`、`retries>0`，发送完成后必须 `flush` 成功再退出。
5. MySQL 已提交但 Kafka 发布失败时，使用相同 `ds + seed + 冻结维度` 重新执行 `--publish`，重发相同 `event_id` 集合。
6. `scenario_tag` 只写造数器日志和断言，不得进入 Kafka 消息。
7. Flink 按 `event_id` 去重，使用 2 分钟 Watermark 和 30 秒空闲分区检测；超过乱序范围的事件只记录日志和指标。

## Linux 集群创建

脚本只创建 Topic，不生产或消费消息，应在能够访问 Kafka 集群的节点执行：

```bash
export KAFKA_HOME=此处自定义
export KAFKA_BOOTSTRAP_SERVERS=此处自定义
bash kafka/create-topics.sh
```

创建后使用 `kafka-topics.sh --describe` 核对三个 Topic 的 3 分区、2 副本和 7 天保留时间。
