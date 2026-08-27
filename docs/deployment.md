# 实时部署

## 配置

加载 `config/realtime-env.sh`，将“此处自定义”替换为实际值。Flink 作业读取以下变量：

```text
KAFKA_BOOTSTRAP_SERVERS
MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD
REALTIME_MYSQL_DATABASE
MYSQL_CHARSET / MYSQL_TIMEZONE
FLINK_CHECKPOINT_ROOT
```

## 初始化和构建

1. 执行 `contracts/mysql/03_realtime.sql`。
2. 在 Kafka 集群执行 `kafka/create-topics.sh`，确认三个 Topic 为 3 分区、2 副本、7 天保留。
3. 使用 JDK 17 在 `flink/` 目录执行 `mvn test package`。

## 集群提交

将打包 JAR 提交到 Flink Session 集群，分别指定：

```text
com.ecommerce.realtime.job.OrderRiskJob
com.ecommerce.realtime.job.UserRiskJob
```

生产集群建议 2 个 TaskManager、每个 3 个 Slot、默认并行度 3。两个作业分别使用 `order-risk` 和 `user-risk` 消费组及独立 Checkpoint 子目录。

## 故障恢复

运行故障由 RestartStrategy 从最近 Checkpoint 自动恢复；计划停机使用 Savepoint，恢复时必须传入精确的 Savepoint 路径。Kafka Topic 保留期必须覆盖最长停机和追数时间，否则状态存在也可能因历史消息过期产生数据缺口。

## 联调样例

`samples/events/` 提供订单、明细和支付 JSONL。可使用 Kafka 命令行 Producer 或现有数据生成器将样例写入对应 Topic；样例包含订单金额、时序和用户窗口风险场景。实时作业本身不接收 `ds` 参数。
