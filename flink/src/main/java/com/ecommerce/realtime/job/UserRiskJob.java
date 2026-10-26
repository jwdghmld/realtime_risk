package com.ecommerce.realtime.job;

import com.ecommerce.realtime.config.RealtimeConfig;
import com.ecommerce.realtime.model.RiskEvent;
import com.ecommerce.realtime.sql.EventParsers;
import com.ecommerce.realtime.util.TimeUtils;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用户实时风控作业。
 *
 * <p>消费订单和支付事件，按用户统计一分钟高频未支付与三分钟连续支付失败，
 * 结果写入 realtime.rt_user_risk_alert。
 */
public final class UserRiskJob {
    private static final int PARALLELISM = 3;
    private static final long SLIDE_MS = 10_000L;
    private static final long UNPAID_WINDOW_MS = 60_000L;
    private static final long UNPAID_WAIT_MS = 120_000L;
    private static final long FAILURE_WINDOW_MS = 180_000L;
    private static final long COOLDOWN_MS = 180_000L;
    private static final BigInteger ID_MODULUS = BigInteger.valueOf(1_000_000_000_000L);

    private UserRiskJob() {
    }

    public static void main(String[] args) throws Exception {
        RealtimeConfig config = RealtimeConfig.userRisk();
        // 使用当前 Flink 集群环境，由提交参数决定运行模式。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        // 开启 Exactly-Once Checkpoint，故障时从最近状态恢复。
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig conf = env.getCheckpointConfig();
        conf.setCheckpointTimeout(600_000L);
        conf.setMinPauseBetweenCheckpoints(30_000L);
        conf.setMaxConcurrentCheckpoints(1);
        conf.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        conf.setCheckpointStorage(config.checkpointDir);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(30)));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(TimeUtils.BUSINESS_ZONE);
        tableEnv.getConfig().getConfiguration()
                .setInteger("table.exec.resource.default-parallelism", PARALLELISM);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "UserRiskJob");

        // 用户规则只需要订单创建和支付尝试，不消费订单明细 Topic。
        tableEnv.executeSql("""
                CREATE TEMPORARY TABLE order_source (
                    event_id STRING,
                    event_type STRING,
                    event_time STRING,
                    business_date STRING,
                    order_id BIGINT,
                    user_id BIGINT,
                    shop_id BIGINT,
                    order_amount DECIMAL(20, 2),
                    create_time STRING
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'ods_order_info',
                    'properties.bootstrap.servers' = '%s',
                    'properties.group.id' = '%s',
                    'properties.enable.auto.commit' = 'false',
                    'properties.auto.offset.reset' = 'earliest',
                    'scan.startup.mode' = 'group-offsets',
                    'format' = 'json',
                    'json.fail-on-missing-field' = 'true',
                    'json.ignore-parse-errors' = 'false'
                )
                """.formatted(sql(config.bootstrapServers), sql(config.groupId)));

        tableEnv.executeSql("""
                CREATE TEMPORARY TABLE payment_source (
                    event_id STRING,
                    event_type STRING,
                    event_time STRING,
                    business_date STRING,
                    payment_id BIGINT,
                    order_id BIGINT,
                    user_id BIGINT,
                    payment_status STRING,
                    payment_amount DECIMAL(20, 2),
                    payment_time STRING
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'ods_payment_info',
                    'properties.bootstrap.servers' = '%s',
                    'properties.group.id' = '%s',
                    'properties.enable.auto.commit' = 'false',
                    'properties.auto.offset.reset' = 'earliest',
                    'scan.startup.mode' = 'group-offsets',
                    'format' = 'json',
                    'json.fail-on-missing-field' = 'true',
                    'json.ignore-parse-errors' = 'false'
                )
                """.formatted(sql(config.bootstrapServers), sql(config.groupId)));

        DataStream<RiskEvent> orderEvents = withWatermarks(
                tableEnv.toDataStream(tableEnv.sqlQuery("SELECT * FROM order_source"))
                        .flatMap(new EventParsers.OrderParser())
                        .name("解析订单事件")
                        .setParallelism(PARALLELISM));
        DataStream<RiskEvent> paymentEvents = withWatermarks(
                tableEnv.toDataStream(tableEnv.sqlQuery("SELECT * FROM payment_source"))
                        .flatMap(new EventParsers.PaymentParser())
                        .name("解析支付事件")
                        .setParallelism(PARALLELISM));

        SingleOutputStreamOperator<UserAlert> alerts = orderEvents
                .union(paymentEvents)
                .keyBy(event -> event.userId)
                .process(new UserRiskProcessFunction())
                .name("用户风险规则")
                .setParallelism(PARALLELISM);

        tableEnv.executeSql("""
                CREATE TEMPORARY TABLE user_alert_sink (
                    alert_id STRING,
                    alert_type STRING,
                    risk_level STRING,
                    risk_reason STRING,
                    user_id BIGINT,
                    stat_period_seconds INT,
                    unpaid_order_count INT,
                    failed_payment_count INT,
                    threshold_count INT,
                    alert_time TIMESTAMP(3),
                    alert_date DATE,
                    update_time TIMESTAMP(3),
                    PRIMARY KEY (alert_id) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '%s',
                    'table-name' = 'rt_user_risk_alert',
                    'username' = '%s',
                    'password' = '%s',
                    'driver' = 'com.mysql.cj.jdbc.Driver',
                    'sink.buffer-flush.max-rows' = '100',
                    'sink.buffer-flush.interval' = '1s',
                    'sink.max-retries' = '3'
                )
                """.formatted(
                sql(config.mysqlUrl), sql(config.mysqlUser), sql(config.mysqlPassword)));

        TypeInformation<Row> rowType = Types.ROW_NAMED(
                new String[] {
                    "alert_id", "alert_type", "risk_level", "risk_reason", "user_id",
                    "stat_period_seconds", "unpaid_order_count", "failed_payment_count",
                    "threshold_count", "alert_time", "alert_date", "update_time"
                },
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG,
                Types.INT, Types.INT, Types.INT, Types.INT, Types.SQL_TIMESTAMP,
                Types.SQL_DATE, Types.SQL_TIMESTAMP);

        DataStream<Row> rows = alerts.map(alert -> Row.of(
                        alert.alertId,
                        alert.alertType,
                        alert.riskLevel,
                        alert.riskReason,
                        alert.userId,
                        alert.statPeriodSeconds,
                        alert.unpaidOrderCount,
                        alert.failedPaymentCount,
                        alert.thresholdCount,
                        alert.alertTime,
                        alert.alertDate,
                        alert.updateTime))
                .returns(rowType)
                .name("用户告警转Row")
                .setParallelism(PARALLELISM);

        // rows 已完成最终字段映射，print 与 JDBC Sink 使用同一份数据，仅输出关键告警字段。
        rows.map(UserRiskJob::formatUserRow)
                .name("打印用户告警最终数据")
                .setParallelism(PARALLELISM)
                .print()
                .setParallelism(PARALLELISM);

        tableEnv.createTemporaryView("user_alert_changes", rows);
        tableEnv.executeSql("INSERT INTO user_alert_sink SELECT * FROM user_alert_changes").await();
    }

    /** 单行输出用户告警的关键字段，核心指标按告警类型动态生成。 */
    private static String formatUserRow(Row row) {
        String alertType = String.valueOf(row.getField(1));
        String metric;
        if ("高频未支付".equals(alertType)) {
            metric = "60秒内未支付订单" + row.getField(6) + "笔，阈值" + row.getField(8) + "笔";
        } else {
            metric = "180秒内支付失败" + row.getField(7) + "次，阈值" + row.getField(8) + "次";
        }
        return "告警ID=" + row.getField(0)
                + "，告警类型=" + row.getField(1)
                + "，风险等级=" + row.getField(2)
                + "，核心异常指标=" + metric
                + "，告警时间=" + row.getField(9);
    }

    /** 两分钟乱序与空闲分区检测保证两个 Topic 的 Watermark 可以持续推进。 */
    private static DataStream<RiskEvent> withWatermarks(DataStream<RiskEvent> events) {
        WatermarkStrategy<RiskEvent> strategy = WatermarkStrategy
                .<RiskEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                .withTimestampAssigner((event, previousTimestamp) -> event.eventTimeMs)
                .withIdleness(Duration.ofSeconds(30));
        return events.assignTimestampsAndWatermarks(strategy)
                .name("两分钟水位线")
                .setParallelism(PARALLELISM);
    }

    private static String sql(String value) {
        return value.replace("'", "''");
    }

    /** 用户规则使用滑动时间桶、事件时间 Timer 和状态 TTL 完成长期流式计算。 */
    private static final class UserRiskProcessFunction
            extends KeyedProcessFunction<Long, RiskEvent, UserAlert> {
        private static final Logger LOG = LoggerFactory.getLogger(UserRiskProcessFunction.class);

        private transient MapState<Long, Long> orderCreatedAt;
        private transient MapState<Long, Boolean> paidOrders;
        private transient MapState<Long, Long> failedPayments;
        private transient MapState<String, Boolean> processedEvents;
        private transient MapState<Long, Boolean> unpaidTimers;
        private transient MapState<Long, Boolean> failureTimers;
        private transient MapState<Long, Boolean> evaluatedUnpaidWindows;
        private transient MapState<Long, Boolean> evaluatedFailureWindows;
        private transient ValueState<Long> unpaidCooldownUntil;
        private transient ValueState<Long> failureCooldownUntil;
        private transient Counter lateEvents;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.minutes(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            orderCreatedAt = mapState("order-created-at", Types.LONG, Types.LONG, ttl);
            paidOrders = mapState("paid-orders", Types.LONG, Types.BOOLEAN, ttl);
            failedPayments = mapState("failed-payments", Types.LONG, Types.LONG, ttl);
            processedEvents = mapState("processed-events", Types.STRING, Types.BOOLEAN, ttl);
            unpaidTimers = mapState("unpaid-timers", Types.LONG, Types.BOOLEAN, ttl);
            failureTimers = mapState("failure-timers", Types.LONG, Types.BOOLEAN, ttl);
            evaluatedUnpaidWindows =
                    mapState("evaluated-unpaid-windows", Types.LONG, Types.BOOLEAN, ttl);
            evaluatedFailureWindows =
                    mapState("evaluated-failure-windows", Types.LONG, Types.BOOLEAN, ttl);

            ValueStateDescriptor<Long> unpaidCooldown =
                    new ValueStateDescriptor<>("unpaid-cooldown", Types.LONG);
            unpaidCooldown.enableTimeToLive(ttl);
            unpaidCooldownUntil = getRuntimeContext().getState(unpaidCooldown);

            ValueStateDescriptor<Long> failureCooldown =
                    new ValueStateDescriptor<>("failure-cooldown", Types.LONG);
            failureCooldown.enableTimeToLive(ttl);
            failureCooldownUntil = getRuntimeContext().getState(failureCooldown);
            lateEvents = getRuntimeContext().getMetricGroup().counter("late_events");
        }

        private <K, V> MapState<K, V> mapState(
                String name,
                TypeInformation<K> keyType,
                TypeInformation<V> valueType,
                StateTtlConfig ttl) {
            MapStateDescriptor<K, V> descriptor =
                    new MapStateDescriptor<>(name, keyType, valueType);
            descriptor.enableTimeToLive(ttl);
            return getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(
                RiskEvent event, Context context, Collector<UserAlert> out) throws Exception {
            if (processedEvents.contains(event.eventId)) {
                return;
            }
            processedEvents.put(event.eventId, true);

            if (context.timerService().currentWatermark() >= event.eventTimeMs) {
                lateEvents.inc();
                LOG.warn("用户事件已迟到，event_id={}，watermark={}",
                        event.eventId, context.timerService().currentWatermark());
            }

            if ("ORDER_CREATED".equals(event.eventType)) {
                orderCreatedAt.put(event.orderId, event.createTimeMs);
                for (long windowEnd : containingWindowEnds(
                        event.createTimeMs, UNPAID_WINDOW_MS, SLIDE_MS)) {
                    // 未支付需要额外等待两分钟，让跨 Topic 的成功支付有机会到达。
                    long timer = windowEnd + UNPAID_WAIT_MS;
                    unpaidTimers.put(timer, true);
                    context.timerService().registerEventTimeTimer(timer);
                }
            } else if ("PAYMENT_SUCCESS".equals(event.eventType)) {
                paidOrders.put(event.orderId, true);
            } else if ("PAYMENT_FAILED".equals(event.eventType)) {
                failedPayments.put(event.paymentId, event.paymentTimeMs);
                for (long windowEnd : containingWindowEnds(
                        event.paymentTimeMs, FAILURE_WINDOW_MS, SLIDE_MS)) {
                    failureTimers.put(windowEnd, true);
                    context.timerService().registerEventTimeTimer(windowEnd);
                }
            } else {
                throw new IllegalArgumentException("不支持的用户事件类型：" + event.eventType);
            }
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext context, Collector<UserAlert> out) throws Exception {
            if (unpaidTimers.contains(timestamp)) {
                evaluateUnpaid(timestamp - UNPAID_WAIT_MS, context.getCurrentKey(), out);
                unpaidTimers.remove(timestamp);
            }
            if (failureTimers.contains(timestamp)) {
                evaluateFailures(timestamp, context.getCurrentKey(), out);
                failureTimers.remove(timestamp);
            }
        }

        private void evaluateUnpaid(
                long windowEnd, long userId, Collector<UserAlert> out) throws Exception {
            if (evaluatedUnpaidWindows.contains(windowEnd)) {
                return;
            }
            evaluatedUnpaidWindows.put(windowEnd, true);
            long windowStart = windowEnd - UNPAID_WINDOW_MS;

            Map<Long, Long> orders = new HashMap<>();
            for (Map.Entry<Long, Long> entry : orderCreatedAt.entries()) {
                orders.put(entry.getKey(), entry.getValue());
            }
            Set<Long> paid = new HashSet<>();
            for (Long orderId : paidOrders.keys()) {
                paid.add(orderId);
            }

            int count = countUnpaid(orders, paid, windowStart, windowEnd);
            Long cooldown = unpaidCooldownUntil.value();
            if (count > 5 && (cooldown == null || windowEnd >= cooldown)) {
                out.collect(UserAlert.of(
                        userId,
                        windowEnd,
                        "高频未支付",
                        "高风险",
                        "用户在 60 秒内产生 " + count + " 笔未支付订单，超过阈值 5 笔",
                        60,
                        count,
                        null,
                        5,
                        "01"));
                unpaidCooldownUntil.update(windowEnd + COOLDOWN_MS);
            }
            cleanupOrders(windowStart);
        }

        private void evaluateFailures(
                long windowEnd, long userId, Collector<UserAlert> out) throws Exception {
            if (evaluatedFailureWindows.contains(windowEnd)) {
                return;
            }
            evaluatedFailureWindows.put(windowEnd, true);
            long windowStart = windowEnd - FAILURE_WINDOW_MS;
            int count = countFailures(failedPayments.values(), windowStart, windowEnd);
            Long cooldown = failureCooldownUntil.value();
            if (count >= 3 && (cooldown == null || windowEnd >= cooldown)) {
                out.collect(UserAlert.of(
                        userId,
                        windowEnd,
                        "连续支付失败",
                        "高风险",
                        "用户在 180 秒内连续支付失败 " + count + " 次，达到阈值 3 次",
                        180,
                        null,
                        count,
                        3,
                        "02"));
                failureCooldownUntil.update(windowEnd + COOLDOWN_MS);
            }
            cleanupFailures(windowStart);
        }

        private void cleanupOrders(long oldestRelevantTime) throws Exception {
            List<Long> expired = new ArrayList<>();
            for (Map.Entry<Long, Long> order : orderCreatedAt.entries()) {
                if (order.getValue() < oldestRelevantTime) {
                    expired.add(order.getKey());
                }
            }
            for (Long orderId : expired) {
                orderCreatedAt.remove(orderId);
                paidOrders.remove(orderId);
            }
        }

        private void cleanupFailures(long oldestRelevantTime) throws Exception {
            List<Long> expired = new ArrayList<>();
            for (Map.Entry<Long, Long> payment : failedPayments.entries()) {
                if (payment.getValue() < oldestRelevantTime) {
                    expired.add(payment.getKey());
                }
            }
            for (Long paymentId : expired) {
                failedPayments.remove(paymentId);
            }
        }
    }

    /** 返回事件所属的所有滑动窗口结束时间。 */
    static List<Long> containingWindowEnds(long timestamp, long windowSize, long slide) {
        int windowCount = Math.toIntExact(windowSize / slide);
        long firstEnd = Math.floorDiv(timestamp, slide) * slide + slide;
        List<Long> ends = new ArrayList<>(windowCount);
        for (int index = 0; index < windowCount; index++) {
            ends.add(firstEnd + index * slide);
        }
        return ends;
    }

    static int countUnpaid(
            Map<Long, Long> orderCreatedAt,
            Set<Long> paidOrderIds,
            long windowStart,
            long windowEnd) {
        int count = 0;
        for (Map.Entry<Long, Long> order : orderCreatedAt.entrySet()) {
            if (order.getValue() >= windowStart
                    && order.getValue() < windowEnd
                    && !paidOrderIds.contains(order.getKey())) {
                count++;
            }
        }
        return count;
    }

    static int countFailures(Iterable<Long> failureTimes, long windowStart, long windowEnd) {
        int count = 0;
        for (Long failureTime : failureTimes) {
            if (failureTime >= windowStart && failureTime < windowEnd) {
                count++;
            }
        }
        return count;
    }

    /** ID 只包含日期，不包含时分秒；12 位编号由稳定业务键生成。 */
    private static String alertId(long windowEnd, long userId, String ruleCode) {
        String seed = userId + ":" + windowEnd + ":" + ruleCode;
        return "H" + TimeUtils.businessDate(windowEnd) + stableNumber(seed);
    }

    private static String stableNumber(String seed) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(seed.getBytes(StandardCharsets.UTF_8));
            String number = new BigInteger(1, digest).mod(ID_MODULUS).toString();
            return "0".repeat(12 - number.length()) + number;
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("当前 JDK 不支持 SHA-256", error);
        }
    }

    private static Timestamp timestamp(long epochMs) {
        return Timestamp.valueOf(TimeUtils.toLocalDateTime(epochMs));
    }

    /** 用户告警输出模型，窗口边界只参与内部计算，不落结果表。 */
    public static final class UserAlert implements Serializable {
        public String alertId;
        public String alertType;
        public String riskLevel;
        public String riskReason;
        public long userId;
        public int statPeriodSeconds;
        public Integer unpaidOrderCount;
        public Integer failedPaymentCount;
        public int thresholdCount;
        public Timestamp alertTime;
        public Date alertDate;
        public Timestamp updateTime;

        public UserAlert() {
        }

        static UserAlert of(
                long userId,
                long windowEnd,
                String alertType,
                String riskLevel,
                String riskReason,
                int statPeriodSeconds,
                Integer unpaidOrderCount,
                Integer failedPaymentCount,
                int thresholdCount,
                String ruleCode) {
            UserAlert alert = new UserAlert();
            alert.alertId = alertId(windowEnd, userId, ruleCode);
            alert.alertType = alertType;
            alert.riskLevel = riskLevel;
            alert.riskReason = riskReason;
            alert.userId = userId;
            alert.statPeriodSeconds = statPeriodSeconds;
            alert.unpaidOrderCount = unpaidOrderCount;
            alert.failedPaymentCount = failedPaymentCount;
            alert.thresholdCount = thresholdCount;
            alert.alertTime = timestamp(windowEnd);
            alert.alertDate = Date.valueOf(TimeUtils.toLocalDate(windowEnd));
            alert.updateTime = timestamp(Math.max(System.currentTimeMillis(), windowEnd));
            return alert;
        }
    }
}
