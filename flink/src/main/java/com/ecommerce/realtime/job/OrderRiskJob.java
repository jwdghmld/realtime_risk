package com.ecommerce.realtime.job;

import com.ecommerce.realtime.config.RealtimeConfig;
import com.ecommerce.realtime.model.RiskEvent;
import com.ecommerce.realtime.sql.EventParsers;
import com.ecommerce.realtime.util.TimeUtils;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * 订单实时风控作业。
 *
 * <p>同时消费订单、订单明细和支付事件，按订单 ID 归并后检查交易时序与金额一致性，
 * 结果写入 realtime.rt_order_risk_alert。
 */
public final class OrderRiskJob {
    private static final int PARALLELISM = 3;
    private static final long WAIT_MS = 2 * 60 * 1000L;
    private static final BigInteger ID_MODULUS = BigInteger.valueOf(1_000_000_000_000L);

    private OrderRiskJob() {
    }

    public static void main(String[] args) throws Exception {
        RealtimeConfig config = RealtimeConfig.orderRisk();
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
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "OrderRiskJob");

        // Source SQL 直接放在主类中，便于从入口完整阅读数据合同。
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
                CREATE TEMPORARY TABLE detail_source (
                    event_id STRING,
                    event_type STRING,
                    event_time STRING,
                    business_date STRING,
                    order_detail_id BIGINT,
                    order_id BIGINT,
                    sku_id BIGINT,
                    sku_num INT,
                    original_amount DECIMAL(20, 2),
                    final_amount DECIMAL(20, 2),
                    create_time STRING
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'ods_order_detail',
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
        DataStream<RiskEvent> detailEvents = withWatermarks(
                tableEnv.toDataStream(tableEnv.sqlQuery("SELECT * FROM detail_source"))
                        .flatMap(new EventParsers.DetailParser())
                        .name("解析订单明细事件")
                        .setParallelism(PARALLELISM));
        DataStream<RiskEvent> paymentEvents = withWatermarks(
                tableEnv.toDataStream(tableEnv.sqlQuery("SELECT * FROM payment_source"))
                        .flatMap(new EventParsers.PaymentParser())
                        .name("解析支付事件")
                        .setParallelism(PARALLELISM));

        SingleOutputStreamOperator<OrderAlert> alerts = orderEvents
                .union(detailEvents, paymentEvents)
                .keyBy(event -> event.orderId)
                .process(new OrderRiskProcessFunction())
                .name("订单风险规则")
                .setParallelism(PARALLELISM);

        tableEnv.executeSql("""
                CREATE TEMPORARY TABLE order_alert_sink (
                    alert_id STRING,
                    alert_type STRING,
                    risk_item STRING,
                    risk_level STRING,
                    risk_reason STRING,
                    order_id BIGINT,
                    user_id BIGINT,
                    shop_id BIGINT,
                    payment_id BIGINT,
                    order_create_time TIMESTAMP(3),
                    payment_time TIMESTAMP(3),
                    order_amount DECIMAL(20, 2),
                    detail_amount DECIMAL(20, 2),
                    payment_amount DECIMAL(20, 2),
                    difference_amount DECIMAL(20, 2),
                    time_difference_seconds BIGINT,
                    detail_count INT,
                    alert_time TIMESTAMP(3),
                    alert_date DATE,
                    update_time TIMESTAMP(3),
                    PRIMARY KEY (alert_id) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '%s',
                    'table-name' = 'rt_order_risk_alert',
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
                    "alert_id", "alert_type", "risk_item", "risk_level", "risk_reason",
                    "order_id", "user_id", "shop_id", "payment_id", "order_create_time",
                    "payment_time", "order_amount", "detail_amount", "payment_amount",
                    "difference_amount", "time_difference_seconds", "detail_count",
                    "alert_time", "alert_date", "update_time"
                },
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.SQL_TIMESTAMP,
                Types.SQL_TIMESTAMP, Types.BIG_DEC, Types.BIG_DEC, Types.BIG_DEC,
                Types.BIG_DEC, Types.LONG, Types.INT, Types.SQL_TIMESTAMP,
                Types.SQL_DATE, Types.SQL_TIMESTAMP);

        DataStream<Row> rows = alerts.map(alert -> Row.of(
                        alert.alertId,
                        alert.alertType,
                        alert.riskItem,
                        alert.riskLevel,
                        alert.riskReason,
                        alert.orderId,
                        alert.userId,
                        alert.shopId,
                        alert.paymentId,
                        alert.orderCreateTime,
                        alert.paymentTime,
                        alert.orderAmount,
                        alert.detailAmount,
                        alert.paymentAmount,
                        alert.differenceAmount,
                        alert.timeDifferenceSeconds,
                        alert.detailCount,
                        alert.alertTime,
                        alert.alertDate,
                        alert.updateTime))
                .returns(rowType)
                .name("订单告警转Row")
                .setParallelism(PARALLELISM);

        // rows 已完成最终字段映射，print 与 JDBC Sink 使用同一份数据，仅输出关键告警字段。
        rows.map(OrderRiskJob::formatOrderRow)
                .name("打印订单告警最终数据")
                .setParallelism(PARALLELISM)
                .print()
                .setParallelism(PARALLELISM);

        tableEnv.createTemporaryView("order_alert_changes", rows);
        tableEnv.executeSql("INSERT INTO order_alert_sink SELECT * FROM order_alert_changes").await();
    }

    /** 单行输出订单告警的关键字段，核心指标按告警类型动态生成。 */
    private static String formatOrderRow(Row row) {
        String riskItem = String.valueOf(row.getField(2));
        String metric;
        if ("支付早于下单".equals(riskItem)) {
            metric = "支付早于下单" + row.getField(15) + "秒";
        } else if ("明细金额不一致".equals(riskItem)) {
            metric = "明细金额差额" + row.getField(14) + "元";
        } else if ("支付金额不一致".equals(riskItem)) {
            metric = "支付金额差额" + row.getField(14) + "元";
        } else {
            metric = "订单创建事件缺失";
        }
        return "告警ID=" + row.getField(0)
                + "，告警类型=" + row.getField(1)
                + "，风险等级=" + row.getField(3)
                + "，核心异常指标=" + metric
                + "，告警时间=" + row.getField(17);
    }

    /** 为三个输入流统一设置两分钟乱序和三十秒空闲分区检测。 */
    private static DataStream<RiskEvent> withWatermarks(DataStream<RiskEvent> events) {
        WatermarkStrategy<RiskEvent> strategy = WatermarkStrategy
                .<RiskEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                .withTimestampAssigner((event, previousTimestamp) -> event.eventTimeMs)
                .withIdleness(Duration.ofSeconds(30));
        return events.assignTimestampsAndWatermarks(strategy)
                .name("两分钟水位线")
                .setParallelism(PARALLELISM);
    }

    /** 转义固定配置中的单引号，避免破坏 Flink SQL 字符串。 */
    private static String sql(String value) {
        return value.replace("'", "''");
    }

    /** 订单、明细和支付跨 Topic 到达，因此按订单维持状态并在支付后等待两分钟。 */
    private static final class OrderRiskProcessFunction
            extends KeyedProcessFunction<Long, RiskEvent, OrderAlert> {
        private static final Logger LOG = LoggerFactory.getLogger(OrderRiskProcessFunction.class);

        private transient ValueState<OrderSnapshot> orderState;
        private transient MapState<Long, DetailSnapshot> detailState;
        private transient MapState<Long, PaymentSnapshot> paymentState;
        private transient MapState<String, Boolean> processedEvents;
        private transient Counter lateEvents;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<OrderSnapshot> orderDescriptor =
                    new ValueStateDescriptor<>("order", TypeInformation.of(OrderSnapshot.class));
            orderDescriptor.enableTimeToLive(ttl);
            orderState = getRuntimeContext().getState(orderDescriptor);

            MapStateDescriptor<Long, DetailSnapshot> detailDescriptor =
                    new MapStateDescriptor<>(
                            "details", Types.LONG, TypeInformation.of(DetailSnapshot.class));
            detailDescriptor.enableTimeToLive(ttl);
            detailState = getRuntimeContext().getMapState(detailDescriptor);

            MapStateDescriptor<Long, PaymentSnapshot> paymentDescriptor =
                    new MapStateDescriptor<>(
                            "payments", Types.LONG, TypeInformation.of(PaymentSnapshot.class));
            paymentDescriptor.enableTimeToLive(ttl);
            paymentState = getRuntimeContext().getMapState(paymentDescriptor);

            MapStateDescriptor<String, Boolean> processedDescriptor =
                    new MapStateDescriptor<>("processed-events", Types.STRING, Types.BOOLEAN);
            processedDescriptor.enableTimeToLive(ttl);
            processedEvents = getRuntimeContext().getMapState(processedDescriptor);
            lateEvents = getRuntimeContext().getMetricGroup().counter("late_events");
        }

        @Override
        public void processElement(
                RiskEvent event, Context context, Collector<OrderAlert> out) throws Exception {
            // event_id 去重用于抵抗 Producer 重发和 Checkpoint 恢复后的重复消费。
            if (processedEvents.contains(event.eventId)) {
                return;
            }
            processedEvents.put(event.eventId, true);

            if (context.timerService().currentWatermark() >= event.eventTimeMs) {
                lateEvents.inc();
                LOG.warn("订单事件已迟到，event_id={}，watermark={}",
                        event.eventId, context.timerService().currentWatermark());
            }

            switch (event.eventType) {
                case "ORDER_CREATED":
                    orderState.update(new OrderSnapshot(
                            event.userId, event.shopId, event.orderAmount, event.createTimeMs));
                    break;
                case "ORDER_DETAIL":
                    detailState.put(event.orderDetailId,
                            new DetailSnapshot(event.orderDetailId, event.finalAmount));
                    break;
                case "PAYMENT_SUCCESS":
                case "PAYMENT_FAILED":
                    boolean successful = "PAYMENT_SUCCESS".equals(event.eventType);
                    paymentState.put(event.paymentId, new PaymentSnapshot(
                            event.paymentId, event.paymentAmount, event.paymentTimeMs, successful));
                    if (successful) {
                        context.timerService().registerEventTimeTimer(event.paymentTimeMs + WAIT_MS);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("不支持的事件类型：" + event.eventType);
            }
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext context, Collector<OrderAlert> out) throws Exception {
            PaymentSnapshot payment = successfulPaymentAt(timestamp);
            if (payment == null) {
                return;
            }

            OrderSnapshot order = orderState.value();
            BigDecimal detailAmount = BigDecimal.ZERO.setScale(2);
            int detailCount = 0;
            for (DetailSnapshot detail : detailState.values()) {
                detailAmount = detailAmount.add(detail.finalAmount);
                detailCount++;
            }

            for (Violation violation : evaluate(order, detailAmount, payment)) {
                out.collect(OrderAlert.of(
                        context.getCurrentKey(), order, payment, detailAmount,
                        detailCount, timestamp, violation));
            }

            // 一张订单最多一笔成功支付，完成判断后主动释放订单状态。
            orderState.clear();
            detailState.clear();
            paymentState.clear();
        }

        private PaymentSnapshot successfulPaymentAt(long timer) throws Exception {
            for (PaymentSnapshot payment : paymentState.values()) {
                if (payment.successful && payment.paymentTimeMs + WAIT_MS == timer) {
                    return payment;
                }
            }
            return null;
        }
    }

    /** 纯规则函数不访问 Flink 状态，便于单独阅读和测试。 */
    static List<Violation> evaluate(
            OrderSnapshot order, BigDecimal detailAmount, PaymentSnapshot payment) {
        List<Violation> violations = new ArrayList<>();
        if (order == null) {
            violations.add(new Violation(
                    "订单时序异常", "订单不存在", "高风险",
                    "成功支付已到达，但未收到对应订单创建事件", null, null, "01"));
            return violations;
        }

        if (payment.paymentTimeMs < order.createTimeMs) {
            long seconds = Math.max(1L, (order.createTimeMs - payment.paymentTimeMs) / 1000L);
            violations.add(new Violation(
                    "订单时序异常", "支付早于下单", "高风险",
                    "支付时间早于订单创建时间 " + seconds + " 秒",
                    null, seconds, "02"));
        }

        BigDecimal detailDifference = detailAmount.subtract(order.orderAmount).abs();
        if (detailDifference.compareTo(BigDecimal.ZERO) != 0) {
            violations.add(new Violation(
                    "订单金额异常", "明细金额不一致", "中风险",
                    "订单明细金额合计与订单金额不一致，差额为 " + detailDifference + " 元",
                    detailDifference, null, "03"));
        }

        BigDecimal paymentDifference = payment.paymentAmount.subtract(order.orderAmount).abs();
        if (paymentDifference.compareTo(BigDecimal.ZERO) != 0) {
            violations.add(new Violation(
                    "订单金额异常", "支付金额不一致", "中风险",
                    "支付金额与订单金额不一致，差额为 " + paymentDifference + " 元",
                    paymentDifference, null, "04"));
        }
        return violations;
    }

    /** 生成“等级前缀 + 日期 + 12位编号”的稳定告警 ID。 */
    private static String alertId(
            String riskLevel, long alertTimeMs, long orderId, long paymentId, String ruleCode) {
        String prefix = "高风险".equals(riskLevel) ? "H" : "M";
        String seed = orderId + ":" + paymentId + ":" + ruleCode;
        return prefix + TimeUtils.businessDate(alertTimeMs) + stableNumber(seed);
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

    /** 订单告警输出模型，字段与 rt_order_risk_alert 一一对应。 */
    public static final class OrderAlert implements Serializable {
        public String alertId;
        public String alertType;
        public String riskItem;
        public String riskLevel;
        public String riskReason;
        public long orderId;
        public Long userId;
        public Long shopId;
        public long paymentId;
        public Timestamp orderCreateTime;
        public Timestamp paymentTime;
        public BigDecimal orderAmount;
        public BigDecimal detailAmount;
        public BigDecimal paymentAmount;
        public BigDecimal differenceAmount;
        public Long timeDifferenceSeconds;
        public int detailCount;
        public Timestamp alertTime;
        public Date alertDate;
        public Timestamp updateTime;

        public OrderAlert() {
        }

        static OrderAlert of(
                long orderId,
                OrderSnapshot order,
                PaymentSnapshot payment,
                BigDecimal detailAmount,
                int detailCount,
                long alertTimeMs,
                Violation violation) {
            OrderAlert alert = new OrderAlert();
            alert.alertId = alertId(
                    violation.riskLevel, alertTimeMs, orderId, payment.paymentId, violation.ruleCode);
            alert.alertType = violation.alertType;
            alert.riskItem = violation.riskItem;
            alert.riskLevel = violation.riskLevel;
            alert.riskReason = violation.riskReason;
            alert.orderId = orderId;
            alert.userId = order == null ? null : order.userId;
            alert.shopId = order == null ? null : order.shopId;
            alert.paymentId = payment.paymentId;
            alert.orderCreateTime = order == null ? null : timestamp(order.createTimeMs);
            alert.paymentTime = timestamp(payment.paymentTimeMs);
            alert.orderAmount = order == null ? null : order.orderAmount;
            alert.detailAmount = detailAmount;
            alert.paymentAmount = payment.paymentAmount;
            alert.differenceAmount = violation.differenceAmount;
            alert.timeDifferenceSeconds = violation.timeDifferenceSeconds;
            alert.detailCount = detailCount;
            alert.alertTime = timestamp(alertTimeMs);
            alert.alertDate = Date.valueOf(TimeUtils.toLocalDate(alertTimeMs));
            alert.updateTime = timestamp(Math.max(System.currentTimeMillis(), alertTimeMs));
            return alert;
        }
    }

    public static final class OrderSnapshot implements Serializable {
        public long userId;
        public long shopId;
        public BigDecimal orderAmount;
        public long createTimeMs;

        public OrderSnapshot() {
        }

        public OrderSnapshot(long userId, long shopId, BigDecimal orderAmount, long createTimeMs) {
            this.userId = userId;
            this.shopId = shopId;
            this.orderAmount = orderAmount;
            this.createTimeMs = createTimeMs;
        }
    }

    public static final class DetailSnapshot implements Serializable {
        public long detailId;
        public BigDecimal finalAmount;

        public DetailSnapshot() {
        }

        public DetailSnapshot(long detailId, BigDecimal finalAmount) {
            this.detailId = detailId;
            this.finalAmount = finalAmount;
        }
    }

    public static final class PaymentSnapshot implements Serializable {
        public long paymentId;
        public BigDecimal paymentAmount;
        public long paymentTimeMs;
        public boolean successful;

        public PaymentSnapshot() {
        }

        public PaymentSnapshot(
                long paymentId, BigDecimal paymentAmount, long paymentTimeMs, boolean successful) {
            this.paymentId = paymentId;
            this.paymentAmount = paymentAmount;
            this.paymentTimeMs = paymentTimeMs;
            this.successful = successful;
        }
    }

    static final class Violation implements Serializable {
        public String alertType;
        public String riskItem;
        public String riskLevel;
        public String riskReason;
        public BigDecimal differenceAmount;
        public Long timeDifferenceSeconds;
        public String ruleCode;

        public Violation() {
        }

        Violation(
                String alertType,
                String riskItem,
                String riskLevel,
                String riskReason,
                BigDecimal differenceAmount,
                Long timeDifferenceSeconds,
                String ruleCode) {
            this.alertType = alertType;
            this.riskItem = riskItem;
            this.riskLevel = riskLevel;
            this.riskReason = riskReason;
            this.differenceAmount = differenceAmount;
            this.timeDifferenceSeconds = timeDifferenceSeconds;
            this.ruleCode = ruleCode;
        }
    }
}
