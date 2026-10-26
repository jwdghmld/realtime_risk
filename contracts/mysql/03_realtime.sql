-- C11 MySQL 实时风控结果表 DDL
-- 订单风险与用户风险的业务粒度不同，因此分别落入两张结果表。
-- 本脚本只创建表，不删除测试阶段已经存在的旧表或历史数据。

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS realtime
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

-- 一行表示一条订单级风险。alert_id 格式为：风险等级前缀 + yyyyMMdd + 12位稳定编号。
CREATE TABLE IF NOT EXISTS realtime.rt_order_risk_alert (
  alert_id CHAR(21) NOT NULL COMMENT '告警ID，例如H20260827012345678901',
  alert_type VARCHAR(32) NOT NULL COMMENT '告警类型：订单时序异常、订单金额异常',
  risk_item VARCHAR(32) NOT NULL COMMENT '具体风险项中文标签',
  risk_level VARCHAR(8) NOT NULL COMMENT '风险等级：高风险、中风险',
  risk_reason VARCHAR(255) NOT NULL COMMENT '可直接展示的中文风险说明',
  order_id BIGINT NOT NULL COMMENT '订单ID',
  user_id BIGINT NULL COMMENT '下单用户ID，订单事件缺失时为空',
  shop_id BIGINT NULL COMMENT '订单店铺ID，订单事件缺失时为空',
  payment_id BIGINT NOT NULL COMMENT '触发检查的成功支付ID',
  order_create_time DATETIME(3) NULL COMMENT '订单创建时间',
  payment_time DATETIME(3) NOT NULL COMMENT '成功支付时间',
  order_amount DECIMAL(20,2) NULL COMMENT '订单金额',
  detail_amount DECIMAL(20,2) NOT NULL COMMENT '订单明细金额合计',
  payment_amount DECIMAL(20,2) NOT NULL COMMENT '成功支付金额',
  difference_amount DECIMAL(20,2) NULL COMMENT '金额异常的绝对差额',
  time_difference_seconds BIGINT NULL COMMENT '时序异常相差秒数',
  detail_count INT NOT NULL COMMENT '已收到的订单明细条数',
  alert_time DATETIME(3) NOT NULL COMMENT '规则判定业务时间',
  alert_date DATE NOT NULL COMMENT '告警业务日期',
  update_time DATETIME(3) NOT NULL COMMENT '最近一次写入时间',
  PRIMARY KEY (alert_id),
  KEY idx_order_alert_date_time (alert_date, alert_time),
  KEY idx_order_alert_filter (alert_date, alert_type, risk_level),
  KEY idx_order_alert_order (order_id, alert_time),
  CONSTRAINT chk_order_alert_id CHECK (alert_id REGEXP '^[HM][0-9]{20}$'),
  CONSTRAINT chk_order_alert_type CHECK (alert_type IN ('订单时序异常', '订单金额异常')),
  CONSTRAINT chk_order_risk_level CHECK (risk_level IN ('高风险', '中风险')),
  CONSTRAINT chk_order_risk_item CHECK (
    risk_item IN ('订单不存在', '支付早于下单', '明细金额不一致', '支付金额不一致')
  ),
  CONSTRAINT chk_order_detail_count CHECK (detail_count >= 0),
  CONSTRAINT chk_order_difference_amount CHECK (
    difference_amount IS NULL OR difference_amount >= 0
  ),
  CONSTRAINT chk_order_time_difference CHECK (
    time_difference_seconds IS NULL OR time_difference_seconds >= 0
  ),
  CONSTRAINT chk_order_alert_date CHECK (alert_date = DATE(alert_time)),
  CONSTRAINT chk_order_update_time CHECK (update_time >= alert_time)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='订单时序与金额风险告警';

-- 一行表示一个用户在一个统计周期结束点产生的风险。
CREATE TABLE IF NOT EXISTS realtime.rt_user_risk_alert (
  alert_id CHAR(21) NOT NULL COMMENT '告警ID，例如H20260827012345678901',
  alert_type VARCHAR(32) NOT NULL COMMENT '告警类型：高频未支付、连续支付失败',
  risk_level VARCHAR(8) NOT NULL COMMENT '风险等级：高风险',
  risk_reason VARCHAR(255) NOT NULL COMMENT '可直接展示的中文风险说明',
  user_id BIGINT NOT NULL COMMENT '风险用户ID',
  stat_period_seconds INT NOT NULL COMMENT '统计周期秒数',
  unpaid_order_count INT NULL COMMENT '统计周期内未支付订单数',
  failed_payment_count INT NULL COMMENT '统计周期内失败支付次数',
  threshold_count INT NOT NULL COMMENT '触发规则的数量阈值',
  alert_time DATETIME(3) NOT NULL COMMENT '统计周期结束时间，也是告警业务时间',
  alert_date DATE NOT NULL COMMENT '告警业务日期',
  update_time DATETIME(3) NOT NULL COMMENT '最近一次写入时间',
  PRIMARY KEY (alert_id),
  KEY idx_user_alert_date_time (alert_date, alert_time),
  KEY idx_user_alert_filter (alert_date, alert_type, risk_level),
  KEY idx_user_alert_user (user_id, alert_time),
  CONSTRAINT chk_user_alert_id CHECK (alert_id REGEXP '^[H][0-9]{20}$'),
  CONSTRAINT chk_user_alert_type CHECK (alert_type IN ('高频未支付', '连续支付失败')),
  CONSTRAINT chk_user_risk_level CHECK (risk_level = '高风险'),
  CONSTRAINT chk_user_stat_period CHECK (stat_period_seconds IN (60, 180)),
  CONSTRAINT chk_user_threshold CHECK (threshold_count > 0),
  CONSTRAINT chk_user_risk_count CHECK (
    (
      alert_type = '高频未支付'
      AND unpaid_order_count IS NOT NULL
      AND unpaid_order_count > threshold_count
      AND failed_payment_count IS NULL
    )
    OR
    (
      alert_type = '连续支付失败'
      AND unpaid_order_count IS NULL
      AND failed_payment_count IS NOT NULL
      AND failed_payment_count >= threshold_count
    )
  ),
  CONSTRAINT chk_user_alert_date CHECK (alert_date = DATE(alert_time)),
  CONSTRAINT chk_user_update_time CHECK (update_time >= alert_time)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='用户短周期交易行为风险告警';

-- Flink 直接以稳定 alert_id 执行 Upsert。
-- Kafka 重发、Checkpoint 恢复或人工重跑会更新原告警，不会新增重复记录。
