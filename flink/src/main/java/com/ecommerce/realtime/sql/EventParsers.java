package com.ecommerce.realtime.sql;

import com.ecommerce.realtime.model.RiskEvent;
import com.ecommerce.realtime.util.TimeUtils;
import java.math.BigDecimal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventParsers {
    private EventParsers() {
    }

    public static final class OrderParser extends BaseParser {
        @Override
        protected RiskEvent parse(Row row) {
            common(row, "ORDER_CREATED");
            RiskEvent event = base(row);
            event.orderId = required(row, 4, Long.class);
            event.userId = required(row, 5, Long.class);
            event.shopId = required(row, 6, Long.class);
            event.orderAmount = money(row, 7);
            event.createTimeMs = time(row, 8);
            sameBusinessTime(event.eventTimeMs, event.createTimeMs);
            return event;
        }
    }

    public static final class DetailParser extends BaseParser {
        @Override
        protected RiskEvent parse(Row row) {
            common(row, "ORDER_DETAIL");
            RiskEvent event = base(row);
            event.orderDetailId = required(row, 4, Long.class);
            event.orderId = required(row, 5, Long.class);
            event.skuId = required(row, 6, Long.class);
            event.skuNum = required(row, 7, Integer.class);
            event.originalAmount = money(row, 8);
            event.finalAmount = money(row, 9);
            event.createTimeMs = time(row, 10);
            sameBusinessTime(event.eventTimeMs, event.createTimeMs);
            return event;
        }
    }

    public static final class PaymentParser extends BaseParser {
        @Override
        protected RiskEvent parse(Row row) {
            String eventType = required(row, 1, String.class);
            if (!"PAYMENT_SUCCESS".equals(eventType) && !"PAYMENT_FAILED".equals(eventType)) {
                throw new IllegalArgumentException("unexpected payment event_type " + eventType);
            }
            RiskEvent event = base(row);
            event.paymentId = required(row, 4, Long.class);
            event.orderId = required(row, 5, Long.class);
            event.userId = required(row, 6, Long.class);
            event.paymentStatus = required(row, 7, String.class);
            event.paymentAmount = money(row, 8);
            event.paymentTimeMs = time(row, 9);
            sameBusinessTime(event.eventTimeMs, event.paymentTimeMs);
            boolean valid = "PAYMENT_SUCCESS".equals(eventType)
                    ? "SUCCESS".equals(event.paymentStatus)
                    : "FAILED".equals(event.paymentStatus);
            if (!valid) {
                throw new IllegalArgumentException("payment_status does not match event_type");
            }
            return event;
        }
    }

    private abstract static class BaseParser extends RichFlatMapFunction<Row, RiskEvent> {
        private static final Logger LOG = LoggerFactory.getLogger(BaseParser.class);
        private transient Counter invalidEvents;

        @Override
        public void open(Configuration parameters) {
            invalidEvents = getRuntimeContext().getMetricGroup().counter("invalid_time_or_contract_events");
        }

        @Override
        public final void flatMap(Row row, Collector<RiskEvent> out) {
            try {
                out.collect(parse(row));
            } catch (RuntimeException error) {
                invalidEvents.inc();
                LOG.warn("Discarding invalid Kafka event: {}", row, error);
            }
        }

        protected abstract RiskEvent parse(Row row);

        protected static RiskEvent base(Row row) {
            RiskEvent event = new RiskEvent();
            event.eventId = required(row, 0, String.class);
            event.eventType = required(row, 1, String.class);
            event.eventTimeMs = time(row, 2);
            event.businessDate = required(row, 3, String.class);
            if (!event.businessDate.equals(TimeUtils.businessDate(event.eventTimeMs))) {
                throw new IllegalArgumentException("business_date does not match event_time");
            }
            return event;
        }

        protected static void common(Row row, String expectedType) {
            String eventType = required(row, 1, String.class);
            if (!expectedType.equals(eventType)) {
                throw new IllegalArgumentException("expected event_type " + expectedType + " but got " + eventType);
            }
        }

        protected static long time(Row row, int position) {
            return TimeUtils.parseMillis(required(row, position, String.class));
        }

        protected static BigDecimal money(Row row, int position) {
            BigDecimal amount = required(row, position, BigDecimal.class);
            return amount.setScale(2);
        }

        protected static void sameBusinessTime(long eventTime, long payloadTime) {
            if (eventTime != payloadTime) {
                throw new IllegalArgumentException("event_time and payload business time differ");
            }
        }

        protected static <T> T required(Row row, int position, Class<T> type) {
            Object value = row.getField(position);
            if (value == null) {
                throw new IllegalArgumentException("required field at position " + position + " is null");
            }
            return type.cast(value);
        }
    }
}
