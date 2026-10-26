package com.ecommerce.realtime.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class OrderRiskJobTest {
    @Test
    void detectsMissingOrder() {
        List<OrderRiskJob.Violation> violations = OrderRiskJob.evaluate(
                null,
                new BigDecimal("99.00"),
                payment("99.00", 1_000L));

        assertEquals(List.of("订单不存在"), riskItems(violations));
    }

    @Test
    void detectsSequenceAndBothAmountMismatches() {
        OrderRiskJob.OrderSnapshot order = new OrderRiskJob.OrderSnapshot(
                7L, 8L, new BigDecimal("100.00"), 2_000L);

        List<OrderRiskJob.Violation> violations = OrderRiskJob.evaluate(
                order, new BigDecimal("99.99"), payment("99.98", 1_000L));

        assertEquals(
                List.of("支付早于下单", "明细金额不一致", "支付金额不一致"),
                riskItems(violations));
        assertEquals(new BigDecimal("0.01"), violations.get(1).differenceAmount);
        assertEquals(new BigDecimal("0.02"), violations.get(2).differenceAmount);
    }

    @Test
    void acceptsConsistentOrder() {
        OrderRiskJob.OrderSnapshot order = new OrderRiskJob.OrderSnapshot(
                7L, 8L, new BigDecimal("100.00"), 1_000L);

        assertEquals(List.of(), OrderRiskJob.evaluate(
                order, new BigDecimal("100.00"), payment("100.00", 2_000L)));
    }

    private static OrderRiskJob.PaymentSnapshot payment(String amount, long time) {
        return new OrderRiskJob.PaymentSnapshot(9L, new BigDecimal(amount), time, true);
    }

    private static List<String> riskItems(List<OrderRiskJob.Violation> violations) {
        return violations.stream().map(row -> row.riskItem).collect(Collectors.toList());
    }
}
