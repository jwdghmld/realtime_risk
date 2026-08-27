package com.ecommerce.realtime.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.realtime.util.TimeUtils;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class UserRiskJobTest {
    @Test
    void countsOnlyCreatedAndStillUnpaidOrdersInsideWindow() {
        Map<Long, Long> orders = Map.of(
                1L, 10_000L,
                2L, 20_000L,
                3L, 59_999L,
                4L, 60_000L);

        assertEquals(2, UserRiskJob.countUnpaid(
                orders, Set.of(2L), 0L, 60_000L));
    }

    @Test
    void failureWindowUsesStartInclusiveEndExclusiveBounds() {
        assertEquals(3, UserRiskJob.countFailures(
                List.of(0L, 1L, 179_999L, 180_000L), 0L, 180_000L));
    }

    @Test
    void assignsEventToEveryContainingSlidingWindow() {
        assertEquals(
                List.of(20_000L, 30_000L, 40_000L, 50_000L, 60_000L, 70_000L),
                UserRiskJob.containingWindowEnds(12_345L, 60_000L, 10_000L));
    }

    @Test
    void alertIdContainsDateWithoutTime() {
        long windowEnd = LocalDateTime.of(2026, 8, 27, 23, 59, 58)
                .atZone(TimeUtils.BUSINESS_ZONE)
                .toInstant()
                .toEpochMilli();

        UserRiskJob.UserAlert alert = UserRiskJob.UserAlert.of(
                1L, windowEnd, "高频未支付", "高风险", "测试",
                60, 5, null, 5, "01");

        assertEquals(21, alert.alertId.length());
        assertTrue(alert.alertId.matches("H20260827\\d{12}"));
    }
}
