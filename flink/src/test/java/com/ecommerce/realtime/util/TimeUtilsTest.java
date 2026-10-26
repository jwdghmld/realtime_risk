package com.ecommerce.realtime.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.Test;

class TimeUtilsTest {
    @Test
    void parsesSecondsAndOptionalMillisecondsInShanghai() {
        long seconds = TimeUtils.parseMillis("2026-08-25 09:15:20");
        long millis = TimeUtils.parseMillis("2026-08-25 09:15:20.123");

        assertEquals(123L, millis - seconds);
        assertEquals("20260825", TimeUtils.businessDate(millis));
        assertEquals("2026-08-25T09:15:20.123", TimeUtils.toLocalDateTime(millis).toString());
    }

    @Test
    void rejectsUnsupportedTimeFormats() {
        assertThrows(DateTimeParseException.class,
                () -> TimeUtils.parseMillis("2026/08/25 09:15:20"));
        assertThrows(DateTimeParseException.class,
                () -> TimeUtils.parseMillis("2026-08-25T09:15:20"));
        assertThrows(DateTimeParseException.class,
                () -> TimeUtils.parseMillis("2026-02-30 09:15:20"));
    }
}
