package com.ecommerce.realtime.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

public final class TimeUtils {
    public static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter INPUT_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter BUSINESS_DATE = DateTimeFormatter.ofPattern("uuuuMMdd");

    private TimeUtils() {
    }

    public static long parseMillis(String value) {
        return LocalDateTime.parse(value, INPUT_FORMAT)
                .atZone(BUSINESS_ZONE)
                .toInstant()
                .toEpochMilli();
    }

    public static LocalDateTime toLocalDateTime(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), BUSINESS_ZONE);
    }

    public static LocalDate toLocalDate(long epochMillis) {
        return toLocalDateTime(epochMillis).toLocalDate();
    }

    public static String businessDate(long epochMillis) {
        return BUSINESS_DATE.format(toLocalDate(epochMillis));
    }
}
