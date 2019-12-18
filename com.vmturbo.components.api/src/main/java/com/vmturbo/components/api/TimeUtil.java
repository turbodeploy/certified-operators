package com.vmturbo.components.api;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import javax.annotation.Nonnull;

/**
 * Utilities for dealing with time-related classes.
 */
public class TimeUtil {

    /**
     * Convert {@link LocalDateTime} to long.
     *
     * @param dateTime start of date with LocalDateTime type.
     * @param clock a clock providing access to the current instant, date and time
     * @return date time in long type.
     */
    public static long localDateTimeToMilli(@Nonnull final LocalDateTime dateTime,
                                            @Nonnull final Clock clock) {
        return Date.from(dateTime.atZone(clock.getZone()).toInstant()).getTime();
    }

    /**
     * Convert {@link LocalDate} to long.
     *
     * @param date start of date with LocalDate type
     * @param clock a clock providing access to the current instant, date and time
     * @return date in long type
     */
    public static long localDateToMilli(@Nonnull final LocalDate date,
                                            @Nonnull final Clock clock) {
        return Date.from(date.atStartOfDay(clock.getZone()).toInstant()).getTime();
    }

    /**
     * Convert epoch milliseconds to {@link LocalDate} according to UTC time.
     *
     * @param epochMillis the timestamp in epoch milliseconds.
     * @return the date matching the timestamp.
     */
    public static LocalDate milliToLocalDateUTC(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalDate();
    }
}
