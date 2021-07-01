package com.vmturbo.components.common.utils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Helper for handling time and duration.
 */
public class TimeUtil {
    /**
     * Get a human-readable duration string.
     * ie "6h 50m 15s"
     *
     * @param duration The duration to format into a more human-readable string.
     * @return A human-readable string representation of the duration.
     */
    @Nonnull
    public static String humanReadable(@Nonnull final Duration duration) {
        return duration.toString()
            .substring(2)
            .replaceAll("(\\d[HMS])(?!$)", "$1 ")
            .toLowerCase();
    }

    public static Instant earliest(@Nonnull Instant a, @Nonnull Instant b) {
        return a.isBefore(b)
                ? a
                : b;
    }

    public static Instant latest(@Nonnull Instant a, @Nonnull Instant b) {
        return a.isAfter(b)
                ? a
                : b;
    }

    /**
     * Convert a {@link LocalDateTime} to a UNIX timestamp.
     *
     * @param localTime The time.
     * @param clock The clock - this should be the same clock that was used to construct the time.
     * @return Epoch millis in the time zone of the clock.
     */
    public static long localTimeToMillis(@Nullable final LocalDateTime localTime, @Nonnull final Clock clock) {
        if (localTime == null) {
            return 0;
        } else {
            return localTime.atZone(clock.getZone()).toInstant().toEpochMilli();
        }
    }
}
