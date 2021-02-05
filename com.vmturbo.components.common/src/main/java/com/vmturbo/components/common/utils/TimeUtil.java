package com.vmturbo.components.common.utils;

import java.time.Duration;
import java.time.Instant;

import javax.annotation.Nonnull;

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
}
