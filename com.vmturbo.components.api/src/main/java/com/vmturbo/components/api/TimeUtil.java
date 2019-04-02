package com.vmturbo.components.api;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import javax.annotation.Nonnull;

/**
 * Utilities for dealing with time-related classes.
 */
public class TimeUtil {

    /**
     * Convert local date time to long.
     * @param dateTime start of date with LocalDateTime type.
     * @return date time in long type.
     */
    public static long localDateTimeToMilli(@Nonnull final LocalDateTime dateTime,
                                            @Nonnull final Clock clock) {
        return Date.from(dateTime.atZone(clock.getZone()).toInstant()).getTime();
    }

}
