package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.TimeUtil;

/**
 * General entity savings related utility methods.
 */
public class SavingsUtil {
    /**
     * Inner instance, not meant to be called.
     */
    private SavingsUtil() {
    }

    /**
     * Util to convert epoch millis to LocalDateTime before storing into DB.
     *
     * @param timeMillis epoch millis.
     * @param clock UTC clock.
     * @return LocalDateTime created from millis.
     */
    @Nonnull
    public static LocalDateTime getLocalDateTime(long timeMillis, final Clock clock) {
        return Instant.ofEpochMilli(timeMillis).atZone(clock.getZone()).toLocalDateTime();
    }

    /**
     * Epoch millis to display timestamp.
     *
     * @param timeMillis Time since epoch in millis.
     * @return LocalDateTime for display.
     */
    @Nonnull
    public static LocalDateTime getLocalDateTime(long timeMillis) {
        return Instant.ofEpochMilli(timeMillis).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * Given a day time, returns the time (epoch millis) of the month end.
     *
     * @param dayTime Time of day, e.g '2021-02-16 20:00:00'
     * @param clock Clock to use (UTC).
     * @return Epoch millis for month end '2021-02-28 00:00:00'
     */
    public static long getMonthEndTime(@Nonnull final LocalDateTime dayTime, final Clock clock) {
        YearMonth month = YearMonth.from(dayTime);
        return TimeUtil.localDateToMilli(month.atEndOfMonth(), clock);
    }

    /**
     * Gets 12:00 AM on the start of the day of the given input time.
     *
     * @param timeMillis Epoch time in millis. E.g. for a time like 'Feb 12, 2022 1:30:00 PM'
     * @param clock Clock to use.
     * @return Day start time, e.g. 'Feb 12, 2022 12:00:00 AM'
     */
    public static LocalDateTime getDayStartTime(long timeMillis, final Clock clock) {
        Instant eachInstant = Instant.ofEpochMilli(timeMillis);
        LocalDateTime eachDateUtc = eachInstant.atZone(clock.getZone()).toLocalDateTime();
        return eachDateUtc.toLocalDate().atStartOfDay();
    }

    /**
     * Gets current date.
     *
     * @param clock Clock to use.
     * @return Current date.
     */
    @Nonnull
    public static LocalDateTime getCurrentDateTime(final Clock clock) {
        return LocalDateTime.now(clock);
    }
}
