package com.vmturbo.cost.component.savings.bottomup;

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
     * Dummy price change used to populate the current recommendation field in the entity state for
     * entities that existed before action revert was implemented.  We now require that all entity
     * state instances contain a valid recommendation.
     */
    public static final EntityPriceChange EMPTY_PRICE_CHANGE = new EntityPriceChange.Builder()
            .active(false)
            .sourceOid(0L).destinationOid(0L)
            .sourceCost(0D).destinationCost(0D)
            .build();


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
}
