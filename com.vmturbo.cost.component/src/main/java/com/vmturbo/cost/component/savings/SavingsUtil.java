package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;

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
}
