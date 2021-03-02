package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.savings.EntitySavingsStore.LastRollupTimes;
import com.vmturbo.cost.component.savings.EntitySavingsStore.RollupTimeInfo;

/**
 * Testing rollups.
 */
public class RollupSavingsProcessorTest {
    /**
     * UTC clock, same as used in config.
     */
    private final Clock clock = Clock.systemUTC();

    /**
     * Checks for single hour rollup in the middle of the 24 hour period, without any history.
     */
    @Test
    public void singleHourRollupPrevious() {
        final LastRollupTimes lastRollupTimes = new LastRollupTimes();
        long time7AmUtc = getTimeMillis("2021-02-25T07:00:00");

        final List<RollupTimeInfo> nextRollupTimes = RollupSavingsProcessor.checkRollupRequired(
                ImmutableList.of(time7AmUtc), lastRollupTimes, clock);

        assertEquals(1, nextRollupTimes.size());
        final RollupTimeInfo rollupTime = nextRollupTimes.get(0);
        assertTrue(rollupTime.isDaily());
        assertEquals(time7AmUtc, rollupTime.fromTime());

        long time12AmUtc = getTimeMillis("2021-02-25T00:00:00");
        assertEquals(time12AmUtc, rollupTime.toTime());
    }

    /**
     * Checks for rollup of timestamp at midnight, with no history (never rolled up previously).
     */
    @Test
    public void midnightHourRollupNoPrevious() {
        final LastRollupTimes lastRollupTimes = new LastRollupTimes();

        checkDailyMonthlyRollup(lastRollupTimes, "2021-02-25T00:00:00",
                "2021-02-24T00:00:00", "2021-02-28T00:00:00");
    }

    /**
     * Checks for midnight rollup on 1st day of month, to make sure we rollup successfully the
     * previous day to the previous month (instead of the current month).
     */
    @Test
    public void midnightHourMonthStartRollupNoPrevious() {
        final LastRollupTimes lastRollupTimes = new LastRollupTimes();

        checkDailyMonthlyRollup(lastRollupTimes, "2021-02-01T00:00:00",
                "2021-01-31T00:00:00", "2021-01-31T00:00:00");
    }

    /**
     * Helper method.
     *
     * @param lastRollupTimes Last time rollup was done, metadata read from DB.
     * @param timeToday String formatted day representing today.
     * @param timeYesterday String formatted day representing 1 day before.
     * @param timeMonthEnd String formatted day representing last day of month (based on day before).
     */
    private void checkDailyMonthlyRollup(@Nonnull final LastRollupTimes lastRollupTimes,
            @Nonnull final String timeToday, @Nonnull final String timeYesterday,
            @Nonnull final String timeMonthEnd) {

        final long time12AmUtcToday = getTimeMillis(timeToday);
        final long time12AmUtcDayBefore = getTimeMillis(timeYesterday);
        final long time12AmUtcMonthEnd = getTimeMillis(timeMonthEnd);

        final List<RollupTimeInfo> nextRollupTimes = RollupSavingsProcessor.checkRollupRequired(
                ImmutableList.of(time12AmUtcToday), lastRollupTimes, clock);
        assertEquals(2, nextRollupTimes.size());

        final RollupTimeInfo rollupTimeDaily = nextRollupTimes.get(0);
        assertTrue(rollupTimeDaily.isDaily());
        assertEquals(time12AmUtcToday, rollupTimeDaily.fromTime());
        assertEquals(time12AmUtcToday, rollupTimeDaily.toTime());

        final RollupTimeInfo rollupTimeMonthly = nextRollupTimes.get(1);
        assertTrue(rollupTimeMonthly.isMonthly());
        assertEquals(time12AmUtcDayBefore, rollupTimeMonthly.fromTime());
        assertEquals(time12AmUtcMonthEnd, rollupTimeMonthly.toTime());
    }

    /**
     * Util method to get epoch millis from UTC display string.
     *
     * @param utcDisplay Display time.
     * @return Epoch millis.
     */
    private long getTimeMillis(String utcDisplay) {
        return TimeUtil.localDateTimeToMilli(LocalDateTime.parse(utcDisplay), clock);
    }
}
