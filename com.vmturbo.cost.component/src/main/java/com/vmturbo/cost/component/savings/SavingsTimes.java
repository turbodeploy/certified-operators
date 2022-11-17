package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.rollup.LastRollupTimes;

/**
 * Data class containing a bunch of times that are needed for processing.
 * Contains the last rollup time, what values to use for last_updated during query, all the
 * distinct daily timestamps for which stats were written to DB this time, etc.
 */
public class SavingsTimes {
    /**
     * Last rollup time read from DB.
     */
    private final LastRollupTimes lastRollupTimes;

    /**
     * UTC clock.
     */
    private final Clock clock;

    /**
     * Timestamps of the daily stats records written.
     */
    private final Set<Long> dayStatsTimes;

    /**
     * Value of last_updated that is read at the beginning of this current processing cycle.
     */
    private long previousLastUpdatedTime;

    /**
     * Value of last_updated that is to be written at the end of this current processing cycle.
     */
    private long currentLastUpdatedTime;

    /**
     * How many minutes back to go for picking up the last_updated time end value.
     */
    private static final long endTimeOffsetMinutes = 10;

    /**
     * Creates a new instance.
     *
     * @param rollupTimes Last rollup time read from DB.
     * @param clock UTC clock for time conversions.
     */
    public SavingsTimes(@Nonnull final LastRollupTimes rollupTimes, @Nonnull final Clock clock) {
        this.lastRollupTimes = rollupTimes;
        this.clock = clock;
        this.dayStatsTimes = new HashSet<>();
        calculatePreviousLastUpdatedTime();
        currentLastUpdatedTime = 0;
    }

    /**
     * Get last rollup time info read from DB.
     * @return LastRollupTimes instance.
     */
    public LastRollupTimes getLastRollupTimes() {
        return lastRollupTimes;
    }

    /**
     * Gets a sorted list of daily stats times.
     *
     * @return Daily stats times sorted in ascending order.
     */
    public List<Long> getSortedDailyStatsTimes() {
        final List<Long> statsTimes = new ArrayList<>(dayStatsTimes);
        Collections.sort(statsTimes);
        return statsTimes;
    }

    /**
     * Updates the internal lastRollupTimes with newly calculated last_updated time if present.
     * Also sets the daily value to the max of the written stats daily values.
     *
     * @param sortedDailyStatsTimes Sorted list of daily stats times.
     */
    public void updateLastRollupTimes(@Nonnull final List<Long> sortedDailyStatsTimes) {
        // This is where we store the last_updated. If processed some savings this time, use
        // the new value of last_updated, else (no records this time), use previous value.
        lastRollupTimes.setLastTimeUpdated(currentLastUpdatedTime != 0
                ? currentLastUpdatedTime : previousLastUpdatedTime);

        // For last daily time, we store the max of the daily stats times that got written this time.
        if (!sortedDailyStatsTimes.isEmpty()) {
            long latestDayTimestamp = sortedDailyStatsTimes.get(sortedDailyStatsTimes.size() - 1);
            lastRollupTimes.setLastTimeByDay(latestDayTimestamp);
        }
    }

    /**
     * Get the saved last_updated value from previous savings run. We have processed records up to
     * this timestamp last time. So for the current savings run, we need to process any changes
     * that have last_updated value that is greater-than this previous value.
     * We are using the aggregation_meta_data.last_aggregated column to store the value of the
     * last_updated field that we have processed till now. Once we process another set of changes
     * from billing table, we will update this value with latest value of last_updated that we have
     * now processed.
     *
     * @return Previous last_updated value, or yesterday's midnight time if never had a previous value.
     */
    public long getPreviousLastUpdatedTime() {
        return previousLastUpdatedTime;
    }

    /**
     * Gets the newly calculated last_updated value.
     *
     * @return New last_updated value.
     */
    public long getCurrentLastUpdatedTime() {
        return currentLastUpdatedTime;
    }

    /**
     * Sets current value of last_updated time, to be written back after current processing is done.
     *
     * @param lud New last_updated value.
     */
    public void setCurrentLastUpdatedTime(long lud) {
        currentLastUpdatedTime = lud;
    }

    /**
     * Adds all daily stats times in the input to the existing internal set.
     *
     * @param dayTimes Daily stats times (i.e. times with which stats were written to daily table).
     */
    public void addAllDayStatsTimes(final Set<Long> dayTimes) {
        dayStatsTimes.addAll(dayTimes);
    }

    /**
     * Gets time up to which savings processing was successfully done last time. If no last time
     * found in DB, returns timestamp a day back. Can be run multiple times in a day if needed,
     * will process from previous last_updated onwards.
     * Updates the internal previousLastUpdatedTime variable to the fetched timestamp value.
     * Case 1: Never ran before.
     *      E.g. current time is 3/21 9:00 AM, we return last time as 3/20 12:00 AM (previous day).
     * Case 2: Aggregated daily before.
     *      In this case get the last updated timestamp from DB and use that.
     */
    private void calculatePreviousLastUpdatedTime() {
        LocalDateTime previousLastUpdated;
        if (lastRollupTimes.getLastTimeUpdated() == 0) {
            // No last time stored, return the previous day midnight. We will check in billing table
            // for any changes that happened since this time.
            // Truncate current time to top of the day - 00:00:00 (12:00 AM).
            previousLastUpdated = SavingsUtil.getCurrentDateTime(clock)
                    .truncatedTo(ChronoUnit.DAYS).minusDays(1);
        } else {
            // Get the last_aggregated column value.
            previousLastUpdated = SavingsUtil.getLocalDateTime(lastRollupTimes.getLastTimeUpdated(),
                    clock);
        }
        // Can be run any number of times per day.
        this.previousLastUpdatedTime = TimeUtil.localDateTimeToMilli(previousLastUpdated, clock);
    }

    /**
     * Helper for format timestamp for logging display.
     *
     * @param timestamp Epoch millis.
     * @return Formatted string.
     */
    private String getFormattedTime(long timestamp) {
        return timestamp == 0 ? "NA" : String.format("%s [%s]", timestamp,
                SavingsUtil.getLocalDateTime(timestamp, clock));
    }

    /**
     * Display essential fields.
     *
     * @return Formatted string.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Last rollup = {U:");
        sb.append(getFormattedTime(lastRollupTimes.getLastTimeUpdated()));
        sb.append(", D:");
        sb.append(getFormattedTime(lastRollupTimes.getLastTimeByDay()));
        sb.append(", M:");
        sb.append(getFormattedTime(lastRollupTimes.getLastTimeByMonth()));
        sb.append("}, previous LUD: ")
                .append(getFormattedTime(previousLastUpdatedTime));
        if (currentLastUpdatedTime > 0) {
            sb.append(", current LUD: ")
                    .append(getFormattedTime(currentLastUpdatedTime));
        }
        if (!dayStatsTimes.isEmpty()) {
            sb.append(", ").append(dayStatsTimes.size()).append(" day times.");
        }
        return sb.toString();
    }
}
