package com.vmturbo.cost.component.rollup;

import java.util.Objects;

/**
 * Util class to keep info about rollup (daily or monthly) to be done.
 */
public class RollupTimeInfo {
    /**
     * Whether rolling to daily or monthly.
     */
    private final RollupDurationType durationType;

    /**
     * Time rolling from, e.g. hourly '2021-02-16 19:00:00' data being rolled over to daily.
     */
    private final long fromTime;

    /**
     * Time rolling to, e.g. hourly data rolled up to day time '2021-02-16 00:00:00'.
     */
    private final long toTime;

    /**
     * Creates a new instance.
     *
     * @param durationType Type of duration - daily or monthly.
     * @param fromTime     Source time from which rollup needs to be done.
     * @param toTime       Target time to which rollup is to be done.
     */
    public RollupTimeInfo(RollupDurationType durationType, long fromTime, long toTime) {
        this.durationType = durationType;
        this.fromTime = fromTime;
        this.toTime = toTime;
    }

    /**
     * Check if it is latest-to-hourly rollup.
     *
     * @return True for latest-to-hourly rollup.
     */
    public boolean isHourly() {
        return durationType == RollupDurationType.HOURLY;
    }

    /**
     * Check if it is hourly-to-daily rollup.
     *
     * @return True for hourly-to-daily rollup.
     */
    public boolean isDaily() {
        return durationType == RollupDurationType.DAILY;
    }

    /**
     * Check if it is daily-to-monthly rollup.
     *
     * @return True for daily-to-monthly rollup.
     */
    public boolean isMonthly() {
        return durationType == RollupDurationType.MONTHLY;
    }

    /**
     * Get rollup start time.
     *
     * @return Rollup start time.
     */
    public long fromTime() {
        return fromTime;
    }

    /**
     * Get rollup end time.
     *
     * @return Rollup end time.
     */
    public long toTime() {
        return toTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RollupTimeInfo that = (RollupTimeInfo)o;
        return fromTime == that.fromTime && toTime == that.toTime
                && durationType == that.durationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationType, fromTime, toTime);
    }

    @Override
    public String toString() {
        return String.format("%s: %d -> %d", isDaily() ? "D" : "M", fromTime, toTime);
    }
}
