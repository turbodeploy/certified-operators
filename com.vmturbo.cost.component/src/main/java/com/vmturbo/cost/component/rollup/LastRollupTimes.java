package com.vmturbo.cost.component.rollup;

/**
 * Contains info about last rollup times, read from aggregation metadata table.
 */
public class LastRollupTimes {
    /**
     * Last time rollup metadata was updated in DB, 0L if never set.
     */
    private long lastTimeUpdated;

    /**
     * Last hourly stats data that was processed and written to DB, 0L if never set.
     */
    private long lastTimeByHour;

    /**
     * Last day start time (12 AM) for which data was rolled up into monthly, 0L if never set.
     */
    private long lastTimeByDay;

    /**
     * Last month end time to which daily data was rolled up, 0L if never set.
     */
    private long lastTimeByMonth;

    /**
     * Create new {@code LastRollupTimes} instance.
     */
    public LastRollupTimes() {
        this(0L, 0L, 0L, 0L);
    }

    /**
     * Create new {@code LastRollupTimes} instance.
     *
     * @param lastUpdated Last updated timestamp.
     * @param lastHourly  Last timestamp for hourly table.
     * @param lastDaily   Last timestamp for daily table.
     * @param lastMonthly Last timestamp for monthly table.
     */
    public LastRollupTimes(long lastUpdated, long lastHourly, long lastDaily, long lastMonthly) {
        lastTimeUpdated = lastUpdated;
        lastTimeByHour = lastHourly;
        lastTimeByDay = lastDaily;
        lastTimeByMonth = lastMonthly;
    }

    /**
     * Set last updated timestamp.
     *
     * @param lastUpdated Last updated timestamp.
     */
    public void setLastTimeUpdated(long lastUpdated) {
        lastTimeUpdated = lastUpdated;
    }

    /**
     * Get last updated timestamp.
     *
     * @return Last updated timestamp.
     */
    public long getLastTimeUpdated() {
        return lastTimeUpdated;
    }

    /**
     * Set last timestamp for hourly table.
     *
     * @param lastHourly Last timestamp for hourly table.
     */
    public void setLastTimeByHour(long lastHourly) {
        lastTimeByHour = lastHourly;
    }

    /**
     * Get last timestamp for hourly table.
     *
     * @return Last timestamp for hourly table.
     */
    public long getLastTimeByHour() {
        return lastTimeByHour;
    }

    /**
     * Check if last timestamp for hourly table is set.
     *
     * @return True if last timestamp for hourly table is set.
     */
    public boolean hasLastTimeByHour() {
        return lastTimeByHour != 0L;
    }

    /**
     * Set last timestamp for daily table.
     *
     * @param lastDaily Last timestamp for daily table.
     */
    public void setLastTimeByDay(long lastDaily) {
        lastTimeByDay = lastDaily;
    }

    /**
     * Get last timestamp for daily table.
     *
     * @return Last timestamp for daily table.
     */
    public long getLastTimeByDay() {
        return lastTimeByDay;
    }

    /**
     * Check if last timestamp for daily table is set.
     *
     * @return True if last timestamp for daily table is set.
     */
    public boolean hasLastTimeByDay() {
        return lastTimeByDay != 0L;
    }

    /**
     * Set last timestamp for monthly table.
     *
     * @param lastMonthly Last timestamp for monthly table.
     */
    public void setLastTimeByMonth(long lastMonthly) {
        lastTimeByMonth = lastMonthly;
    }

    /**
     * Get last timestamp for monthly table.
     *
     * @return Last timestamp for monthly table.
     */
    public long getLastTimeByMonth() {
        return lastTimeByMonth;
    }

    /**
     * Check if last timestamp for monthly table is set.
     *
     * @return True if last timestamp for monthly table is set.
     */
    public boolean hasLastTimeByMonth() {
        return lastTimeByMonth != 0L;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (lastTimeUpdated > 0) {
            sb.append("Updated: ").append(lastTimeUpdated);
        }
        if (lastTimeByHour > 0) {
            sb.append(", LastHour: ").append(lastTimeByHour);
        }
        if (lastTimeByDay > 0) {
            sb.append(", LastDay: ").append(lastTimeByDay);
        }
        if (lastTimeByMonth > 0) {
            sb.append(", LastMonth: ").append(lastTimeByMonth);
        }
        return sb.toString();
    }
}
