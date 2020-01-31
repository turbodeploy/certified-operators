package com.vmturbo.history.schema;

/**
 * Time frames for which we keep either primary or rolled up historical data.
 */
public enum TimeFrame {
    /**
     * Primary data, with arbitrary timestamps.
     *
     * <p>Termed "latest" throughout codebase because primary data is always the latest available data at the time
     * it is collected.</p>
     */
    LATEST,
    /**
     * Hourly rolled up data, with timestamps at time :00:00 of the hour.
     */
    HOUR,
    /**
     * Daily rolled up data, with timestamps at time 00:00:00 (i.e. midnight) of day.
     */
    DAY,
    /**
     * Monthly rolled up data, with timestamps at time 00:00:00 (i.e. midnight) of the last day of the month.
     *
     * <p>Last day may seem an odd choice; it most likely reflects the fact that MySQL includes a last_day() function
     * but no first_day() function.</p>
     */
    MONTH
}
