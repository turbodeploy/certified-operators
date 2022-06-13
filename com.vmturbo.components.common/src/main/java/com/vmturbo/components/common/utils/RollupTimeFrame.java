package com.vmturbo.components.common.utils;

import java.time.temporal.TemporalAmount;
import java.util.function.Function;

import org.threeten.extra.Days;
import org.threeten.extra.Hours;
import org.threeten.extra.Minutes;
import org.threeten.extra.Months;
import org.threeten.extra.PeriodDuration;

/**
 * Time frames for which we keep either primary or rolled up historical data.
 */
public enum RollupTimeFrame {
    /**
     * Primary data, with arbitrary timestamps.
     *
     * <p>Termed "latest" throughout codebase because primary data is always the latest available
     * data at the time it is collected.</p>
     */
    LATEST(Minutes::of),
    /**
     * Hourly rolled up data, with timestamps at time :00:00 of the hour.
     */
    HOUR(Hours::of),
    /**
     * Daily rolled up data, with timestamps at time 00:00:00 (i.e. midnight) of day.
     */
    DAY(Days::of),
    /**
     * Monthly rolled up data, with timestamps at time 00:00:00 (i.e. midnight) of the last day of
     * the month.
     *
     * <p>Last day may seem an odd choice; it most likely reflects the fact that MySQL includes a
     * last_day() function but no first_day() function.</p>
     */
    MONTH(Months::of);

    private final Function<Integer, TemporalAmount> toTemporalAmountFn;

    RollupTimeFrame(Function<Integer, TemporalAmount> toTemporalAmountFn) {
        this.toTemporalAmountFn = toTemporalAmountFn;
    }

    /**
     * Obtain the duration of a period consisting a given number of this timeframe's associated
     * retention setting unit of time.
     *
     * @param n number of time units
     * @return PeriodDuration representing that number of time units
     */
    public PeriodDuration getPeriod(int n) {
        return PeriodDuration.from(toTemporalAmountFn.apply(n));
    }
}

