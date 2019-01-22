package com.vmturbo.cost.component.reserved.instance;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

public class TimeFrameCalculator {
    private final static long MINUTE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private final static long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);

    private final Clock clock;

    private final int numRetainedMinutes;
    private final int numRetainedHours;
    private final int numRetainedDays;

    public TimeFrameCalculator(@Nonnull final Clock clock,
                               final int numRetainedMinutes,
                               final int numRetainedHours,
                               final int numRetainedDays) {
        this.clock = clock;
        this.numRetainedMinutes = numRetainedMinutes;
        this.numRetainedHours = numRetainedHours;
        this.numRetainedDays = numRetainedDays;
    }

    /**
     * Clip a millisecond epoch number to a time frame.
     *
     * @param millis a millisecond epoch number in the past
     * @return a time frame name.
     */
    public TimeFrame millis2TimeFrame(final long millis) {
        final long timeBackMillis = clock.millis() - millis;
        checkArgument(timeBackMillis > 0);
        final long tMinutesBack = timeBackMillis / MINUTE_MILLIS;
        if (tMinutesBack <= numRetainedMinutes) {
            return TimeFrame.LATEST;
        }
        final long tHoursBack = timeBackMillis / HOUR_MILLIS;
        if (tHoursBack <= numRetainedHours) {
            return TimeFrame.HOUR;
        }
        if (tHoursBack / 24 <= numRetainedDays) {
            return TimeFrame.DAY;
        }
        return TimeFrame.MONTH;
    }

    public enum TimeFrame {
        LATEST,
        HOUR,
        DAY,
        MONTH,
    }
}
