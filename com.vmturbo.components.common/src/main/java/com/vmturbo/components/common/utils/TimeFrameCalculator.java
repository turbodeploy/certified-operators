package com.vmturbo.components.common.utils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * A utility class to calculate the {@link TimeFrame} to use for a particular epoch millis
 * timestamp, depending on the stats retainment configuration.
 */
public class TimeFrameCalculator {
    private final Clock clock;

    private final RetentionPeriodFetcher retentionPeriodFetcher;

    public TimeFrameCalculator(@Nonnull final Clock clock,
                               @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher) {
        this.clock = Objects.requireNonNull(clock);
        this.retentionPeriodFetcher = Objects.requireNonNull(retentionPeriodFetcher);
    }

    /**
     * Clip a millisecond epoch number to a time frame.
     *
     * @param startTimestamp a millisecond epoch number in the past
     * @return a time frame name.
     */
    @Nonnull
    public TimeFrame millis2TimeFrame(final long startTimestamp) {
        final Duration timeBack =
                        Duration.between(Instant.ofEpochMilli(startTimestamp), clock.instant());
        return range2TimeFrame(timeBack.toMillis(), retentionPeriodFetcher.getRetentionPeriods());
    }

    /**
     * Converts a range represented in milliseconds into {@link TimeFrame} instance.
     *
     * @param range range in milliseconds
     * @param retentionPeriods retention periods used for as a borders to detect the
     *                 smallest {@link TimeFrame}.
     * @return a time frame name.
     */
    @Nonnull
    public TimeFrame range2TimeFrame(long range, @Nonnull RetentionPeriods retentionPeriods) {
        // no start date was mentioned - use latest
        if (range == 0) {
            return TimeFrame.LATEST;
        }
        final Duration duration = Duration.ofMillis(range);
        if (duration.toMinutes() <= retentionPeriods.latestRetentionMinutes()) {
            return TimeFrame.LATEST;
        }
        if (duration.toHours() <= retentionPeriods.hourlyRetentionHours()) {
            return TimeFrame.HOUR;
        }
        if (duration.toDays() <= retentionPeriods.dailyRetentionDays()) {
            return TimeFrame.DAY;
        }
        return TimeFrame.MONTH;
    }

    /**
     * It is possible that one timestamp lies in more than one tables related to timeframe.
     * For example, a timestamp an hour ago (LATEST timeframe) from current time has potential to be in hour/day/month
     * table. Hence, for such a case we return all such time frames.
     * @param timeFrame current time frame to apply.
     * @return all time frames where this timeframe can possibly fit.
     */
    public List<TimeFrame> getAllRelevantTimeFrames(TimeFrame timeFrame) {
        List<TimeFrame> timeFrames = new ArrayList<>();
        // NOTE : Break statements are intentionally avoided after every case.
        switch (timeFrame) {
            case LATEST:
                timeFrames.add(TimeFrame.LATEST);
            case HOUR:
                timeFrames.add(TimeFrame.HOUR);
            case DAY:
                timeFrames.add(TimeFrame.DAY);
            case MONTH:
                timeFrames.add(TimeFrame.MONTH);
                break;
            default:
                timeFrames.add(TimeFrame.MONTH);
        }
        return timeFrames;
    }




}
