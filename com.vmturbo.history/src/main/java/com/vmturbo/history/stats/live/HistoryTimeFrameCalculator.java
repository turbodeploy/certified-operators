package com.vmturbo.history.stats.live;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.TimeFrameCalculator;
import com.vmturbo.history.db.TimeFrame;

/**
 * A utility class to calculate the {@link TimeFrame} to use for a particular epoch millis
 * timestamp, depending on the stats retainment configuration in the history component.
 */
public class HistoryTimeFrameCalculator {
    private static final Logger logger = LogManager.getLogger();

    private TimeFrameCalculator timeFrameCalculator;

    public HistoryTimeFrameCalculator(@Nonnull final Clock clock,
                                      final int numRetainedMinutes,
                                      final int numRetainedHours,
                                      final int numRetainedDays) {
        this.timeFrameCalculator = new TimeFrameCalculator(clock,
            numRetainedMinutes, numRetainedHours, numRetainedDays);
    }

    /**
     * Clip a millisecond epoch number to a {@link TimeFrame}, e.g. LATEST, HOUR, DAY, etc. ago.
     *
     * @param millis a millisecond epoch number in the past
     * @return a {@link TimeFrame} representing how far in the past the given ms epoch number is.
     */
    public TimeFrame millis2TimeFrame(final long millis) {
        final TimeFrameCalculator.TimeFrame tf = timeFrameCalculator.millis2TimeFrame(millis);
        switch (tf) {
            case LATEST:
                return TimeFrame.LATEST;
            case HOUR:
                return TimeFrame.HOUR;
            case DAY:
                return TimeFrame.DAY;
            case MONTH:
                return TimeFrame.MONTH;
            default:
                logger.error("Unhandled time frame {} for millis {}. Returning MONTH", tf, millis);
                return TimeFrame.MONTH;
        }
    }
}
