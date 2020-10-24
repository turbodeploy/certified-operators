package com.vmturbo.cloud.commitment.analysis.util;

import java.time.Duration;
import java.time.Instant;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.common.data.TimeInterval;

/**
 * A utility class for doing time math.
 */
public class TimeCalculator {

    private TimeCalculator() {}

    /**
     * Divides {@code dividend} by {@code divisor}, flooring the result.
     * @param dividend The dividend.
     * @param divisor The divisor.
     * @return The floored division result.
     */
    public static long flooredDivision(@Nonnull Duration dividend,
                                       @Nonnull Duration divisor) {

        Preconditions.checkNotNull(dividend);
        Preconditions.checkNotNull(divisor);

        return dividend.toNanos() / divisor.toNanos();
    }

    /**
     * Determines the overlap between {@code intervalA} and {@code intervalB}. Will return a duration
     * of zero if the intervals do not overlap.
     * @param intervalA The first time interval.
     * @param intervalB The second time interval.
     * @return The overlap duration between the two time intervals.
     */
    @Nonnull
    public static Duration overlap(@Nonnull TimeInterval intervalA,
                                   @Nonnull TimeInterval intervalB) {

        Preconditions.checkNotNull(intervalA);
        Preconditions.checkNotNull(intervalB);

        final Instant startTime = intervalA.startTime().isAfter(intervalB.startTime())
                ? intervalA.startTime()
                : intervalB.startTime();

        final Instant endTime = intervalA.endTime().isBefore(intervalB.endTime())
                ? intervalA.endTime()
                : intervalB.endTime();

        return startTime.isBefore(endTime)
                ? Duration.between(startTime, endTime)
                : Duration.ZERO;
    }

    /**
     * Divides {@code dividend} by {@code divisor}.
     * @param dividend The dividend.
     * @param divisor The divisor.
     * @return The division result.
     */
    @Nonnull
    public static double divide(@Nonnull Duration dividend,
                                @Nonnull Duration divisor) {

        Preconditions.checkNotNull(dividend);
        Preconditions.checkNotNull(divisor);

        return ((double)dividend.toNanos()) / divisor.toNanos();
    }
}
