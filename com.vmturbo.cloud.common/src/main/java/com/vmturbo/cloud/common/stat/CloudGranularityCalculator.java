package com.vmturbo.cloud.common.stat;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;

/**
 * A wrapper for {@link TimeFrameCalculator}, used to convert request durations to a cloud-specific
 * granularity. {@link CloudStatGranularity} does not have a concept of latest and is a protobuf
 * definition, potentially included as part of the request.
 */
public class CloudGranularityCalculator {

    //TODO: Currently we only support daily and monthly granularity so if the granularity is determined
    // to be hourly, we should look up the daily table. Once we add support for hourly granularity, this
    // should be changed to looking up the hourly table.
    private static final Map<TimeFrame, CloudStatGranularity> TIME_FRAME_TO_GRANULARITY_MAP = ImmutableMap.of(
            TimeFrame.LATEST, CloudStatGranularity.HOURLY,
            TimeFrame.HOUR, CloudStatGranularity.DAILY,
            TimeFrame.DAY, CloudStatGranularity.DAILY,
            TimeFrame.MONTH, CloudStatGranularity.MONTHLY,
            // If the window requested resolves to a yearly time frame, we return
            // monthly data.
            TimeFrame.YEAR, CloudStatGranularity.MONTHLY);

    private final TimeFrameCalculator timeFrameCalculator;

    /**
     * Constructs a new {@link CloudGranularityCalculator}, wrapping {@code timeFrameCalculator}.
     * @param timeFrameCalculator The {@link TimeFrameCalculator} to wrap.
     */
    public CloudGranularityCalculator(@Nonnull TimeFrameCalculator timeFrameCalculator) {
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
    }

    /**
     * Determines the {@link CloudStatGranularity}, based on the start time requested.
     * @param startTime The start time of the request.
     * @return The cloud stat granularity.
     */
    @Nonnull
    public CloudStatGranularity startTimeToGranularity(@Nonnull Instant startTime) {

        final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(startTime.toEpochMilli());

        return TIME_FRAME_TO_GRANULARITY_MAP.getOrDefault(timeFrame, CloudStatGranularity.HOURLY);
    }

}
