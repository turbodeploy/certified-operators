package com.vmturbo.history.stats.live;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.TimeFrame;
import com.vmturbo.history.db.VmtDbException;

/**
 * A {@link TimeRange} is a utility to encapsulate information about the start/end date in
 * a request for stats (via {@link StatsFilter}).
 */
public class TimeRange {

    private final long startTime;

    private final long endTime;

    private final TimeFrame timeFrame;

    private final List<Timestamp> timestampsInRange;

    private TimeRange(final long startTime,
                     final long endTime,
                     @Nonnull final TimeFrame timeFrame,
                     @Nonnull final List<Timestamp> timestampsInRange) {
        Preconditions.checkArgument(!timestampsInRange.isEmpty());
        this.startTime = startTime;
        this.endTime = endTime;
        this.timeFrame = Objects.requireNonNull(timeFrame);
        this.timestampsInRange = Objects.requireNonNull(timestampsInRange);
    }

    /**
     * Get the list of snapshot times in the range.
     *
     * @return The list of {@link Timestamp} objects that can be used to query specific
     *         snapshots. The list will be non-empty.
     */
    @Nonnull
    public List<Timestamp> getSnapshotTimesInRange() {
        return Collections.unmodifiableList(timestampsInRange);
    }

    /**
     * Get the timestamp of the most recent snapshot time in the range.
     *
     * @return The most recent {@link Timestamp} in the range.
     */
    @Nonnull
    public Timestamp getMostRecentSnapshotTime() {
        return timestampsInRange.get(0);
    }

    /**
     * Get the start time for this range.
     *
     * @return The start time, in epoch millis.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Get the end time for this range.
     *
     * @return The end time, in epoch millis.
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * Get the time frame for this range. The time frame determines which table to query
     * (e.g. the per-hour table or the per-month table).
     *
     * @return The {@link TimeFrame}.
     */
    @Nonnull
    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    /**
     * A factory used to create {@link TimeRange} instances in tests (via mocking) or production
     * (via {@link DefaultTimeRangeFactory}).
     */
    @FunctionalInterface
    public interface TimeRangeFactory {

        /**
         * Resolve the time range that a {@link StatsFilter} specifies.
         *
         * @param statsFilter The {@link StatsFilter}.
         * @return An {@link Optional} containing the time range, or an empty optional if
         *         there is no data in the time range specified by the filter.
         * @throws VmtDbException If there is an error connecting to the database.
         * @throws IllegalArgumentException If the filter is misconfigured.
         */
        @Nonnull
        Optional<TimeRange> resolveTimeRange(@Nonnull final StatsFilter statsFilter)
                throws IllegalArgumentException, VmtDbException;

        /**
         * The default implementation of {@link TimeRangeFactory} used in production.
         */
        class DefaultTimeRangeFactory implements TimeRangeFactory {

            private final HistorydbIO historydbIO;

            private final TimeFrameCalculator timeFrameCalculator;

            // time (MS) to specify a window before startTime; the config property is latestTableTimeWindowMin
            private final long latestTableTimeWindowMS;

            public DefaultTimeRangeFactory(@Nonnull final HistorydbIO historydbIO,
                                           @Nonnull final TimeFrameCalculator timeFrameCalculator,
                                           final long latestTableTimeWindow,
                                           final TimeUnit latestTableTimeWindowUnit) {
                this.historydbIO = Objects.requireNonNull(historydbIO);
                this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
                this.latestTableTimeWindowMS = latestTableTimeWindowUnit.toMillis(latestTableTimeWindow);
            }

            @Override
            @Nonnull
            public Optional<TimeRange> resolveTimeRange(@Nonnull final StatsFilter statsFilter)
                    throws IllegalArgumentException, VmtDbException {
                // assume that either both startTime and endTime are null, or startTime and endTime are set
                if (statsFilter.hasStartDate() != statsFilter.hasEndDate()) {
                    throw new IllegalArgumentException("one of 'startTime', 'endTime' null but not both: "
                            + statsFilter.getStartDate() + ":" + statsFilter.getEndDate());
                }

                final long resolvedStartTime;
                final long resolvedEndTime;
                final List<Timestamp> timestampsInRange;
                final TimeFrame timeFrame;
                if (!statsFilter.hasStartDate()) {
                    // if startTime / endtime are not set, use the most recent timestamp
                    // get most recent snapshot_time from _latest database
                    final Optional<Timestamp> mostRecentDbTimestamp = historydbIO.getMostRecentTimestamp();
                    if (!mostRecentDbTimestamp.isPresent()) {
                        // no data persisted yet; just return an empty answer
                        return Optional.empty();
                    }
                    timestampsInRange = Collections.singletonList(mostRecentDbTimestamp.get());
                    resolvedStartTime = resolvedEndTime = mostRecentDbTimestamp.get().getTime();
                    timeFrame = timeFrameCalculator.millis2TimeFrame(resolvedStartTime);
                } else {
                    Preconditions.checkArgument(statsFilter.hasEndDate());
                    // if the startTime and endTime are equal, and within the LATEST table window, open
                    // up the window a bit so that we will catch a stats value
                    timeFrame = timeFrameCalculator.millis2TimeFrame(statsFilter.getStartDate());
                    if (statsFilter.getStartDate() == statsFilter.getEndDate() &&
                            timeFrame.equals(TimeFrame.LATEST)) {
                        resolvedStartTime = statsFilter.getStartDate() - latestTableTimeWindowMS;
                    } else {
                        resolvedStartTime = statsFilter.getStartDate();
                    }
                    resolvedEndTime = statsFilter.getEndDate();
                    timestampsInRange = historydbIO.getTimestampsInRange(timeFrame, resolvedStartTime, resolvedEndTime);
                }

                if (timestampsInRange.isEmpty()) {
                    // No data persisted in range.
                    return Optional.empty();
                } else {
                    // Now that we have the page of records, get all the records to use.
                    return Optional.of(new TimeRange(resolvedStartTime, resolvedEndTime, timeFrame, timestampsInRange));
                }
            }
        }
    }

}
