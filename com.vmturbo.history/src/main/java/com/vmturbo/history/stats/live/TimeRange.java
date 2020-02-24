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
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
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
     * snapshots. The list will be non-empty.
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
         * Resolve the time range that a {@link StatsFilter} specifies for the commodity in
         * {@link EntityStatsPaginationParams} if it exists, or for the commodities in
         * {@link StatsFilter} commodity request list. In order to have more accurate results,
         * the method can also use the list of entities that we need the timeframe for,
         * and the type of those entities. Note that all the entities should be of the same type.
         *
         * @param statsFilter The {@link StatsFilter}.
         * @param entityOIDsOpt  List of entities that we want to get the timeframe for
         * @param entityTypeOpt  Type of those entities
         * @param paginationParams The option to use for getting the time based on pagination
         *                         sort commodity.
         * @return An {@link Optional} containing the time range, or an empty optional if
         * there is no data in the time range specified by the filter.
         * @throws VmtDbException           If there is an error connecting to the database.
         * @throws IllegalArgumentException If the filter is misconfigured.
         */
        @Nonnull
        Optional<TimeRange> resolveTimeRange(@Nonnull final StatsFilter statsFilter,
                                             @Nonnull final Optional<List<String>> entityOIDsOpt,
                                             @Nonnull final Optional<EntityType> entityTypeOpt,
                                             @Nonnull final Optional<EntityStatsPaginationParams> paginationParams)
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
            public Optional<TimeRange> resolveTimeRange(@Nonnull final StatsFilter statsFilter,
                                                        @Nonnull final Optional<List<String>> entityOIDsOpt,
                                                        @Nonnull final Optional<EntityType> entityTypeOpt,
                                                        @Nonnull final Optional<EntityStatsPaginationParams> paginationParams)
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

                // Right now we are only dealing with a single specific entity, not a group
                // of them. let's check if a single entity has been specified
                Optional<String> entityOidForQuery = Optional.empty();
                if (entityOIDsOpt.isPresent() && entityOIDsOpt.get().size() == 1) {
                    entityOidForQuery = Optional.of(entityOIDsOpt.get().get(0));
                }

                if (!statsFilter.hasStartDate()) {
                    // if startTime / endtime are not set, use the most recent timestamp
                    // in order to have a more accurate timestamp, we need to check it related to
                    // the entities that we are looking for, because especially if storing topology
                    // in db is taking a long time, different entities/entities types might have
                    // different "most recent "timestamps and we might not get data for some of them

                    final Optional<Timestamp> mostRecentDbTimestamp = historydbIO
                            .getClosestTimestampBefore(statsFilter, entityTypeOpt, entityOidForQuery,
                                    Optional.empty(), paginationParams);

                    if (!mostRecentDbTimestamp.isPresent()) {
                        // no data persisted yet; just return an empty answer
                        return Optional.empty();
                    }
                    timestampsInRange = Collections.singletonList(mostRecentDbTimestamp.get());
                    resolvedStartTime = resolvedEndTime = mostRecentDbTimestamp.get().getTime();
                    timeFrame = getTimeFrame(resolvedStartTime, statsFilter);

                } else {
                    // in this case we have a start and end date

                    Preconditions.checkArgument(statsFilter.hasEndDate());
                    // if the startTime and endTime are equal, and within the LATEST table window, open
                    // up the window a bit so that we will catch a stats value
                    timeFrame = getTimeFrame(statsFilter.getStartDate(), statsFilter);

                    if (statsFilter.getStartDate() == statsFilter.getEndDate() &&
                        timeFrame.equals(TimeFrame.LATEST)) {
                        // resolve the most recent time stamp with regard to the start date
                        Optional<Timestamp> closestTimestamp = historydbIO
                                .getClosestTimestampBefore(statsFilter, entityTypeOpt, entityOidForQuery,
                                        Optional.of(statsFilter.getStartDate()), paginationParams);
                        if (!closestTimestamp.isPresent()) {
                            // no data persisted yet; just return an empty answer
                            return Optional.empty();
                        } else {
                            resolvedEndTime = statsFilter.getStartDate();
                            resolvedStartTime = closestTimestamp.get().getTime();
                            timestampsInRange = Collections.singletonList(closestTimestamp.get());
                        }
                    } else {
                        resolvedStartTime = statsFilter.getStartDate();
                        resolvedEndTime = statsFilter.getEndDate();
                        timestampsInRange = historydbIO.getTimestampsInRange(timeFrame, resolvedStartTime,
                            resolvedEndTime, entityTypeOpt, entityOidForQuery);
                    }
                }

                if (timestampsInRange.isEmpty()) {
                    // No data persisted in range.
                    return Optional.empty();
                } else {
                    // Now that we have the page of records, get all the records to use.
                    return Optional.of(new TimeRange(resolvedStartTime, resolvedEndTime, timeFrame, timestampsInRange));
                }

            }

            @Nonnull
            private TimeFrame getTimeFrame(long resolvedStartTime,
                            @Nonnull StatsFilter statsFilter) {
                if (statsFilter.hasRollupPeriod()) {
                    return timeFrameCalculator.millis2TimeFrame(statsFilter.getRollupPeriod());
                }
                return timeFrameCalculator.millis2TimeFrame(resolvedStartTime);
            }
        }
    }

}
