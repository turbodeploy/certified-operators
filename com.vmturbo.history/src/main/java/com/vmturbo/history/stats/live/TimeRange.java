package com.vmturbo.history.stats.live;

import static com.vmturbo.components.common.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.stats.StatsHistoryRpcService.HEADROOM_STATS;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.Table;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.ClusterStatsReader;

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
         * @param statsFilter       The {@link StatsFilter}.
         * @param entityOIDsOpt     List of entities that we want to get the timeframe for
         * @param entityTypeOpt     Type of those entities
         * @param paginationParams  The option to use for getting the time based on pagination
         *                          sort commodity.
         * @param requiredTimeFrame the timeframe that must be used for resolution, if provided
         * @return An {@link Optional} containing the time range, or an empty optional if
         * there is no data in the time range specified by the filter.
         * @throws VmtDbException           If there is an error connecting to the database.
         * @throws IllegalArgumentException If the filter is misconfigured.
         */
        @Nonnull
        Optional<TimeRange> resolveTimeRange(@Nonnull StatsFilter statsFilter,
                @Nonnull final Optional<List<String>> entityOIDsOpt,
                @Nonnull final Optional<EntityType> entityTypeOpt,
                @Nonnull final Optional<EntityStatsPaginationParams> paginationParams,
                @Nonnull final Optional<TimeFrame> requiredTimeFrame)
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
                    @Nonnull final Optional<EntityStatsPaginationParams> paginationParams,
                    @Nonnull final Optional<TimeFrame> requiredTimeFrame)
                    throws IllegalArgumentException, VmtDbException {

                // assume that either both startTime and endTime are null, or startTime and endTime are set
                if (statsFilter.hasStartDate() != statsFilter.hasEndDate()) {
                    throw new IllegalArgumentException("one of 'startTime', 'endTime' null but not both: "
                        + statsFilter.getStartDate() + ":" + statsFilter.getEndDate());
                }

                long resolvedStartTime = -1;
                long resolvedEndTime = -1;
                List<Timestamp> timestampsInRange = null;
                TimeFrame timeFrame = null;

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
                            .getClosestTimestampBefore(statsFilter,
                                    Optional.empty(), requiredTimeFrame);

                    if (!mostRecentDbTimestamp.isPresent()) {
                        // no data persisted yet; just return an empty answer
                        return Optional.empty();
                    }
                    timestampsInRange = Collections.singletonList(mostRecentDbTimestamp.get());
                    resolvedStartTime = resolvedEndTime = mostRecentDbTimestamp.get().getTime();
                    // in this case we only considered a single timeframe table, so the time range
                    // needs to reflect that timeframe
                    timeFrame = requiredTimeFrame.orElse(TimeFrame.LATEST);

                } else {
                    // in this case we have a start and end date
                    Preconditions.checkArgument(statsFilter.hasEndDate());

                    timeFrame = requiredTimeFrame.orElseGet(() ->
                            getTimeFrame(statsFilter.getStartDate(), statsFilter));
                    List<TimeFrame> timeFrames = timeFrameCalculator.getAllRelevantTimeFrames(timeFrame);

                    // Iterate over all applicable time frames in chronological order
                    // (for example : latest->hourly->daily->monthly)
                    for (TimeFrame currentTimeFrame : timeFrames) {
                        if (!CollectionUtils.isEmpty(timestampsInRange) ||
                                requiredTimeFrame.map(tf -> tf != currentTimeFrame).orElse(false)) {
                            break;
                        }
                        timeFrame = currentTimeFrame;

                        // if the startTime and endTime are equal, and within the LATEST table window, open
                        // up the window a bit so that we will catch a stats value
                        if (statsFilter.getStartDate() == statsFilter.getEndDate()) {
                            // resolve the most recent time stamp with regard to the start date
                            Optional<Timestamp> closestTimestamp = historydbIO
                                    .getClosestTimestampBefore(statsFilter, Optional.of(statsFilter.getStartDate()),
                                            Optional.of(currentTimeFrame));
                            if (closestTimestamp.isPresent()) {
                                resolvedEndTime = statsFilter.getStartDate();
                                resolvedStartTime = closestTimestamp.get().getTime();
                                timestampsInRange = Collections.singletonList(closestTimestamp.get());
                            }
                        } else {
                            if (timeFrame == TimeFrame.MONTH && statsFilter.getStartDate() == statsFilter.getEndDate()) {
                                resolvedStartTime = resolveDateForMonthlyTimeFrame(statsFilter.getStartDate());
                                resolvedEndTime = resolvedStartTime;
                            } else {
                                resolvedStartTime = statsFilter.getStartDate();
                                resolvedEndTime = statsFilter.getEndDate();
                            }
                            timestampsInRange = historydbIO.getTimestampsInRange(timeFrame, resolvedStartTime,
                                    resolvedEndTime, entityTypeOpt, entityOidForQuery);
                        }
                    }
                }

                if (CollectionUtils.isEmpty(timestampsInRange)) {
                    // No data persisted in range.
                    return Optional.empty();
                } else {
                    // Now that we have the page of records, get all the records to use.
                    return Optional.of(new TimeRange(resolvedStartTime, resolvedEndTime, timeFrame, timestampsInRange));
                }

            }

            /**
             * Convert given timestamp to end of month date to query the db.
             * Sometimes, a timestamp meant to query monthly data will not match any records because
             * the timestamp sent by the UI is for the first day of the month,
             * while the monthly stats data is always saved with the last date of the month.
             * This happens when a monthly timestamp returned from a prior query appears in a subsequent correlated query,
             * since in responding to the first query, the timestamp was adjusted to beginning-of-month for presentation purposes.
             * @param startDate date to resolve.
             * @return date to end of month date w.r.t given startDate.
             */
            private long resolveDateForMonthlyTimeFrame(long startDate) {
                Calendar cal = Calendar.getInstance();
                cal.setTimeZone(TimeZone.getTimeZone("UTC"));
                cal.setTime(new Date(startDate));
                cal.add(Calendar.MONTH, 1);
                cal.add(Calendar.DATE, -1);
                return cal.getTimeInMillis();
            }

            @Nonnull
            private TimeFrame getTimeFrame(long resolvedStartTime,
                            @Nonnull StatsFilter statsFilter) {
                if (statsFilter.hasRollupPeriod()) {
                    return timeFrameCalculator.range2TimeFrame(statsFilter.getRollupPeriod(),
                                    RetentionPeriods.BOUNDARY_RETENTION_PERIODS);
                }
                return timeFrameCalculator.millis2TimeFrame(resolvedStartTime);
            }
        }

        /**
         * Time range factory suitable for use with cluster stats tables.
         */
        class ClusterTimeRangeFactory implements TimeRangeFactory {

            private final HistorydbIO historydbIO;
            private final TimeFrameCalculator timeFrameCalculator;

            /**
             * Create a new instance.
             *
             * @param historydbIO         DB stuff
             * @param timeFrameCalculator time frame calculator based on retention policies
             */
            public ClusterTimeRangeFactory(@Nonnull final HistorydbIO historydbIO,
                    @Nonnull final TimeFrameCalculator timeFrameCalculator) {
                this.historydbIO = historydbIO;
                this.timeFrameCalculator = timeFrameCalculator;
            }

            @Nonnull
            @Override
            public Optional<TimeRange> resolveTimeRange(
                    @Nonnull final StatsFilter statsFilter,
                    @Nonnull final Optional<List<String>> clusterIdsOpt,
                    // entity type is not relevant for cluster stats
                    @Nonnull final Optional<EntityType> unusedEntityType,
                    // pagination params curerntly not relevant
                    @Nonnull final Optional<EntityStatsPaginationParams> unusedPaginationParams,
                    @Nonnull final Optional<TimeFrame> requiredTimeFrame)
                    throws IllegalArgumentException, VmtDbException {

                TimeRange result = null;

                // start and end time must be either both present or both absent
                if (statsFilter.hasStartDate() != statsFilter.hasEndDate()) {
                    throw new IllegalArgumentException(
                            "Either start and end times must both be provided or neither must be.");
                }

                // we can only deal with a single specified cluster id
                Optional<String> clusterId = clusterIdsOpt.map(list -> list.get(0));

                if (!statsFilter.hasStartDate()) {
                    // no dates specified... default to using the most recent date available in
                    // given timeframe
                    final TimeFrame timeFrame = requiredTimeFrame.orElse(TimeFrame.LATEST);
                    final Timestamp latest = getMaxTimestamp(null, clusterId, statsFilter,
                            timeFrame);
                    if (latest != null) {
                        final long millis = latest.getTime();
                        result = new TimeRange(millis, millis, timeFrame, Collections.singletonList(latest));
                    }
                } else {
                    // in this case we have a start and end time. Check whether start time in the
                    // LATEST time window - computed time frame will be LATEST if so
                    // Except for special case for headroom stats, because they only ever appear
                    // in daily and monthly tables
                    TimeFrame timeFrame = requiredTimeFrame.orElseGet(() ->
                            timeFrameCalculator.millis2TimeFrame(statsFilter.getStartDate()));
                    if (requestsHeadroomStats(statsFilter) && (
                            timeFrame == TimeFrame.LATEST || timeFrame == TimeFrame.HOUR)) {
                        timeFrame = TimeFrame.DAY;
                    }
                    if (statsFilter.getStartDate() == statsFilter.getEndDate()) {
                        // equal timestamps, resolve to latest prior (or equal) timestamp in timeframe table
                        final Timestamp latest = getMaxTimestamp(statsFilter.getStartDate(),
                                clusterId, statsFilter, timeFrame);

                        if (latest != null) {
                            result = new TimeRange(latest.getTime(), statsFilter.getEndDate(),
                                    timeFrame, Collections.singletonList(latest));
                        }
                    } else {
                        // both times given, but they're different
                        final List<Timestamp> available = getClusterTimestamps(timeFrame,
                                statsFilter.getStartDate(), statsFilter.getEndDate(),
                                clusterId, statsFilter);
                        if (available.size() > 0) {
                            result = new TimeRange(
                                    available.get(0).getTime(), statsFilter.getEndDate(),
                                    timeFrame, available);
                        }
                    }
                }
                return Optional.ofNullable(result);
            }

            /**
             * Find latest timestamp for which a relevant record appears in the specified
             * cluster stats table
             *
             * <p>Record must be for the given cluster, and must specify a property type that
             * appears in as a requested commodity in the stats filter.</p>
             *
             * @param maxInclusiveMillis upper bound on returned timestamp, or null if none
             * @param clusterId          optional cluster id
             * @param statsFilter        stats filter, for requested commodities list
             * @param timeFrame          timeframe to consider
             * @return retrieved timestamp, or null if no records available
             * @throws VmtDbException for database error
             */
            private Timestamp getMaxTimestamp(@Nullable Long maxInclusiveMillis,
                    Optional<String> clusterId, StatsFilter statsFilter, TimeFrame timeFrame)
                    throws VmtDbException {
                // compute timestamp upperbound as a Timestamp, using current time if none supplied
                Timestamp maxInclusive = new Timestamp(maxInclusiveMillis != null ? maxInclusiveMillis
                        : System.currentTimeMillis());
                Table<?> table = ClusterStatsReader.getStatsTable(timeFrame);
                // gather conditions for the query
                List<Condition> conditions = new ArrayList<>();
                final Field<Timestamp> recordedOnField = getTimestampField(table, RECORDED_ON);
                final Field<String> internalNameField = getStringField(table, INTERNAL_NAME);
                final Field<String> propertyTypeField = getStringField(table, PROPERTY_TYPE);
                conditions.add(recordedOnField.le(maxInclusive));
                clusterId.ifPresent(id -> {
                    conditions.add(internalNameField.eq(id));
                });
                if (!statsFilter.getCommodityRequestsList().isEmpty()) {
                    conditions.add(propertyTypeField.in(
                            statsFilter.getCommodityRequestsList().stream()
                                    .map(req -> req.getCommodityName())
                                    .collect(Collectors.toList())));
                }
                // execute query to retrieve timestamp
                final Result<Record1<Timestamp>> result = (Result<Record1<Timestamp>>)
                        historydbIO.execute(HistorydbIO.getJooqBuilder()
                                .selectDistinct(recordedOnField)
                                .from(table)
                                .where(conditions)
                                .orderBy(recordedOnField.desc())
                                .limit(1));
                // return single result
                return result.size() > 0 ? result.get(0).value1() : null;
            }

            /**
             * Retrieve available timestamps from a cluster stats table.
             *
             * <p>Records may be required to be for a given cluster id, and/or to specify one of
             * a given set of property types</p>
             *
             * @param timeFrame        timeframe corresponding to table to be queried
             * @param minInclusiveMsec lower bound on timestamps
             * @param maxInclusiveMsec upper bound on timestamps
             * @param clusterId        optional cluster id
             * @param statsFilter      stats filter used for requested properties
             * @return list of available timestamps, in increasing order
             * @throws VmtDbException if a database error occurs
             */
            private List<Timestamp> getClusterTimestamps(TimeFrame timeFrame,
                    long minInclusiveMsec, long maxInclusiveMsec,
                    Optional<String> clusterId, StatsFilter statsFilter)
                    throws VmtDbException {
                // compute time bounds as Timestamp values
                final Timestamp minInclusive = new Timestamp(minInclusiveMsec);
                final Timestamp maxInclusive = new Timestamp(maxInclusiveMsec);
                // get cluster_stats table based on time frame
                Table<?> table = ClusterStatsReader.getStatsTable(timeFrame);
                // create field values we'll need
                final Field<Timestamp> recordedOnField = table.field(RECORDED_ON, Timestamp.class);
                final Field<String> internalNameField = table.field(INTERNAL_NAME, String.class);
                final Field<String> propertyTypeField = table.field(PROPERTY_TYPE, String.class);
                // collect conditions for the query
                List<Condition> conditions = new ArrayList<>();
                if (minInclusiveMsec == maxInclusiveMsec) {
                    // equal time bounds serves as an upper-limit only
                    conditions.add(recordedOnField.le(maxInclusive));
                } else {
                    conditions.add(recordedOnField.between(minInclusive, maxInclusive));
                }
                clusterId.ifPresent(id -> conditions.add(internalNameField.eq(id)));
                final List<String> propertyTypes = statsFilter.getCommodityRequestsList().stream()
                        .map(req -> req.getCommodityName()).collect(Collectors.toList());
                if (!propertyTypes.isEmpty()) {
                    conditions.add(propertyTypeField.in(propertyTypes));
                }
                // retrieve timestamps
                final Result<Record1<Timestamp>> result = (Result<Record1<Timestamp>>)
                        historydbIO.execute(historydbIO.getJooqBuilder()
                                .selectDistinct(recordedOnField)
                                .from(table)
                                .where(conditions)
                                .orderBy(recordedOnField.asc()));
                // and return them all
                return result.getValues(recordedOnField);
            }

            private boolean requestsHeadroomStats(StatsFilter statsFilter) {
                return statsFilter.getCommodityRequestsList().stream()
                        .anyMatch(req -> HEADROOM_STATS.contains(req.getCommodityName()));
            }
        }
    }
}
