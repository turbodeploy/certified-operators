package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU_EXHAUSTION;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.HEADROOM_VMS;
import static com.vmturbo.common.protobuf.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM_EXHAUSTION;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE_EXHAUSTION;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.TOTAL_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.USED;
import static com.vmturbo.common.protobuf.utils.StringConstants.VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.VM_GROWTH;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsChunk;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.stats.StatsUtils;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.queries.ClusterStatsQuery;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ClusterRecordsProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;
import com.vmturbo.history.stats.live.TimeRange;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.ClusterTimeRangeFactory;

/**
 * This class retrieves records from cluster_stats_* tables to satisfy API requests.
 *
 * <p>A timeframe is computed from the start and end times, and that determines which table is
 * queried.</p>
 */
public class ClusterStatsReader {
    private static final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;
    private final ClusterTimeRangeFactory timeRangeFactory;
    private final ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory;

    /**
     * These stats have two DB records per observation:
     * one for usage and one for capacity.
     * Containment in this set is case-insensitive.
     */
    private static final Set<String> STATS_STORED_IN_TWO_RECORDS;
    private int maxAmountOfEntitiesPerGrpcMessage;

    /**
     * These stats are not stored on the "latest" and "hourly" table.
     * They are only sampled daily.
     * Containment in this set is case-insensitive.
     */
    public static final Set<String> STATS_STORED_DAILY;

    /**
     * For these stats we calculate projections.
     * Containment in this set is case-insensitive.
     */
    private static final Set<String> PROJECTED_STATS;

    static {
        final SortedSet<String> statsStoredInTwoRecords = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        statsStoredInTwoRecords.add(MEM);
        statsStoredInTwoRecords.add(CPU);
        statsStoredInTwoRecords.add(CPU_HEADROOM);
        statsStoredInTwoRecords.add(MEM_HEADROOM);
        statsStoredInTwoRecords.add(STORAGE_HEADROOM);
        statsStoredInTwoRecords.add(TOTAL_HEADROOM);
        STATS_STORED_IN_TWO_RECORDS = Collections.unmodifiableSortedSet(statsStoredInTwoRecords);

        final SortedSet<String> projectedStats = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        projectedStats.add(CPU_HEADROOM);
        projectedStats.add(MEM_HEADROOM);
        projectedStats.add(STORAGE_HEADROOM);
        projectedStats.add(TOTAL_HEADROOM);
        PROJECTED_STATS = Collections.unmodifiableSet(projectedStats);

        final SortedSet<String> statsStoredDaily = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        statsStoredDaily.addAll(projectedStats);
        statsStoredDaily.add(VM_GROWTH);
        statsStoredDaily.add(MEM_EXHAUSTION);
        statsStoredDaily.add(CPU_EXHAUSTION);
        statsStoredDaily.add(STORAGE_EXHAUSTION);
        statsStoredDaily.add(HEADROOM_VMS);
        STATS_STORED_DAILY = Collections.unmodifiableSet(statsStoredDaily);
    }

    /**
     * Default pagination.
     */
    private static final OrderBy DEFAULT_ORDER_BY = OrderBy.newBuilder()
                                                        .setEntityStats(EntityStatsOrderBy.newBuilder()
                                                                            .setStatName(INTERNAL_NAME))
                                                        .build();
    private static final PaginationParameters DEFAULT_PAGINATION = PaginationParameters.newBuilder()
                                                                        .setAscending(false)
                                                                        .setCursor("0")
                                                                        .setOrderBy(DEFAULT_ORDER_BY)
                                                                        .setLimit(Integer.MAX_VALUE)
                                                                        .build();

    private static final long ONE_HOUR_IN_MILLIS = 3_600_000;
    private static final long ONE_DAY_IN_MILLIS = 86_400_000;
    private static final long ONE_MONTH_IN_MILLIS = 2_592_000_000L;
    private static final long ONE_MONTH_IN_DAYS = 30;

    /**
     * Create a new instance.
     * @param historydbIO                        Access to some DB utilities
     * @param timeRangeFactory                   an instance of ClusterTimeRangeFactory used to
     *                                           determine time frame for query results
     * @param computedPropertiesProcessorFactory factory for processors tohandle computed properties
     * @param maxAmountOfEntitiesPerGrpcMessage max amount of entities in a grpc chunk
     */
    ClusterStatsReader(HistorydbIO historydbIO, ClusterTimeRangeFactory timeRangeFactory,
                       ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory,
                       final int maxAmountOfEntitiesPerGrpcMessage) {
        this.historydbIO = historydbIO;
        this.timeRangeFactory = timeRangeFactory;
        this.computedPropertiesProcessorFactory = computedPropertiesProcessorFactory;
        this.maxAmountOfEntitiesPerGrpcMessage = maxAmountOfEntitiesPerGrpcMessage;
    }

    /**
     * Obtain records from the appropriate cluster stats table to satisfy an API request.
     *
     * <p>Records are returned in a wrapper that provides uniform access to record data regardless
     * of the specific cluster_stats_* table from which the records were retrieved.</p>
     *
     * @param clusterUuid   cluster ID
     * @param startTime     beginning of time range of interest
     * @param endTime       end of time range of interest
     * @param propertyTypes property types of interest
     * @param timeFrame     an optional time frame, to be used when picking
     *                      the appropriate DB table for lookup
     * @return retrieved records, each wrapped in a {@link ClusterStatsRecordReader} instance.
     * @throws VmtDbException if retrieval fails
     */
    List<ClusterStatsRecordReader> getStatsRecordsForHeadRoomPlanRequest(
            long clusterUuid, long startTime, long endTime, Optional<TimeFrame> timeFrame,
            @Nonnull Set<String> propertyTypes) throws VmtDbException {
        Objects.requireNonNull(propertyTypes);
        StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(startTime)
                .setEndDate(endTime)
                .addAllCommodityRequests(propertyTypes.stream()
                        .map(prop -> CommodityRequest.newBuilder().setCommodityName(prop).build())
                        .collect(Collectors.toList()))
                .build();
        final ComputedPropertiesProcessor computedPropertiesProcessor =
                computedPropertiesProcessorFactory.getProcessor(statsFilter, new ClusterRecordsProcessor());
        final StatsFilter augmentedFilter = computedPropertiesProcessor.getAugmentedFilter();
        Optional<TimeRange> timeRange = timeRangeFactory.resolveTimeRange(augmentedFilter,
                Optional.of(Collections.singletonList(Long.toString(clusterUuid))),
                Optional.empty(), Optional.empty(), timeFrame);
        if (timeRange.isPresent()) {
            final Set<String> augmentedPropertyTypes = augmentedFilter.getCommodityRequestsList().stream()
                    .map(CommodityRequest::getCommodityName)
                    .collect(Collectors.toSet());
            final Result<? extends Record> records = getStatsRecordsForHeadRoomPlanRequest(clusterUuid, augmentedPropertyTypes, timeRange.get());
            final Timestamp defaultTimestamp = timeRange.map(TimeRange::getMostRecentSnapshotTime).orElse(null);
            final List<Record> result = computedPropertiesProcessor.processResults(records, defaultTimestamp);
            return result.stream().map(ClusterStatsRecordReader::new).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Obtain records from the appropriate cluster stats table to satisfy an API request.
     *
     * <p>Records are returned in a wrapper that provides uniform access to record data regardless
     * of the specific cluster_stats_* table from which the records were retrieved.</p>
     *
     * @param clusterUuid   cluster id
     * @param propertyTypes property types of interest
     * @param timeRange     time range determined from start end end times
     * @return retrieved records, each wrapped in a {@link ClusterStatsRecordReader} instance
     * @throws VmtDbException if retrieval fails
     */
    private Result<? extends Record> getStatsRecordsForHeadRoomPlanRequest(long clusterUuid, Set<String> propertyTypes, TimeRange timeRange)
            throws VmtDbException {
        Table<?> table = getStatsTable(timeRange.getTimeFrame());
        List<Condition> conditions = new ArrayList<>();
        conditions.add(getStringField(table, INTERNAL_NAME).eq(Long.toString(clusterUuid)));
        // Don't add inCommodityNames condition if commodityNames is empty.
        if (!propertyTypes.isEmpty()) {
            conditions.add(getStringField(table, PROPERTY_TYPE).in(propertyTypes));
        }
        conditions.add(getTimestampField(table, RECORDED_ON).between(
                new Timestamp(timeRange.getStartTime()), new Timestamp(timeRange.getEndTime())));

        return historydbIO.execute(Style.FORCED, historydbIO.JooqBuilder().selectFrom(table)
                                                        .where(conditions).getQuery());
    }

    /**
     * Get the cluster_stats table for the given time frame.
     *
     * @param timeFrame time frame
     * @return corresponding cluster_stats table
     */
    public static Table<?> getStatsTable(TimeFrame timeFrame) {
        switch (timeFrame) {
            case LATEST:
                return CLUSTER_STATS_LATEST;
            case HOUR:
                return CLUSTER_STATS_BY_HOUR;
            case DAY:
                return Tables.CLUSTER_STATS_BY_DAY;
            case MONTH:
                return Tables.CLUSTER_STATS_BY_MONTH;
            default:
                throw new IllegalArgumentException(
                        "Illegal time frame for cluster stats: " + timeFrame.name());
        }
    }

    /**
     * Responds to a {@link ClusterStatsRequest} coming from the gRPC service.
     * The response is returned (or an exception is thrown). The stream observer
     * will be used by the caller.
     *
     * @param request the gRPC request
     * @return the gRPC response
     * @throws VmtDbException when the query to the database fails
     */
    @Nonnull
    public List<ClusterStatsResponse> getStatsRecords(@Nonnull ClusterStatsRequest request) throws VmtDbException {
        // collector of stats
        final Map<Long, SingleClusterStats> statsPerCluster = new HashMap<>();

        // extract and validate dates
        long now = System.currentTimeMillis();
        final StatsFilter filter = request.hasStats() ? request.getStats()
                                                      : StatsFilter.getDefaultInstance();
        final Optional<Long> startDate = filter != null && filter.hasStartDate()
                                                ? Optional.of(filter.getStartDate()) : Optional.empty();
        final Optional<Long> endDate = filter != null && filter.hasEndDate()
                                                ? Optional.of(filter.getEndDate()) : Optional.empty();
        if ((endDate.isPresent() && !startDate.isPresent())
                || startDate.orElse(now) > Math.min(now, endDate.orElse(now))) {
            throw new IllegalArgumentException("Invalid date range for retrieving cluster statistics.");
        }

        // extract cluster ids
        final Set<Long> clusterIds = request.getClusterIdsList().stream().collect(Collectors.toSet());

        // extract the pagination parameters
        final PaginationParameters paginationParameters;
        if (request.hasPaginationParams() && request.getPaginationParams().hasOrderBy()
                && request.getPaginationParams().getOrderBy().hasEntityStats()
                && request.getPaginationParams().getOrderBy().getEntityStats().hasStatName()) {
            paginationParameters = request.getPaginationParams();
        } else {
            logger.warn("Missing sort-by stat for cluster stats request");
            paginationParameters = DEFAULT_PAGINATION;
        }
        final String orderByStat = paginationParameters.getOrderBy().getEntityStats().getStatName();

        // generate computed properties processor and augmented filter
        final ComputedPropertiesProcessor computedPropertiesProcessor =
                computedPropertiesProcessorFactory.getProcessor(filter, new ClusterRecordsProcessor());
        final StatsFilter augmentedFilter = computedPropertiesProcessor.getAugmentedFilter();

        // extract requested fields (stats)
        final Set<String> requestedStats = StatsUtils.collectCommodityNames(augmentedFilter);
        final Map<Boolean, Set<String>> splitRequestedStats =
            requestedStats.stream()
                .collect(Collectors.partitioningBy(STATS_STORED_DAILY::contains, Collectors.toSet()));
        final Set<String> requestedDailyStats = splitRequestedStats.get(true);
        final Set<String> requestedNonDailyStats = splitRequestedStats.get(false);

        // set some flags related to the request
        final boolean allStatsAreRequested = requestedStats.isEmpty();
        final boolean dailyStatsAreRequested = allStatsAreRequested || !requestedDailyStats.isEmpty();
        final boolean nonDailyStatsAreRequested = allStatsAreRequested || !requestedNonDailyStats.isEmpty();
        final boolean projectedStatsAreIncludedInTheRequest =
                allStatsAreRequested || requestedDailyStats.stream().anyMatch(PROJECTED_STATS::contains);
        final boolean oneDataPointRequested = !startDate.isPresent();
        final boolean includeProjectedStats = request.getStats().getRequestProjectedHeadroom()
                                                    && !oneDataPointRequested
                                                    && projectedStatsAreIncludedInTheRequest
                                                    && roundToNextDay(endDate.orElse(now)) >= now;

        // fetch data from the database and ingest them
        if (dailyStatsAreRequested) {
            fetchAndIngestDailyStats(computedPropertiesProcessor, statsPerCluster, clusterIds,
                                     requestedDailyStats, startDate, endDate, includeProjectedStats);
        }
        if (nonDailyStatsAreRequested) {
            fetchAndIngestNonDailyStats(statsPerCluster, clusterIds, requestedNonDailyStats, startDate, endDate);
        }

        // add projections
        if (includeProjectedStats) {
            statsPerCluster.values().forEach(s -> s.addProjectedStats(roundToNextDay(endDate.orElse(now))));
        }

        // paginate
        final EntityStatsPaginationParams entityStatsPaginationParams =
            new EntityStatsPaginationParams(maxAmountOfEntitiesPerGrpcMessage,
                                            paginationParameters.getLimit(), "", paginationParameters);
        final Function<SingleClusterStats, Float> comparisonFunction =
                SingleClusterStats.getComparisonFunction(orderByStat, paginationParameters.getAscending());
        final PaginatedStats paginatedStats =
                new EntityStatsPaginator()
                        .paginate(statsPerCluster.keySet(),
                                  oid -> Optional.of(comparisonFunction.apply(statsPerCluster.get(oid))),
                                  entityStatsPaginationParams);
        final PaginationResponse paginationResponse = paginatedStats.getPaginationResponse();
        final List<EntityStats> entityStats =
                paginatedStats.getNextPageIds().stream()
                    .map(id -> statsPerCluster.get(id).toEntityStats(oneDataPointRequested))
                    .collect(Collectors.toList());

        // return result
        return createClusterStatsResponseList(entityStats, paginationResponse);
    }

    private void fetchAndIngestNonDailyStats(@Nonnull Map<Long, SingleClusterStats> statsPerCluster,
                                             @Nonnull Set<Long> clusterIds, @Nonnull Set<String> fields,
                                             @Nonnull Optional<Long> startDate,
                                             @Nonnull Optional<Long> endDate)
            throws VmtDbException {
        // find tables to query
        // assumptions on the input:
        // if start date exists, it is in the past and the end date exists
        // and the end date is later than the start date
        long now = System.currentTimeMillis();
        final Set<Table<?>> dbTablesToQuery;
        if (!startDate.isPresent() && !endDate.isPresent()) {
            // only latest point is requested
            dbTablesToQuery = Collections.singleton(Tables.CLUSTER_STATS_LATEST);
        } else {
            dbTablesToQuery = new HashSet<>();
            final long endDateUsed = endDate.orElse(now);
            final long startDateUsed = startDate.get(); // guaranteed to exist
            final long oneHourAgo = now - ONE_HOUR_IN_MILLIS;
            final long yesterday = now - ONE_DAY_IN_MILLIS;

            // should the latest stats be included?
            if (endDateUsed >= oneHourAgo) {
                dbTablesToQuery.add(CLUSTER_STATS_LATEST);
            }

            // should the hourly stats be included?
            if (endDateUsed >= yesterday && startDateUsed < oneHourAgo) {
                dbTablesToQuery.add(CLUSTER_STATS_BY_HOUR);
            }

            // should the daily stats be included?
            if (startDateUsed < yesterday) {
                dbTablesToQuery.add(CLUSTER_STATS_BY_DAY);
            }

            // should the monthly stats be included?
            if (startDateUsed < now - ONE_MONTH_IN_MILLIS) {
                dbTablesToQuery.add(CLUSTER_STATS_BY_MONTH);
            }
        }

        // timestamps for the query
        final Optional<Timestamp> startTimestamp = startDate.map(Timestamp::new);
        final Optional<Timestamp> endTimestamp = endDate.map(Timestamp::new);

        // execute query on all applicable tables and
        // insert the results into the statsPerCluster map
        for (Table<?> t : dbTablesToQuery) {
            fetchAndIngestDBRecords(null, statsPerCluster, clusterIds, fields,
                                    startTimestamp, endTimestamp, false, t);
        }
    }

    private void fetchAndIngestDailyStats(@Nullable ComputedPropertiesProcessor computedPropertiesProcessor,
                                          @Nonnull Map<Long, SingleClusterStats> statsPerCluster,
                                          @Nonnull Set<Long> clusterIds, @Nonnull Set<String> fields,
                                          @Nonnull Optional<Long> startDate, @Nonnull Optional<Long> endDate,
                                          boolean includeProjectedStats)
            throws VmtDbException {
        // find tables to query
        // assumptions on the input:
        // if start date exists, it is in the past and the end date exists
        // and the end date is later than the start date
        long now = System.currentTimeMillis();
        final Set<Table<?>> dbTablesToQuery = new HashSet<>();
        dbTablesToQuery.add(CLUSTER_STATS_BY_DAY);
        if (startDate.map(d -> d < now - ONE_MONTH_IN_MILLIS).orElse(false)) {
            dbTablesToQuery.add(CLUSTER_STATS_BY_MONTH);
        }

        // timestamps for the query
        // round start time to previous day and end time to next day
        final Optional<Timestamp> startTimestamp = startDate.map(ClusterStatsReader::roundToPreviousDay)
                                                            .map(Timestamp::new);
        final Optional<Timestamp> endTimestamp = endDate.map(ClusterStatsReader::roundToNextDay)
                                                        .map(Timestamp::new);

        // execute query on all applicable tables and
        // insert the results into the statsPerCluster map
        for (Table<?> t : dbTablesToQuery) {
            fetchAndIngestDBRecords(computedPropertiesProcessor, statsPerCluster, clusterIds,
                                    fields, startTimestamp, endTimestamp, includeProjectedStats, t);
        }
    }

    private void fetchAndIngestDBRecords(@Nullable ComputedPropertiesProcessor computedPropertiesProcessor,
                                         @Nonnull Map<Long, SingleClusterStats> statsPerCluster,
                                         @Nonnull Set<Long> clusterIds, @Nonnull Set<String> fields,
                                         @Nonnull Optional<Timestamp> startTimestamp,
                                         @Nonnull Optional<Timestamp> endTimestamp,
                                         boolean includeProjectedStats, @Nonnull Table<?> table)
            throws VmtDbException {
        // if we want projections, we need to fetch the latest VM growth from the DB
        // this flag is raised if the VM growth must be added to the requested fields
        // (note that the flag is false if we don't *need* to include VM growth, which
        // can be either because we don't want projections or because VM growth is already
        // among the required fields)
        // if the flag is true, we later need to remove VM growth data from the result
        final boolean vmGrowthMustBeAddedToQuery = includeProjectedStats && !fields.isEmpty()
                                                      && fields.stream()
                                                            .noneMatch(s -> s.equalsIgnoreCase(VM_GROWTH));

        // add VM growth to the fields, if we need to
        final Set<String> queryFields;
        if (vmGrowthMustBeAddedToQuery) {
            queryFields = new HashSet<>(fields);
            queryFields.add(VM_GROWTH);
        } else {
            queryFields = fields;
        }

        // construct and execute query
        final ClusterStatsQuery query = new ClusterStatsQuery(table, startTimestamp, endTimestamp,
                                                              queryFields, clusterIds);
        final Result<? extends Record> queryResults = historydbIO.execute(query.getQuery());

        // post-process the results of the query, if a computed-properties processor is available
        final Iterable<? extends Record> processedQueryResults;
        if (computedPropertiesProcessor != null) {
            processedQueryResults = computedPropertiesProcessor.processResults(queryResults, null);
        } else {
            processedQueryResults = queryResults;
        }

        // ingest query results
        for (Record r : processedQueryResults) {
            final long id = Long.valueOf(r.get(INTERNAL_NAME, String.class));
            statsPerCluster.computeIfAbsent(
                    id, k -> new SingleClusterStats(k, includeProjectedStats, vmGrowthMustBeAddedToQuery))
                .ingestRecord(r);
        }
    }

    private static long roundToPreviousDay(long time) {
        return (time / ONE_DAY_IN_MILLIS) * ONE_DAY_IN_MILLIS;
    }

    private static long roundToNextDay(long time) {
        long previous = roundToPreviousDay(time);
        if (time > previous) {
            return previous + ONE_DAY_IN_MILLIS;
        } else {
            return previous;
        }
    }

    /**
     * This class collects stats that concern a single cluster,
     * observed at a specific point in time.
     */
    private static class SingleClusterSingleTimeStats {
        private final Timestamp recordedOn;
        private final long clusterId;
        private final Map<String, Double> values = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        private final Map<String, Double> usages = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        private final Map<String, Double> capacities = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        SingleClusterSingleTimeStats(@Nonnull Timestamp recordedOn, long clusterId) {
            this.recordedOn = recordedOn;
            this.clusterId = clusterId;
        }

        public Timestamp getRecordedOn() {
            return recordedOn;
        }

        /**
         * Ingest a DB record about this cluster and timestamp.
         *
         * @param record record to ingest
         */
        public void ingestRecord(@Nonnull Record record) {
            if (!record.get(RECORDED_ON, Timestamp.class).equals(recordedOn)
                    || Long.valueOf(record.get(INTERNAL_NAME, String.class)) != clusterId) {
                throw new IllegalArgumentException("Cannot ingest cluster stats record");
            }

            final String propertyType = record.get(PROPERTY_TYPE, String.class);
            final String propertySubtype = record.get(PROPERTY_SUBTYPE, String.class);
            final Double value = record.get(VALUE, Double.class);

            if (STATS_STORED_IN_TWO_RECORDS.contains(propertyType)) {
                if (CAPACITY.equals(propertySubtype)) {
                    capacities.put(propertyType, value);
                } else if (USED.equals(propertySubtype)) {
                    usages.put(propertyType, value);
                }
            } else {
                values.put(propertyType, value);
            }
        }

        /**
         * Convert to a {@link StatSnapshot} object.
         *
         * @param isCurrent true if it is the current stat, false if it is historical
         * @return the {@link StatSnapshot} object
         */
        public StatSnapshot toStatSnapshot(boolean isCurrent) {
            final StatSnapshot.Builder resultBuilder = StatSnapshot.newBuilder()
                                                            .setSnapshotDate(recordedOn.getTime());
            // usages and capacities
            for (Entry<String, Double> e : usages.entrySet()) {
                final double usage = e.getValue();
                final double capacity = capacities.getOrDefault(e.getKey(), 0.0);
                final CommodityTypeUnits units = ClassicEnumMapper.CommodityTypeUnits
                                                        .fromStringIgnoreCase(e.getKey());
                resultBuilder.addStatRecords(StatRecord.newBuilder()
                                                .setName(e.getKey())
                                                .setUsed(makeStatValue(usage))
                                                .setValues(makeStatValue(usage))
                                                .setCapacity(makeStatValue(capacity))
                                                .setUnits(units == null ? "" : units.getUnits()));
            }

            // commodities that only have one value
            for (Entry<String, Double> e : values.entrySet()) {
                final CommodityTypeUnits units = ClassicEnumMapper.CommodityTypeUnits
                                                        .fromStringIgnoreCase(e.getKey());
                resultBuilder.addStatRecords(StatRecord.newBuilder()
                                                    .setName(e.getKey())
                                                    .setUsed(makeStatValue(e.getValue()))
                                                    .setUnits(e.getKey())
                                                    .setReserved(0.0f)
                                                    .setUnits(units == null ? "" : units.getUnits()));
            }

            return resultBuilder
                        .setStatEpoch(isCurrent ? StatEpoch.CURRENT : StatEpoch.HISTORICAL)
                        .build();
        }

        /**
         * Return utilization of a stat for this cluster and timestamp.
         *
         * @param key the stat name
         * @param isAscending whether sort in ascending order or not
         * @return the utilization
         */
        public double getUtilization(@Nonnull String key, boolean isAscending) {
            final Double used = usages.get(key);
            final Double capacity = capacities.get(key);
            if (used == null || capacity == null || capacity == 0.0) {
                // Here we return positive infinity when it's ascending and
                // -1.0 when it's descending so that such records will be grouped at the end.
                return isAscending ? Double.POSITIVE_INFINITY : -1.0;
            }
            return used / capacity;
        }

        /**
         * Return value of a stat for this cluster and timestamp.
         *
         * @param key the stat name
         * @return the value
         */
        public double getValue(@Nonnull String key) {
            return values.getOrDefault(key, 0.0);
        }

        private static StatValue makeStatValue(double value) {
            final float floatValue = (float)value;
            return StatValue.newBuilder()
                        .setAvg(floatValue)
                        .setMin(floatValue)
                        .setMax(floatValue)
                        .setTotal(floatValue)
                        .setTotalMax(floatValue)
                        .setTotalMin(floatValue)
                        .build();
        }
    }

    /**
     * This class collects stats that concern a single cluster.
     */
    private static class SingleClusterStats {
        private final long clusterId;
        private final boolean includeProjectedStats;
        private final boolean removeVMGrowthFromResult;

        double mostRecentVMGrowth = 0.0;

        // historical data sort from more recent to less recent
        private final SortedMap<Timestamp, SingleClusterSingleTimeStats> allStats =
                new TreeMap<>(Comparator.reverseOrder());

        // list to hold projected stats, if required
        private final Deque<StatSnapshot> projectedStatSnapshots = new LinkedList<>();

        private SingleClusterSingleTimeStats mostRecentRecordForDailyStats = null;
        private SingleClusterSingleTimeStats mostRecentRecordForNonDailyStats = null;

        SingleClusterStats(long clusterId, boolean includeProjectedStats, boolean removeVMGrowthFromResult) {
            if (removeVMGrowthFromResult && !includeProjectedStats) {
                throw new IllegalStateException("Cluster stats request illegally augmented with VM growth");
            }
            this.clusterId = clusterId;
            this.includeProjectedStats = includeProjectedStats;
            this.removeVMGrowthFromResult = removeVMGrowthFromResult;
        }

        /**
         * Ingest a DB record for this cluster.
         *
         * @param record the DB record to ingest
         */
        public void ingestRecord(@Nonnull Record record) {
            if (Long.valueOf(record.get(INTERNAL_NAME, String.class)) != clusterId) {
                throw new IllegalArgumentException("Cannot ingest cluster stats record");
            }

            final String statName = record.get(PROPERTY_TYPE, String.class);

            // get or create the appropriate SingleClusterSingleTimeStats object
            // for this timestamp
            final Timestamp recordedOn = record.get(RECORDED_ON, Timestamp.class);
            final SingleClusterSingleTimeStats statRecord =
                    allStats.computeIfAbsent(recordedOn,
                                             k -> new SingleClusterSingleTimeStats(recordedOn, clusterId));

            // update "most recent" objects accordingly
            if (STATS_STORED_DAILY.contains(statName)) {
                if (mostRecentRecordForDailyStats == null
                        || recordedOn.getTime() > mostRecentRecordForDailyStats.getRecordedOn().getTime()) {
                    mostRecentRecordForDailyStats = statRecord;
                }
            } else {
                if (mostRecentRecordForNonDailyStats == null
                        || recordedOn.getTime() > mostRecentRecordForNonDailyStats
                                                        .getRecordedOn().getTime()) {
                    mostRecentRecordForNonDailyStats = statRecord;
                }
            }

            final boolean isVMGrowth = statName.equals(VM_GROWTH);

            // update the latest VM growth, if we want projected stats
            if (includeProjectedStats && mostRecentRecordForDailyStats == statRecord && isVMGrowth) {
                mostRecentVMGrowth = record.get(VALUE, Double.class);
            }

            // ingest record, except if it is VM growth and we don't want VM growth records
            if (!isVMGrowth || !removeVMGrowthFromResult) {
                statRecord.ingestRecord(record);
            }
        }

        public long getClusterId() {
            return clusterId;
        }

        @Nullable
        public SingleClusterSingleTimeStats getMostRecentRecordForDailyStats() {
            return mostRecentRecordForDailyStats;
        }

        @Nullable
        public SingleClusterSingleTimeStats getMostRecentRecordForNonDailyStats() {
            return mostRecentRecordForNonDailyStats;
        }

        /**
         * Add projected stats for this cluster.
         *
         * @param endDate end date of the original request.
         *                If the end date given here is older that the date of the latest record
         *                for the cluster, then no projected stats will be added
         */
        public void addProjectedStats(long endDate) {
            if (mostRecentRecordForDailyStats == null
                    || mostRecentRecordForDailyStats.getRecordedOn().getTime() >= endDate) {
                return;
            }

            final long latestRecordDate = mostRecentRecordForDailyStats.getRecordedOn().getTime();

            // get the stats to be projected from the last record
            final List<StatRecord> headroomCommodityRecords =
                mostRecentRecordForDailyStats.toStatSnapshot(true).getStatRecordsList().stream()
                    .filter(record -> PROJECTED_STATS.contains(record.getName()))
                    .collect(Collectors.toList());

            // add extra current snapshot
            projectedStatSnapshots.addFirst(StatSnapshot.newBuilder()
                                                .setStatEpoch(StatEpoch.CURRENT)
                                                .setSnapshotDate(latestRecordDate)
                                                .addAllStatRecords(headroomCommodityRecords)
                                                .build());

            final int dailyVMGrowth = (int)Math.ceil(mostRecentVMGrowth / ONE_MONTH_IN_DAYS);
            final long daysDifference = (endDate - latestRecordDate) / ONE_DAY_IN_MILLIS + 1;

            // add projected stats per day
            for (int day = 1; day <= daysDifference; day++) {
                StatSnapshot.Builder statSnapshotBuilder =
                        StatSnapshot.newBuilder()
                            .setStatEpoch(StatEpoch.PROJECTED)
                            .setSnapshotDate(latestRecordDate + day * ONE_DAY_IN_MILLIS);

                // create projected headroom stats for each commodity.
                final List<StatRecord> projectedStatRecords = new ArrayList<>(
                                                                    headroomCommodityRecords.size());
                final float accumulatedGrowth = dailyVMGrowth * day;
                for (StatRecord statRecord : headroomCommodityRecords) {
                    final StatRecord.Builder projectedStatRecord = statRecord.toBuilder();
                    final float projectedHeadroom =
                            Math.max(0, statRecord.getUsed().getAvg() - accumulatedGrowth);
                    projectedStatRecord.setUsed(
                            SingleClusterSingleTimeStats.makeStatValue(projectedHeadroom));
                    projectedStatRecords.add(projectedStatRecord.build());
                }

                statSnapshotBuilder.addAllStatRecords(projectedStatRecords);
                projectedStatSnapshots.addFirst(statSnapshotBuilder.build());
            }
        }

        /**
         * Convert this to an {@link EntityStats} object.
         *
         * @param onlyOneDataPointRequested keep only most recent snapshot per cluster and stat
         * @return the {@link EntityStats} translation
         */
        public EntityStats toEntityStats(boolean onlyOneDataPointRequested) {
            final EntityStats.Builder resultBuilder = EntityStats.newBuilder()
                                                            .setOid(clusterId);
            // if only one data point is requested, fetch the latest record for daily stats
            // and the latest record for non-daily starts, and then return
            if (onlyOneDataPointRequested) {
                if (mostRecentRecordForDailyStats != null) {
                    resultBuilder
                        .addStatSnapshots(mostRecentRecordForDailyStats.toStatSnapshot(true));
                }
                if (mostRecentRecordForNonDailyStats != null) {
                    resultBuilder
                        .addStatSnapshots(mostRecentRecordForNonDailyStats.toStatSnapshot(true));
                }
                return resultBuilder.build();
            }

            // add projections
            projectedStatSnapshots.forEach(resultBuilder::addStatSnapshots);

            // add historical stats and return
            return resultBuilder
                        .addAllStatSnapshots(allStats.values().stream()
                                                .map(s -> s.toStatSnapshot(false))
                                                .collect(Collectors.toList()))
                        .build();
        }

        /**
         * Get a function to sort {@link SingleClusterStats} objects by.
         * Sorting happens according to a stat of the most recent historical record.
         *
         * @param key name of the stat to be used for sorting
         * @param isAscending whether sort in ascending order or not
         * @return function to be used by the comparator when sorting
         */
        public static Function<SingleClusterStats, Float> getComparisonFunction(@Nonnull String key,
                                                                                boolean isAscending) {
            // if comparing ids, the comparison function is straightforward
            if (key.equalsIgnoreCase(INTERNAL_NAME)) {
                return r -> r == null ? 0.0f : (float)(r.getClusterId());
            }

            // is the sort commodity a daily commodity?
            // if so, it must come from the most recent daily record,
            // otherwise the most recent non-daily record
            // function selectRecord decides that
            final Function<SingleClusterStats, SingleClusterSingleTimeStats> selectRecord =
                STATS_STORED_DAILY.contains(key) ? SingleClusterStats::getMostRecentRecordForDailyStats
                                                 : SingleClusterStats::getMostRecentRecordForNonDailyStats;

            // is the sort commodity a utilization commodity?
            // if so, the SingleClusterSingleTimeStats.getUtilization method must be used
            // otherwise the SingleClusterSingleTimeStats.getValue
            // function selectComparisonMethod decides that
            final Function<SingleClusterSingleTimeStats, Float> selectComparisonMethod;
            if (STATS_STORED_IN_TWO_RECORDS.contains(key)) {
                selectComparisonMethod = r -> r == null ? 0.0f : (float)(r.getUtilization(key, isAscending));
            } else {
                selectComparisonMethod = r -> r == null ? 0.0f : (float)(r.getValue(key));
            }

            // the comparison function is a composition of the two functions defined above
            return selectComparisonMethod.compose(selectRecord);
        }
    }

    private List<ClusterStatsResponse> createClusterStatsResponseList(
            @Nonnull List<EntityStats> entityStats, @Nonnull PaginationResponse paginationResponse) {
        final List<ClusterStatsResponse> clusterStatsResponses = new ArrayList<>();
        clusterStatsResponses.add(ClusterStatsResponse.newBuilder()
                                        .setPaginationResponse(paginationResponse)
                                        .build());
        Iterators.partition(entityStats.iterator(), maxAmountOfEntitiesPerGrpcMessage)
                .forEachRemaining(chunk -> clusterStatsResponses.add(
                                                ClusterStatsResponse.newBuilder()
                                                        .setSnapshotsChunk(EntityStatsChunk.newBuilder()
                                                                                .addAllSnapshots(chunk))
                                                        .build()));
        return clusterStatsResponses;
    }

    /**
     * Wrapper for any of the various cluster_stats_* tables, providing uniform access to the
     * column values.
     */
    public static class ClusterStatsRecordReader {

        private final Record record;

        /**
         * Wrap a cluster stats record.
         *
         * @param record record to be wrapped
         */
        ClusterStatsRecordReader(Record record) {
            this.record = record;
        }

        /**
         * Get the timestamp for this record.
         *
         * @return record timestamp
         */
        public Timestamp getRecordedOn() {
            return record.get(RECORDED_ON, Timestamp.class);
        }

        /**
         * Get this record's property type.
         *
         * @return property type
         */
        public String getPropertyType() {
            return record.get(PROPERTY_TYPE, String.class);
        }

        /**
         * Get this record's property subtype.
         *
         * @return the property subtype
         */
        public String getPropertySubtype() {
            return record.get(PROPERTY_SUBTYPE, String.class);
        }

        /**
         * Get this record's value.
         *
         * @return the value
         */
        public Float getValue() {
            Double value = record.get(VALUE, Double.class);
            return value != null ? value.floatValue() : null;
        }
    }
}
