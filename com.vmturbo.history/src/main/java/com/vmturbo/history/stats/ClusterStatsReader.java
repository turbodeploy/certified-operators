package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.common.protobuf.utils.StringConstants.SAMPLES;
import static com.vmturbo.common.protobuf.utils.StringConstants.USED;
import static com.vmturbo.common.protobuf.utils.StringConstants.VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.VM_GROWTH;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.utils.StringConstants;
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
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsLatestRecord;
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
    private final HistorydbIO historydbIO;
    private final ClusterTimeRangeFactory timeRangeFactory;
    private final ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory;

    /**
     * These stats have two DB records per observation: one for usage and one for capacity.
     */
    private static final Set<String> STATS_STORED_IN_TWO_RECORDS;

    private static final PaginationParameters DEFAULT_PAGINATION = PaginationParameters.newBuilder()
                                                                        .setAscending(false)
                                                                        .setCursor("0")
                                                                        .setLimit(Integer.MAX_VALUE)
                                                                        .build();

    static {
        final SortedSet<String> statsStoredInTwoRecords = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        statsStoredInTwoRecords.add(StringConstants.MEM);
        statsStoredInTwoRecords.add(StringConstants.CPU);
        statsStoredInTwoRecords.add(StringConstants.CPU_HEADROOM);
        statsStoredInTwoRecords.add(StringConstants.MEM_HEADROOM);
        statsStoredInTwoRecords.add(StringConstants.STORAGE_HEADROOM);
        STATS_STORED_IN_TWO_RECORDS = Collections.unmodifiableSortedSet(statsStoredInTwoRecords);
    }

    // Cluster commodities for which to create projected headroom
    private static final Set<String> CLUSTER_COMMODITY_STATS =
            ImmutableSet.of(StringConstants.CPU_HEADROOM, StringConstants.MEM_HEADROOM,
                            StringConstants.STORAGE_HEADROOM, StringConstants.TOTAL_HEADROOM);

    /**
     * Create a new instance.
     *
     * @param historydbIO                        Access to some DB utilities
     * @param timeRangeFactory                   an instance of ClusterTimeRangeFactory used to
     *                                           determine time frame for query results
     * @param computedPropertiesProcessorFactory factory for processors tohandle computed properties
     */
    ClusterStatsReader(HistorydbIO historydbIO, ClusterTimeRangeFactory timeRangeFactory,
                       ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory) {
        this.historydbIO = historydbIO;
        this.timeRangeFactory = timeRangeFactory;
        this.computedPropertiesProcessorFactory = computedPropertiesProcessorFactory;
    }

    /**
     * Obtain records from the appropriate cluster stats table to satisfy an API request.
     * <p>
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
     * <p>
     * <p>Records are returned in a wrapper that provides uniform access to record data regardless
     * of the specific cluster_stats_* table from which the records were retrieved.</p>
     *
     * @param clusterUuid   cluster id
     * @param propertyTypes property types of interest
     * @param timeRange     time range determined from start end end times
     * @return retrieved records, each wrapped in a {@link ClusterStatsRecordReader} instance
     * @throws VmtDbException if retrieval fails
     */
    Result<? extends Record> getStatsRecordsForHeadRoomPlanRequest(long clusterUuid, Set<String> propertyTypes, TimeRange timeRange)
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

        final Result<? extends Record> results = historydbIO.execute(Style.FORCED,
                historydbIO.JooqBuilder().selectFrom(table)
                        .where(conditions).getQuery());
        return results;
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
    public ClusterStatsResponse getStatsRecords(@Nonnull ClusterStatsRequest request) throws VmtDbException {
        long now = System.currentTimeMillis();

        // extract and validate the request parameters
        if (!request.hasStats()) {
            throw new IllegalArgumentException("Cluster stats request is empty");
        }
        final StatsFilter filter = request.getStats();
        final Optional<Long> startDate = filter.hasStartDate()
                ? Optional.of(filter.getStartDate()) : Optional.empty();
        final Optional<Long> endDate = filter.hasEndDate()
                ? Optional.of(filter.getEndDate()) : Optional.empty();
        if (startDate.orElse(now) > endDate.orElse(now)) {
            throw new IllegalArgumentException("Invalid date range for retrieving cluster statistics.");
        }
        final Set<String> requestedFields = StatsUtils.collectCommodityNames(filter);
        final Set<Long> clusterIds = request.getClusterIdsList().stream().collect(Collectors.toSet());

        // extract the pagination parameters
        final PaginationParameters paginationParameters = request.hasPaginationParams()
                ? request.getPaginationParams()
                : DEFAULT_PAGINATION;
        final String orderByField;
        if (paginationParameters.hasOrderBy() && paginationParameters.getOrderBy().hasEntityStats()
                && paginationParameters.getOrderBy().getEntityStats().hasStatName()) {
            orderByField = paginationParameters.getOrderBy().getEntityStats().getStatName();
        } else {
            orderByField = INTERNAL_NAME;
        }

        final boolean includeProjectedStats = request.getStats().getRequestProjectedHeadroom()
                && startDate.isPresent() && endDate.isPresent();

        // decide on time frame
        final StatsFilter statsFilter = StatsFilter.newBuilder()
                .setStartDate(startDate.orElse(now))
                .setEndDate(endDate.orElse(now))
                .addAllCommodityRequests(requestedFields.stream()
                        .map(prop ->
                                CommodityRequest.newBuilder()
                                        .setCommodityName(prop)
                                        .build())
                        .collect(Collectors.toList()))
                .build();
        final Optional<TimeRange> timeRange = timeRangeFactory.resolveTimeRange(
                statsFilter, Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        // create query and run it
        final ClusterStatsQuery query =
                new ClusterStatsQuery(getStatsTable(timeRange.map(TimeRange::getTimeFrame)
                        .orElse(TimeFrame.LATEST)),
                        timeRange.map(TimeRange::getStartTime).map(Timestamp::new),
                        timeRange.map(TimeRange::getStartTime).map(Timestamp::new),
                        requestedFields, clusterIds);
        final Result<? extends Record> results = historydbIO.execute(query.getQuery());

        // process query results
        final Map<Long, SingleClusterStats> statsPerCluster = new HashMap<>();
        for (Record r : results) {
            final long id = Long.valueOf(r.get(INTERNAL_NAME, String.class));
            statsPerCluster.computeIfAbsent(id, SingleClusterStats::new).ingestRecord(r);
        }
        if (includeProjectedStats) {
            statsPerCluster.values().forEach(s -> s.addProjectedStats(endDate.get()));
        }

        // paginate
        final EntityStatsPaginationParams entityStatsPaginationParams =
                new EntityStatsPaginationParams(paginationParameters.getLimit(), paginationParameters.getLimit(),
                        "", paginationParameters);
        final PaginatedStats paginatedStats =
                new EntityStatsPaginator()
                        .paginate(statsPerCluster.keySet(),
                                oid -> Optional.of(SingleClusterStats.getComparisonFunction(orderByField)
                                        .apply(statsPerCluster.get(oid))),
                                entityStatsPaginationParams);
        final PaginationResponse paginationResponse = paginatedStats.getPaginationResponse();
        final List<EntityStats> entityStats = paginatedStats.getNextPageIds().stream()
                .map(id -> statsPerCluster.get(id).toEntityStats())
                .collect(Collectors.toList());

        // return response
        return ClusterStatsResponse.newBuilder()
                .setPaginationResponse(paginationResponse)
                .addAllSnapshots(entityStats)
                .build();
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

        /**
         * Get the number of samples averaged into this record's value.
         *
         * @return sample count
         */
        public Integer getSamples() {
            return record instanceof ClusterStatsLatestRecord ? 1
                    : record.get(SAMPLES, Integer.class);
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

        public StatSnapshot toStatSnapshot() {
            final StatSnapshot.Builder resultBuilder = StatSnapshot.newBuilder()
                    .setSnapshotDate(recordedOn.getTime());
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
                capacities.remove(e.getKey());
            }
            for (Entry<String, Double> e : capacities.entrySet()) {
                final CommodityTypeUnits units = ClassicEnumMapper.CommodityTypeUnits
                        .fromStringIgnoreCase(e.getKey());
                resultBuilder.addStatRecords(StatRecord.newBuilder()
                        .setName(e.getKey())
                        .setUsed(makeStatValue(0.0))
                        .setValues(makeStatValue(0.0))
                        .setCapacity(makeStatValue(e.getValue()))
                        .setUnits(units == null ? "" : units.getUnits()));
            }
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
                        .setStatEpoch(StatEpoch.HISTORICAL)
                        .build();
        }

        public double getUtilization(@Nonnull String key) {
            final Double used = usages.get(key);
            final Double capacity = capacities.get(key);
            if (used == null || capacity == null || capacity == 0.0) {
                return 0.0;
            }
            return used / capacity;
        }

        public double getValue(@Nonnull String key) {
            return values.getOrDefault(key, 0.0);
        }

        public boolean hasValue(@Nonnull String key) {
            return values.containsKey(key);
        }

        private static StatValue makeStatValue(double value) {
            final float floatValue = (float) value;
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

        // sort from more recent to less recent
        private final SortedMap<Timestamp, SingleClusterSingleTimeStats> allStats =
                new TreeMap<>(Comparator.reverseOrder());

        // list to hold projected stats, if required
        private final Queue<StatSnapshot> projectedStatSnapshots = new LinkedList<>();

        SingleClusterStats(long clusterId) {
            this.clusterId = clusterId;
        }

        public void ingestRecord(@Nonnull Record record) {
            if (Long.valueOf(record.get(INTERNAL_NAME, String.class)) != clusterId) {
                throw new IllegalArgumentException("Cannot ingest cluster stats record");
            }

            final Timestamp recordedOn = record.get(RECORDED_ON, Timestamp.class);
            allStats.computeIfAbsent(recordedOn,
                                     k -> new SingleClusterSingleTimeStats(recordedOn, clusterId))
                    .ingestRecord(record);
        }

        /**
         * Add projected stats for this cluster.
         *
         * @param endDate end date of the original request.
         *                If the end date given here is older that the date of the latest record
         *                for the cluster, then no projected stats will be added
         */
        public void addProjectedStats(long endDate) {
            if (allStats.isEmpty()) {
                return;
            }

            // get latest record date and compare with the end date
            // of the current request
            final long latestRecordDate = allStats.firstKey().getTime();
            if (latestRecordDate >= endDate) {
                return;
            }

            // get last reported VM growth
            final Double vmGrowth = allStats.values().stream()
                    .filter(s -> s.hasValue(VM_GROWTH))
                    .findFirst()
                    .map(s -> s.getValue(VM_GROWTH))
                    .orElse(null);
            if (vmGrowth == null) {
                return;
            }

            // get the stats to be projected from the last record
            final List<StatRecord> headroomCommodityRecords =
                allStats.get(allStats.firstKey()).toStatSnapshot().getStatRecordsList().stream()
                    .filter(record -> CLUSTER_COMMODITY_STATS.contains(record.getName()))
                    .collect(Collectors.toList());

            // decide how many days of projected stats to add
            final int daysPerMonth = 30;
            final int dailyVMGrowth = (int)Math.ceil(vmGrowth / daysPerMonth);
            final long millisPerDay = TimeUnit.DAYS.toMillis(1);
            final long daysDifference = (endDate - latestRecordDate) / millisPerDay + 1;

            // add projected stats per day
            for (int day = 1; day <= daysDifference; day++) {
                StatSnapshot.Builder statSnapshotBuilder = StatSnapshot.newBuilder();
                statSnapshotBuilder.setStatEpoch(StatEpoch.PROJECTED);
                statSnapshotBuilder.setSnapshotDate(
                        Math.min(endDate, latestRecordDate + day * millisPerDay));

                // create projected headroom stats for each commodity.
                final List<StatRecord> projectedStatRecords = new ArrayList<>(
                                                                    headroomCommodityRecords.size());
                for (StatRecord statRecord : headroomCommodityRecords) {
                    final StatRecord.Builder projectedStatRecord = statRecord.toBuilder();
                    final float projectedHeadroom =
                            Math.max(0, statRecord.getUsed().getAvg() - dailyVMGrowth * day);
                    projectedStatRecord.setUsed(
                            SingleClusterSingleTimeStats.makeStatValue(projectedHeadroom));
                    projectedStatRecord.clearCurrentValue();
                    projectedStatRecord.clearValues();
                    projectedStatRecord.clearPeak();
                    projectedStatRecords.add(projectedStatRecord.build());
                }

                statSnapshotBuilder.addAllStatRecords(projectedStatRecords);
                projectedStatSnapshots.add(statSnapshotBuilder.build());
            }
        }

        public EntityStats toEntityStats() {
            final EntityStats.Builder resultBuilder = EntityStats.newBuilder()
                                                            .setOid(clusterId);

            // projected stats go first
            projectedStatSnapshots.forEach(resultBuilder::addStatSnapshots);

            // add historical stats and return
            return resultBuilder
                        .addAllStatSnapshots(allStats.values().stream()
                                                .map(SingleClusterSingleTimeStats::toStatSnapshot)
                                                .collect(Collectors.toList()))
                        .build();
        }

        public static Function<SingleClusterStats, Float> getComparisonFunction(@Nonnull String key) {
            if (STATS_STORED_IN_TWO_RECORDS.contains(key)) {
                return s -> (float) (s.allStats.get(s.allStats.firstKey()).getUtilization(key));
            } else {
                return s -> (float) (s.allStats.get(s.allStats.firstKey()).getValue(key));
            }
        }
    }
}
