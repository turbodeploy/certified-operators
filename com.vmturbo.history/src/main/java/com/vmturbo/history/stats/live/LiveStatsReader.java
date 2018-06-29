package com.vmturbo.history.stats.live;

import static com.vmturbo.history.db.jooq.JooqUtils.dField;
import static com.vmturbo.history.db.jooq.JooqUtils.statsTableByTimeFrame;
import static com.vmturbo.history.db.jooq.JooqUtils.str;
import static com.vmturbo.history.schema.StringConstants.AVG_VALUE;
import static com.vmturbo.history.schema.StringConstants.CONTAINER;
import static com.vmturbo.history.schema.StringConstants.NUM_CNT_PER_HOST;
import static com.vmturbo.history.schema.StringConstants.NUM_CNT_PER_STORAGE;
import static com.vmturbo.history.schema.StringConstants.NUM_CONTAINERS;
import static com.vmturbo.history.schema.StringConstants.NUM_HOSTS;
import static com.vmturbo.history.schema.StringConstants.NUM_STORAGES;
import static com.vmturbo.history.schema.StringConstants.NUM_VMS;
import static com.vmturbo.history.schema.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.history.schema.StringConstants.NUM_VMS_PER_STORAGE;
import static com.vmturbo.history.schema.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.history.schema.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.history.schema.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.history.schema.StringConstants.STORAGE;
import static com.vmturbo.history.schema.StringConstants.UUID;
import static com.vmturbo.history.schema.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.history.utils.HistoryStatsUtils.betweenStartEndTimestampCond;
import static com.vmturbo.history.utils.HistoryStatsUtils.countPerSEsMetrics;
import static com.vmturbo.history.utils.HistoryStatsUtils.countSEsMetrics;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.db.TimeFrame;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.live.StatsQueryFactory.AGGREGATE;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Read from the stats database tables for the "live", i.e. real, discovered topology.
 **/
public class LiveStatsReader {

    private static final Logger logger = LogManager.getLogger();

    // Partition the list of entities to read into chunks of this size in order not to flood the DB.
    private static final int ENTITIES_PER_CHUNK = 50000;

    private final HistorydbIO historydbIO;

    private final TimeRangeFactory timeRangeFactory;

    private final StatsQueryFactory statsQueryFactory;

    // TODO: After StringConstants class add entity type constant, we can use it from StringConstants.
    private final String ENTITY_TYPE = "entity_type";

    private static final DataMetricSummary GET_STATS_RECORDS_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("history_get_live_stats_records_duration_seconds")
            .withHelp("Duration in seconds it takes the history component to get live stat snapshots for a set of entities.")
            .build()
            .register();

    public LiveStatsReader(@Nonnull final HistorydbIO historydbIO,
                           @Nonnull final TimeRangeFactory timeRangeFactory,
                           @Nonnull final StatsQueryFactory statsQueryFactory)  {
        this.historydbIO = historydbIO;
        this.timeRangeFactory = timeRangeFactory;
        this.statsQueryFactory = statsQueryFactory;
    }

    /**
     * A page of {@link Record}s, sorted in the order requested in input
     * {@link EntityStatsPaginationParams}.
     */
    public static class StatRecordPage {

        private final Map<Long, List<Record>> nextPageRecords;

        private final Optional<String> nextCursor;

        StatRecordPage(@Nonnull final Map<Long, List<Record>> nextPageRecords,
                       @Nonnull final Optional<String> nextCursor) {
            this.nextPageRecords = nextPageRecords;
            this.nextCursor = nextCursor;
        }

        /**
         * Get the next page of records.
         *
         * @return A sorted map of (entityId, records for entity). The entities are ordered
         *         in the order requested in the {@link EntityStatsPaginationParams}.
         */
        public Map<Long, List<Record>> getNextPageRecords() {
            return nextPageRecords;
        }

        /**
         * Get the next cursor.
         *
         * @return An {@link Optional} containing the next cursor, or an empty optional if end of
         *         results.
         */
        public Optional<String> getNextCursor() {
            return nextCursor;
        }

        private static StatRecordPage empty() {
            return new StatRecordPage(Collections.emptyMap(), Optional.empty());
        }
    }


    /**
     * Get a page of stat records. The stats records are returned individually for each entity.
     * It is the caller's responsibility to aggregate them if desired.
     *
     * @param entityIds The set of IDs to retrieve records for.
     * @param statsFilter The filter specifying which stats to get. If the filter time range spans
     *                    across multiple snapshots, the sort order for pagination will be
     *                    derived from the most recent snapshot. However, once we determine the
     *                    IDs of the entities in the next page, we retrieve records for those
     *                    entities from all matching snapshots.
     * @param paginationParams The {@link EntityStatsPaginationParams} specifying the pagination
     *                         parameters to use.
     * @return A {@link StatRecordPage} containing the next page of per-entity records and the
     *         next cursor for subsequent calls to this function.
     * @throws VmtDbException If there is an error interacting with the database.
     */
    @Nonnull
    public StatRecordPage getPaginatedStatsRecords(@Nonnull final Set<String> entityIds,
                                     @Nonnull final StatsFilter statsFilter,
                                     @Nonnull final EntityStatsPaginationParams paginationParams) throws VmtDbException {
        final Optional<TimeRange> timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter);
        if (!timeRangeOpt.isPresent()) {
            // no data persisted yet; just return an empty answer
            logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps." +
                    " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate());
            return StatRecordPage.empty();
        }
        final TimeRange timeRange = timeRangeOpt.get();

        // We first get the IDs of the entities in the next page using the most recent snapshot
        // in the time range.
        final NextPageInfo nextPageInfo = historydbIO.getNextPage(entityIds,
                timeRange.getMostRecentSnapshotTime(),
                timeRange.getTimeFrame(),
                paginationParams);

        // Now we build up a query to get ALL relevant stats for the entities in the page.
        // This may include stats for other snapshots, if the time range in the stats filter
        // matches multiple snapshots.
        final Optional<Select<?>> query = statsQueryFactory.createStatsQuery(nextPageInfo.getEntityOids(),
                nextPageInfo.getTable(), statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG);
        if (!query.isPresent()) {
            return StatRecordPage.empty();
        }

        final Map<Long, List<Record>> recordsByEntityId =
                new LinkedHashMap<>(nextPageInfo.getEntityOids().size());
        // Initialize entries in the linked hashmap in the order that they appeared in for the next page.
        // Preserving the order is very important!
        nextPageInfo.getEntityOids().forEach(entityId ->
                recordsByEntityId.put(Long.parseLong(entityId), new ArrayList<>()));

        // Run the query to get all relevant stat records.
        // TODO (roman, Jun 29 2018): Ideally we should get the IDs of entities in the page and
        // run the query to get the stats in the same transaction.
        final Result<? extends Record> statsRecords = historydbIO.execute(
                BasedbIO.Style.FORCED, query.get());
        // Process the records, inserting them into the right entry in the linked hashmap.
        statsRecords.forEach(record -> {
            final String recordUuid = record.getValue((Field<String>)dField(nextPageInfo.getTable(), UUID));
            final List<Record> recordListForEntity = recordsByEntityId.get(Long.parseLong(recordUuid));
            if (recordListForEntity == null) {
                throw new IllegalStateException("Record without requested ID returned from DB query.");
            } else {
                recordListForEntity.add(record);
            }
        });

        return new StatRecordPage(recordsByEntityId, nextPageInfo.getNextCursor());
    }

    /**
     * Fetch rows from the stats tables based on the date range, and looking in the appropriate table
     * for each entity. This method returns individual records. It is the caller's responsibility
     * to accumulate them.
     *
     * This requires looking up the entity type for each entity id in the list, and and then iterating
     * over the time-based tables for that entity type.
     *
     * The time interval is widened to ensure we capture past stats when startTime == endTime.
     *
     * @param entityIds a list of primary-level entities to gather stats from; groups have been
     *                  expanded before we get here
     * @return a list of Jooq records, one for each stats information row retrieved
     */
    @Nonnull
    public List<Record> getStatsRecords(@Nonnull final Set<String> entityIds,
                                        @Nonnull final StatsFilter statsFilter)
            throws VmtDbException {
        final DataMetricTimer timer = GET_STATS_RECORDS_DURATION_SUMMARY.startTimer();
        final Optional<TimeRange> timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter);
        if (!timeRangeOpt.isPresent()) {
            // no data persisted yet; just return an empty answer
            logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps." +
                    " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate());
            return Collections.emptyList();
        }
        final TimeRange timeRange = timeRangeOpt.get();

        final Map<String, String> entityClsMap = historydbIO.getTypesForEntities(entityIds);

        final Multimap<String, String> entityIdsByType = HashMultimap.create();
        for (final String serviceEntityId : entityIds) {
            String entityClass = entityClsMap.get(serviceEntityId);
            entityIdsByType.put(entityClass, serviceEntityId);
        }

        // Accumulate stats records, iterating by entity type at the top level
        final List<Record> answer = new ArrayList<>();
        for (final Map.Entry<String, Collection<String>> entityTypeAndId : entityIdsByType.asMap().entrySet()) {
            final String entityClsName = entityTypeAndId.getKey();
            logger.debug("fetch stats for entity type {}", entityClsName);

            final Optional<EntityType> entityType = EntityType.getTypeForName(entityClsName);
            if (!entityType.isPresent()) {
                // no entity type found for this class name; not supposed to happen
                logger.warn("DB Entity type not found for clsName {}", entityClsName);
                continue;
            }
            final List<String> entityIdsForType  = Lists.newArrayList(entityTypeAndId.getValue());
            final int numberOfEntitiesToPersist = entityIdsForType.size();
            logger.debug("entity count for {} = {}", entityClsName, numberOfEntitiesToPersist);
            final Instant start = Instant.now();

            int entityIndex = 0;
            while(entityIndex < numberOfEntitiesToPersist) {
                final int nextIndex = Math.min(entityIndex+ENTITIES_PER_CHUNK, numberOfEntitiesToPersist);
                final List<String> entityIdChunk = entityIdsForType.subList(entityIndex, nextIndex);
                final Optional<Select<?>> query = statsQueryFactory.createStatsQuery(entityIdChunk, statsTableByTimeFrame(entityType.get(), timeRange.getTimeFrame()),
                        statsFilter.getCommodityRequestsList(), timeRange, AGGREGATE.NO_AGG);
                if (!query.isPresent()) {
                    continue;
                }
                final Result<? extends Record> statsRecords =
                        historydbIO.execute(BasedbIO.Style.FORCED, query.get());
                final int answerSize = statsRecords.size();
                if (logger.isDebugEnabled() && answerSize == 0) {
                    logger.debug("zero answers returned from: {}, time range: {}",
                            query.get(), timeRange);
                }
                answer.addAll(statsRecords);
                logger.debug("  chunk size {}, statsRecords {}", entityIdChunk.size(), answerSize);
                entityIndex = entityIndex + ENTITIES_PER_CHUNK;
            }
            final Duration elapsed = Duration.between(start, Instant.now());
            if (logger.isDebugEnabled()) {
                logger.debug(" answer size {}, fetch time: {}", answer.size(), elapsed);
            }
        }

        // if requested, add counts
        addCountStats(timeRange.getSnapshotTimesInRange().get(0).getTime(), entityClsMap, statsFilter.getCommodityRequestsList(), answer);

        final double elapsedSeconds = timer.observe();
        logger.debug("total stats returned: {}, overall elapsed: {}", answer.size(), elapsedSeconds);
        return answer;
    }

    /**
     * Get the full-market stats table to use, based on the time frame we're looking at.
     * @param timeFrame The time frame.
     * @return The table to use. */
    private @Nonnull Table<?> getMarketStatsTable(@Nonnull final TimeFrame timeFrame) {
        switch (timeFrame) {
            case LATEST:
                return Tables.MARKET_STATS_LATEST;
            case HOUR:
                return Tables.MARKET_STATS_BY_HOUR;
            case DAY:
                return Tables.MARKET_STATS_BY_DAY;
            case MONTH:
                return Tables.MARKET_STATS_BY_MONTH;
            case YEAR:
                return Tables.MARKET_STATS_BY_MONTH;
            default:
                throw new IllegalArgumentException("invalid timeframe: " + timeFrame);
        }
    }

    /**
     * Read the stats for the entire market; This can be faster since the list of entities
     * to include is implicit. Fetch stats for the given commodity names that occur between the
     * startTime and endTime.
     *
     * @param statsFilter The filter to use to get the stats.
     * @param entityType optional of entity type
     * @return an ImmutableList of DB Stats Records containing the result from searching all the stats tables
     * for the given time range and commodity names
     * @throws VmtDbException if there's an exception querying the data
     */
    public @Nonnull List<Record> getFullMarketStatsRecords(@Nonnull final StatsFilter statsFilter,
                                                 @Nonnull Optional<String> entityType)
            throws VmtDbException {
        final Optional<TimeRange> timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter);
        if (!timeRangeOpt.isPresent()) {
            // no data persisted yet; just return an empty answer
            logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps." +
                    " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate());
            return Collections.emptyList();
        }
        final TimeRange timeRange = timeRangeOpt.get();

        logger.debug("getting stats for full market");

        Instant overallStart = Instant.now();

        final Table<?> table = getMarketStatsTable(timeRange.getTimeFrame());

        // accumulate the conditions for this query
        List<Condition> whereConditions = new ArrayList<>();

        // add where clause for time range; null if the timeframe cannot be determined
        final Condition timeRangeCondition = betweenStartEndTimestampCond(dField(table, SNAPSHOT_TIME),
                timeRange.getTimeFrame(), timeRange.getStartTime(), timeRange.getEndTime());
        if (timeRangeCondition != null) {
            whereConditions.add(timeRangeCondition);
        }
        // add select on the given commodity requests; if no commodityRequests specified,
        // leave out the were clause and thereby include all commodities.
        final Optional<Condition> commodityRequestsCond =
                statsQueryFactory.createCommodityRequestsCond(statsFilter.getCommodityRequestsList(), table);
        commodityRequestsCond.ifPresent(whereConditions::add);

        // if no entity type provided, it will include all entity type.
        Optional<Condition> entityTypeCond = entityTypeCond(entityType, table);
        entityTypeCond.ifPresent(whereConditions::add);

        // Format the query.
        // No need to order or group by anything, since when preparing the response
        // we rearrange things by snapshot time anyway.
        final Query query = historydbIO.JooqBuilder()
                .select(Arrays.asList(table.fields()))
                .from(table)
                .where(whereConditions);

        logger.debug("Running query... {}", query);
        Result<? extends Record> results = historydbIO.execute(Style.FORCED, query);

        final List<Record> answer = new ArrayList<>(results);

        final Set<String> requestedRatioProps = new HashSet<>(countPerSEsMetrics);
        if (!statsFilter.getCommodityRequestsList().isEmpty()) {
            // This does countStats.size() lookups in commodityNames, which is a list.
            // This is acceptable because the asked-for commodityNames is supposed to be
            // a small ( < 10) list.
            requestedRatioProps.retainAll(statsFilter.getCommodityRequestsList());
        }

        // Count any "__ per __" (e.g. vm per host) stats.
        // TODO (roman, Feb 22, 2017): Refactor the count implementation to share the code
        // between the full-market case and the "regular" case. The individual cases
        // would be responsible for getting the entity counts (in the full-market case
        // that's saved as a standalone property), but the shared code would calculate
        // various ratios.
        if (!requestedRatioProps.isEmpty()) {
            // The results we get back from the query above contain multiple entity counts
            // associated with snapshot times. We want, for every snapshot that has entity
            // counts, to create the ratio properties. The resulting data structure
            // is a map of timestamp -> map of entity count -> num of entities.
            final Map<Timestamp, Map<String, Float>> entityCountsByTime = new HashMap<>();
            // Go through all the returned records, and initialize the entity counts.
            // The number of records is not expected to be super-high, so this pass
            // won't be very expensive.
            answer.forEach(record -> {
                String propName = record.getValue(str(dField(table, PROPERTY_TYPE)));
                // containsValue is efficient in a BiMap.
                if (countSEsMetrics.containsValue(propName)) {
                    Timestamp statTime = record.getValue(JooqUtils.timestamp(dField(table, SNAPSHOT_TIME)));
                    Map<String, Float> snapshotCounts =
                            entityCountsByTime.computeIfAbsent(statTime, key -> new HashMap<>());
                    // Each count metric should appear only once per SNAPSHOT_TIME.
                    // If it appears more than once, use the latest value.
                    Float prevValue = snapshotCounts.put(propName,
                            record.getValue(AVG_VALUE, Float.class));
                    if (prevValue != null) {
                        logger.warn("Value for " + propName +
                                " appeared twice for the same snapshot {}", statTime);
                    }
                }
            });

            // Go through the entity counts by time, and for each timestamp create
            // ratio properties based on the various counts.
            entityCountsByTime.forEach((snapshotTime, entityCounts) -> requestedRatioProps.forEach(ratioPropName -> {
                final double ratio;
                switch (ratioPropName) {
                    case NUM_VMS_PER_HOST: {
                        final Float numHosts = entityCounts.get(NUM_HOSTS);
                        ratio = numHosts != null && numHosts > 0 ?
                                entityCounts.getOrDefault(NUM_VMS, 0f) / numHosts : 0;
                        break;
                    }
                    case NUM_VMS_PER_STORAGE: {
                        final Float numStorages = entityCounts.get(NUM_STORAGES);
                        ratio = numStorages != null && numStorages > 0 ?
                                entityCounts.getOrDefault(NUM_VMS, 0f) / numStorages : 0;
                        break;
                    }
                    case NUM_CNT_PER_HOST: {
                        final Float numHosts = entityCounts.get(NUM_HOSTS);
                        ratio = numHosts != null && numHosts > 0 ?
                                entityCounts.getOrDefault(NUM_CONTAINERS, 0f) / numHosts : 0;
                        break;
                    }
                    case NUM_CNT_PER_STORAGE:
                        final Float numStorages = entityCounts.get(NUM_STORAGES);
                        ratio = numStorages != null && numStorages > 0 ?
                                entityCounts.getOrDefault(NUM_CONTAINERS, 0f) / numStorages : 0;
                        break;
                    default:
                        throw new IllegalStateException("Illegal stat name: " + ratioPropName);
                }
                final MarketStatsLatestRecord countRecord = new MarketStatsLatestRecord();
                countRecord.setSnapshotTime(snapshotTime);
                countRecord.setPropertyType(ratioPropName);
                countRecord.setAvgValue(ratio);
                answer.add(countRecord);
            }));
        }


        final Duration overallElapsed = Duration.between(overallStart, Instant.now());
        logger.debug("total stats returned: {}, overall elapsed: {}", answer.size(),
                overallElapsed.toMillis() / 1000.0);
        return Collections.unmodifiableList(answer);
    }

    /**
     * If any stats requiring entity counts are requested, then count entity types and
     * add stats records to the snapshot.
     *
     * If the commodityNames list is null or empty, then include all count-based stats.
     *  @param currentSnapshotTime the time of the current snapshot
     * @param entityClassMap map from entity OID to entity class name
     * @param commodityNames a list of the commodity names to filter on
     * @param countStatsRecords the list that count stats will be added to
     */
    private void addCountStats(long currentSnapshotTime,
                               @Nonnull Map<String, String> entityClassMap,
                               @Nullable List<CommodityRequest> commodityNames,
                               @Nonnull List<Record> countStatsRecords) {

        // use the startTime for the all counted stats
        final Timestamp snapshotTimestamp = new Timestamp(currentSnapshotTime);

        // derive list of calculated metrics
        List<String> filteredCommodityNames = Lists.newArrayList(countPerSEsMetrics);
        filteredCommodityNames.addAll(countSEsMetrics.values());

        // default is to include all counted commodities
        if (commodityNames != null && !commodityNames.isEmpty()) {
            // leave only the requested metrics - might not be any
            filteredCommodityNames.retainAll(commodityNames);
        }

        // if no counted stats of interest, no work to do
        if (filteredCommodityNames.isEmpty()) {
            return;
        }

        // initialize map of entity types -> counts
        Map<String, Integer> entityTypeCounts = new HashMap<>();
        for (String entityTypeName : countSEsMetrics.keySet()) {
            entityTypeCounts.put(entityTypeName, 0);
        }
        // count the entity types that we care about
        entityClassMap.forEach((String entityOid, String entityType) -> {
            if (countSEsMetrics.containsKey(entityType)) {
                entityTypeCounts.put(entityType, entityTypeCounts.get(entityType) + 1);
            }
        });

        // for requested calculated stat, add a db record
        ImmutableBiMap<String, String> mapStatToEntityType = countSEsMetrics.inverse();
        for (String commodityName : filteredCommodityNames) {
            PmStatsLatestRecord countRecord = new PmStatsLatestRecord();
            double statValue = 0;
            if (mapStatToEntityType.containsKey(commodityName)) {
                statValue = entityTypeCounts.get(mapStatToEntityType.get(commodityName));
            } else {
                final Integer vmCounts = entityTypeCounts.get(VIRTUAL_MACHINE);
                final Integer pmCounts = entityTypeCounts.get(PHYSICAL_MACHINE);
                final Integer stCounts = entityTypeCounts.get(STORAGE);
                final Integer cntCounts = entityTypeCounts.get(CONTAINER);

                switch(commodityName) {
                    case NUM_VMS_PER_HOST:
                        if (pmCounts != null && vmCounts != null && pmCounts != 0) {
                            statValue = ((double) vmCounts) / pmCounts;
                        }
                        break;
                    case NUM_VMS_PER_STORAGE:
                        if (vmCounts != null && stCounts != null && stCounts != 0) {
                            statValue = ((double) vmCounts) / stCounts;
                        }
                        break;
                    case NUM_CNT_PER_HOST:
                        if (cntCounts != null && pmCounts != null && pmCounts != 0) {
                            statValue = ((double) cntCounts) / pmCounts;
                        }
                        break;
                    case NUM_CNT_PER_STORAGE:
                        if (cntCounts != null && stCounts != null && stCounts != 0) {
                            statValue = ((double) cntCounts) / stCounts;
                        }
                        break;
                    default:
                        logger.warn("unhandled commodity count stat name: {}", commodityName);
                        continue;
                }
            }
            countRecord.setSnapshotTime(snapshotTimestamp);
            countRecord.setPropertyType(commodityName);
            countRecord.setAvgValue(statValue);
            countRecord.setRelation(RelationType.METRICS);
            countStatsRecords.add(countRecord);
        }
    }

    /**
     * Create a Jooq conditional clause to filter on entity type if it is present.
     *
     * @param entityType entity type need to filter on.
     * @param table the DB table from which these stats will be collected
     * @return an Optional contains a Jooq condition to filter on entity type.
     */
    private Optional<Condition> entityTypeCond(@Nonnull final Optional<String> entityType,
                                               @Nonnull final Table<?> table) {
        return entityType
                .filter(serviceEntityType -> !serviceEntityType.isEmpty())
                .map(serviceEntityType ->
                        str(dField(table, ENTITY_TYPE)).in(serviceEntityType));
    }

    /**
     * Return the display name for the Entity for the given entity ID (OID).
     * <p>
     * If the entity ID is not known, then return null.
     *
     * TODO: Should probably be moved to a new EntityReader class.
     *
     * @param entityOID the OID for the entity to look up
     * @return the display name for the given OID, if found, otherwise null
     */
    public String getEntityDisplayNameForId(Long entityOID) {
        return historydbIO.getEntityDisplayNameForId(entityOID);
    }

}
