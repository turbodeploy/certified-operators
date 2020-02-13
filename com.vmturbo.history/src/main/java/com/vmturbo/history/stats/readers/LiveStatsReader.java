package com.vmturbo.history.stats.readers;

import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.components.common.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;

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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;

import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.BasedbIO.Style;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.stats.INonPaginatingStatsReader;
import com.vmturbo.history.stats.live.FullMarketRatioProcessor;
import com.vmturbo.history.stats.live.FullMarketRatioProcessor.FullMarketRatioProcessorFactory;
import com.vmturbo.history.stats.live.RatioRecordFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.AGGREGATE;
import com.vmturbo.history.stats.live.TimeRange;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Read from the stats database tables for the "live", i.e. real, discovered topology.
 **/
public class LiveStatsReader implements INonPaginatingStatsReader<Record> {

    private static final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private final TimeRangeFactory timeRangeFactory;

    private final StatsQueryFactory statsQueryFactory;

    private final FullMarketRatioProcessorFactory fullMarketRatioProcessorFactory;

    private final RatioRecordFactory ratioRecordFactory;

    private static final DataMetricSummary GET_STATS_RECORDS_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("history_get_live_stats_records_duration_seconds")
            .withHelp("Duration in seconds it takes the history component to get live stat snapshots for a set of entities.")
            .build()
            .register();

    private final INonPaginatingStatsReader<HistUtilizationRecord> histUtilizationReader;
    private final int entitiesPerChunk;

    public LiveStatsReader(@Nonnull final HistorydbIO historydbIO,
                    @Nonnull final TimeRangeFactory timeRangeFactory,
                    @Nonnull final StatsQueryFactory statsQueryFactory,
                    @Nonnull final FullMarketRatioProcessorFactory fullMarketRatioProcessorFactory,
                    @Nonnull final RatioRecordFactory ratioRecordFactory,
                    @Nonnull INonPaginatingStatsReader<HistUtilizationRecord> histUtilizationReader,
                    int entitiesPerChunk) {
        this.historydbIO = historydbIO;
        this.timeRangeFactory = timeRangeFactory;
        this.statsQueryFactory = statsQueryFactory;
        this.fullMarketRatioProcessorFactory = Objects.requireNonNull(fullMarketRatioProcessorFactory);
        this.ratioRecordFactory = Objects.requireNonNull(ratioRecordFactory);
        this.histUtilizationReader = Objects.requireNonNull(histUtilizationReader);
        this.entitiesPerChunk = entitiesPerChunk;
    }

    /**
     * A page of {@link Record}s, sorted in the order requested in input
     * {@link EntityStatsPaginationParams}.
     */
    public static class StatRecordPage {

        private final Map<Long, List<Record>> nextPageRecords;

        private final Optional<String> nextCursor;

        private final Optional<Integer> totalRecordCount;

        StatRecordPage(@Nonnull final Map<Long, List<Record>> nextPageRecords,
                       @Nonnull final Optional<String> nextCursor,
                       @Nonnull final Optional<Integer> totalRecordCount) {
            this.nextPageRecords = nextPageRecords;
            this.nextCursor = nextCursor;
            this.totalRecordCount = totalRecordCount;
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
            return new StatRecordPage(Collections.emptyMap(), Optional.empty(), Optional.empty());
        }

        public Optional<Integer> getTotalRecordCount() {
            return totalRecordCount;
        }
    }


    /**
     * Get a page of stat records. The stats records are returned individually for each entity.
     * It is the caller's responsibility to aggregate them if desired.
     *
     * @param entityStatsScope The scope for an entity stats query.
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
    public StatRecordPage getPaginatedStatsRecords(@Nonnull final EntityStatsScope entityStatsScope,
                                     @Nonnull final StatsFilter statsFilter,
                                     @Nonnull final EntityStatsPaginationParams paginationParams) throws VmtDbException {

        EntityType entityType = historydbIO.getEntityTypeFromEntityStatsScope(entityStatsScope);

        // resolve the time range for pagination param
        final Optional<TimeRange> paginationTimeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter,
                Optional.empty(), Optional.of(entityType), Optional.of(paginationParams));

        if (!paginationTimeRangeOpt.isPresent()) {
            // no data persisted yet; just return an empty answer
            logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps for pagination param {}."
                    + " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate(),
                    paginationParams.getSortCommodity());
            return StatRecordPage.empty();
        }
        final TimeRange paginationTimeRange = paginationTimeRangeOpt.get();

        // We first get the IDs of the entities in the next page using the most recent snapshot
        // in the pagination time range.
        final NextPageInfo nextPageInfo = historydbIO.getNextPage(entityStatsScope,
                paginationTimeRange.getMostRecentSnapshotTime(),
                paginationTimeRange.getTimeFrame(),
                paginationParams,
                entityType);

        //  Only add records when next page is NOT empty, otherwise do an early return.
        if (nextPageInfo.getEntityOids().isEmpty()) {
            logger.warn("Empty next page for scope {} and pagination params {}",
                TextFormat.shortDebugString(entityStatsScope),
                paginationParams);
            return StatRecordPage.empty();
        }

        final Map<Long, List<Record>> recordsByEntityId =
                new LinkedHashMap<>(nextPageInfo.getEntityOids().size());
        // Initialize entries in the linked hashmap in the order that they appeared in for the next page.
        // Preserving the order is very important!
        nextPageInfo.getEntityOids().forEach(entityId ->
                recordsByEntityId.put(Long.parseLong(entityId), new ArrayList<>()));

        // get the time range for commodities in the statsFilter request list. If the request
        // contains only price index and the pagination parameter is price index, commRequestTimeRange
        // will be the same as paginationTimeRange. If the request contains other commodities or
        // the pagination param is not price index, we need to resolve the time range for commodity
        // in request.
        final Optional<TimeRange> commRequestTimeRange;
        boolean isCommRequestOnlyPI = historydbIO.isCommRequestsOnlyPI(statsFilter.getCommodityRequestsList());
        boolean isPaginationParamPI = StringConstants.PRICE_INDEX.equals(paginationParams.getSortCommodity())
                        || StringConstants.CURRENT_PRICE_INDEX.equals(paginationParams.getSortCommodity());
        // when 1) the request contains only price index or current price index and pagination param is
        // price index or 2) the request is empty or contains regular commodities and pagination param
        // is not price index(means it is regular commodities), reuse the pagination time range for comm
        // request time range
        if (isCommRequestOnlyPI == isPaginationParamPI) {
            commRequestTimeRange = paginationTimeRangeOpt;
        } else {
            commRequestTimeRange = timeRangeFactory.resolveTimeRange(statsFilter,
                    Optional.empty(), Optional.of(entityType), Optional.empty());
        }
        if (!commRequestTimeRange.isPresent()) {
            // no data persisted yet; just return an empty answer
            logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps for commodities {}."
                    + " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate(),
                    statsFilter.getCommodityRequestsList());
            return StatRecordPage.empty();
        }
        // we need to specifically query for PI stats record if the commodity request list only
        // contains price index, otherwise, just fetch the regular commodity stats
        final List<String> nextPageEntityOids = nextPageInfo.getEntityOids();
        Optional<Select<?>> query = statsQueryFactory.createStatsQuery(nextPageEntityOids,
                nextPageInfo.getTable(), statsFilter.getCommodityRequestsList(),
                commRequestTimeRange.get(), AGGREGATE.NO_AGG);
        if (!query.isPresent()) {
            return StatRecordPage.empty();
        }
        // Run the query to get all relevant stat records.
        // TODO (roman, Jun 29 2018): Ideally we should get the IDs of entities in the page and
        // run the query to get the stats in the same transaction.
        final Result<? extends Record> statsRecords = historydbIO.execute(
                BasedbIO.Style.FORCED, query.get());
        histUtilizationReader.getRecords(new HashSet<>(nextPageEntityOids), statsFilter)
                        .forEach(record -> {
                            final Long oid = record.getOid();
                            addRecord(String.valueOf(oid), record, recordsByEntityId);
                        });
        // Process the records, inserting them into the right entry in the linked hashmap.
        statsRecords.forEach(record -> {
            final String recordUuid = record.getValue(getStringField(nextPageInfo.getTable(), UUID));
            addRecord(recordUuid, record, recordsByEntityId);
        });

        return new StatRecordPage(recordsByEntityId, nextPageInfo.getNextCursor(), nextPageInfo.getTotalRecordCount());
    }

    private static void addRecord(String recordUuid, Record record,
                    Map<Long, List<Record>> recordsByEntityId) {
        final List<Record> recordListForEntity = recordsByEntityId.get(Long.parseLong(recordUuid));
        if (recordListForEntity == null) {
            throw new IllegalStateException("Record without requested ID returned from DB query.");
        } else {
            recordListForEntity.add(record);
        }
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
    public List<Record> getRecords(@Nonnull final Set<String> entityIds,
                                        @Nonnull final StatsFilter statsFilter)
            throws VmtDbException {

        final DataMetricTimer timer = GET_STATS_RECORDS_DURATION_SUMMARY.startTimer();

        // find out of which types those entities are ...(Map<oid, type>)
        final Map<String, String> entityClsMap = historydbIO.getTypesForEntities(entityIds);
        // handle any missing records
        final Set<String> notFoundIds = Sets.difference(entityIds, entityClsMap.keySet());
        if (!notFoundIds.isEmpty()) {
            logger.warn(
                    "Entity OIDs not found in DB (probably for an entity type that is not saved): {}",
                    notFoundIds);
        }
        // ... and create a reverse map (type -> list of oid)
        final Multimap<String, String> entityIdsByType = HashMultimap.create();
        for (final String serviceEntityId : entityIds) {
            String entityClass = entityClsMap.get(serviceEntityId);
            entityIdsByType.put(entityClass, serviceEntityId);
        }

        Optional<TimeRange> timeRangeOpt = Optional.empty();

        // Accumulate stats records, iterating by entity type at the top level
        final List<Record> answer = new ArrayList<>();
        for (final Map.Entry<String, Collection<String>> entityTypeAndId : entityIdsByType.asMap().entrySet()) {
            final String entityClsName = entityTypeAndId.getKey();
            logger.debug("fetch stats for entity type {}", entityClsName);
            // get the entity type
            final Optional<EntityType> entityTypeOpt = EntityType.getTypeForName(entityClsName);
            if (!entityTypeOpt.isPresent()) {
                // no entity type found for this class name; not supposed to happen
                logger.warn("DB Entity type not found for entity class name {}, oids {}",
                        entityClsName, entityTypeAndId.getValue());
                continue;
            }
            final EntityType entityType = entityTypeOpt.get();

            // get the entities of that type
            final List<String> entityIdsForType  = Lists.newArrayList(entityTypeAndId.getValue());
            final int numberOfEntitiesToPersist = entityIdsForType.size();
            logger.debug("entity count for {} = {}", entityClsName, numberOfEntitiesToPersist);

            // create a timerange, given the start/end range in the filter
            // use also the entities in order to be more exact on the timerange calculation
            timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter, Optional.of(entityIdsForType),
                    Optional.of(entityType), Optional.empty());
            if (!timeRangeOpt.isPresent()) {
                // no data persisted yet; just return an empty answer
                logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps." +
                    " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate());
                continue;
            }
            final TimeRange timeRange = timeRangeOpt.get();

            final Instant start = Instant.now();

            int entityIndex = 0;
            while(entityIndex < numberOfEntitiesToPersist) {
                final int nextIndex =
                                Math.min(entityIndex + entitiesPerChunk, numberOfEntitiesToPersist);
                final List<String> entityIdChunk = entityIdsForType.subList(entityIndex, nextIndex);
                final Optional<Select<?>> query = statsQueryFactory.createStatsQuery(entityIdChunk,
                        entityType.getTimeFrameTable(timeRange.getTimeFrame()),
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
                entityIndex = entityIndex + entitiesPerChunk;
            }
            final Duration elapsed = Duration.between(start, Instant.now());
            if (logger.isDebugEnabled()) {
                logger.debug(" answer size {}, fetch time: {}", answer.size(), elapsed);
            }
        }

        // in thise case the timerange will be the value retrieved from the last loop
        if (timeRangeOpt.isPresent()) {
            addCountStats(timeRangeOpt.get().getMostRecentSnapshotTime(),
                entityClsMap, statsFilter.getCommodityRequestsList(), answer);
        }

        answer.addAll(histUtilizationReader.getRecords(entityIds, statsFilter));
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
     * @param globalFilter The global filter to apply to all returned stats.
     * @return an ImmutableList of DB Stats Records containing the result from searching all the stats tables
     * for the given time range and commodity names
     * @throws VmtDbException if there's an exception querying the data
     */
    public @Nonnull List<Record> getFullMarketStatsRecords(@Nonnull final StatsFilter statsFilter,
                                                 @Nonnull GlobalFilter globalFilter)
            throws VmtDbException {
        final Optional<TimeRange> timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter,
            Optional.empty(), Optional.empty(), Optional.empty());
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
        final Condition timeRangeCondition = HistoryStatsUtils.betweenStartEndTimestampCond(getTimestampField(table, SNAPSHOT_TIME),
                timeRange.getTimeFrame(), timeRange.getStartTime(), timeRange.getEndTime());
        if (timeRangeCondition != null) {
            whereConditions.add(timeRangeCondition);
        }

        final FullMarketRatioProcessor ratioProcessor =
            fullMarketRatioProcessorFactory.newProcessor(statsFilter);

        // add select on the given commodity requests; if no commodityRequests specified,
        // leave out the where clause and thereby include all commodities.
        final Optional<Condition> commodityRequestsCond =
                statsQueryFactory.createCommodityRequestsCond(
                    ratioProcessor.getFilterWithCounts().getCommodityRequestsList(), table);
        commodityRequestsCond.ifPresent(whereConditions::add);

        // if a related entity type provided, add a where clause to restrict to that entityType
        final Optional<Condition> entityTypeCond = statsQueryFactory.entityTypeCond(
            Sets.newHashSet(globalFilter.getRelatedEntityTypeList()), table);
        entityTypeCond.ifPresent(whereConditions::add);

        if (globalFilter.hasEnvironmentType()) {
            statsQueryFactory.environmentTypeCond(globalFilter.getEnvironmentType(), table)
                .ifPresent(whereConditions::add);
        }

        // Format the query.
        // No need to order or group by anything, since when preparing the response
        // we rearrange things by snapshot time anyway.
        final Query query = historydbIO.JooqBuilder()
                .select(Arrays.asList(table.fields()))
                .from(table)
                .where(whereConditions);

        logger.debug("Running query... {}", query);
        final Result<? extends Record> result = historydbIO.execute(Style.FORCED, query);

        // Fill in the ratio counts and add/remove entity counts based on the requested commodities
        final List<Record> answer = ratioProcessor.processResults(result, timeRange.getMostRecentSnapshotTime());

        answer.addAll(histUtilizationReader.getRecords(Collections.emptySet(), statsFilter));
        final Duration overallElapsed = Duration.between(overallStart, Instant.now());
        logger.debug("total stats returned: {}, overall elapsed: {}", answer.size(),
                overallElapsed.toMillis() / 1000.0);
        return answer;
    }

    /**
     * If any stats requiring entity counts are requested, then count entity types and
     * add stats records to the snapshot.
     *
     * If the commodityNames list is null or empty, then include all count-based stats.
     *  @param snapshotTimestamp the time of the current snapshot
     * @param entityClassMap map from entity OID to entity class name
     * @param commodityNames a list of the commodity names to filter on
     * @param countStatsRecords the list that count stats will be added to
     */
    private void addCountStats(@Nonnull final Timestamp snapshotTimestamp,
                               @Nonnull Map<String, String> entityClassMap,
                               @Nullable List<CommodityRequest> commodityNames,
                               @Nonnull List<Record> countStatsRecords) {

        // derive list of calculated metrics
        List<String> filteredCommodityNames = Lists.newArrayList(HistoryStatsUtils.countPerSEsMetrics);
        filteredCommodityNames.addAll(HistoryStatsUtils.countSEsMetrics.values());

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
        for (String entityTypeName : HistoryStatsUtils.countSEsMetrics.keySet()) {
            entityTypeCounts.put(entityTypeName, 0);
        }
        // count the entity types that we care about
        entityClassMap.forEach((String entityOid, String entityType) -> {
            if (HistoryStatsUtils.countSEsMetrics.containsKey(entityType)) {
                entityTypeCounts.put(entityType, entityTypeCounts.get(entityType) + 1);
            }
        });

        // for requested calculated stat, add a db record
        ImmutableBiMap<String, String> mapStatToEntityType = HistoryStatsUtils.countSEsMetrics.inverse();
        for (final String commodityName : filteredCommodityNames) {
            final Record countRecord;
            if (mapStatToEntityType.containsKey(commodityName)) {
                final PmStatsLatestRecord record = new PmStatsLatestRecord();
                record.setSnapshotTime(snapshotTimestamp);
                record.setPropertyType(commodityName);
                record.setAvgValue((double)entityTypeCounts.get(mapStatToEntityType.get(commodityName)));
                record.setRelation(RelationType.METRICS);
                countRecord = record;
            } else {
                countRecord = ratioRecordFactory.makeRatioRecord(
                    snapshotTimestamp, commodityName, entityTypeCounts);
            }
            countStatsRecords.add(countRecord);
        }
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
