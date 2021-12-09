package com.vmturbo.history.stats.readers;

import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.HistorydbIO.NextPageInfo;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.stats.INonPaginatingStatsReader;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.StatsRecordsProcessor;
import com.vmturbo.history.stats.live.PropertyType;
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

    private static final DataMetricSummary GET_STATS_RECORDS_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("history_get_live_stats_records_duration_seconds")
            .withHelp("Duration in seconds it takes the history component to get live stat snapshots for a set of entities.")
            .build()
            .register();
    private final ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory;

    private final INonPaginatingStatsReader<HistUtilizationRecord> histUtilizationReader;
    private final int entitiesPerChunk;
    private final DSLContext dsl;

    public LiveStatsReader(@Nonnull final HistorydbIO historydbIO,
            @Nonnull final DSLContext dsl,
            @Nonnull final TimeRangeFactory timeRangeFactory,
            @Nonnull final StatsQueryFactory statsQueryFactory,
            @Nonnull final ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory,
            @Nonnull INonPaginatingStatsReader<HistUtilizationRecord> histUtilizationReader,
            int entitiesPerChunk) {
        this.historydbIO = historydbIO;
        this.dsl = dsl;
        this.timeRangeFactory = timeRangeFactory;
        this.statsQueryFactory = statsQueryFactory;
        this.computedPropertiesProcessorFactory = computedPropertiesProcessorFactory;
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
     * @param statsFilter      The filter specifying which stats to get. If the filter time range spans
     *                         across multiple snapshots, the sort order for pagination will be
     *                         derived from the most recent snapshot. However, once we determine the
     *                         IDs of the entities in the next page, we retrieve records for those
     *                         entities from all matching snapshots.
     * @param paginationParams The {@link EntityStatsPaginationParams} specifying the pagination
     *                         parameters to use.
     * @return A {@link StatRecordPage} containing the next page of per-entity records and the
     *         next cursor for subsequent calls to this function.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    public StatRecordPage getPaginatedStatsRecords(@Nonnull final EntityStatsScope entityStatsScope,
            @Nonnull final StatsFilter statsFilter,
            @Nonnull final EntityStatsPaginationParams paginationParams) throws
            DataAccessException {

        EntityType entityType = historydbIO.getEntityTypeFromEntityStatsScope(entityStatsScope);

        // resolve the time range for pagination param
        final Optional<TimeRange> paginationTimeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter,
                Optional.empty(), Optional.ofNullable(entityType), Optional.of(paginationParams), Optional.empty());

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
                paginationTimeRange,
                paginationParams,
                entityType,
                statsFilter);

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

        final List<String> nextPageEntityOids = nextPageInfo.getEntityOids();
        Optional<ResultQuery<?>> query = statsQueryFactory.createStatsQuery(nextPageEntityOids,
                nextPageInfo.getTable(), statsFilter.getCommodityRequestsList(),
                paginationTimeRange, AGGREGATE.NO_AGG);
        if (!query.isPresent()) {
            return StatRecordPage.empty();
        }
        // Run the query to get all relevant stat records.
        // TODO (roman, Jun 29 2018): Ideally we should get the IDs of entities in the page and
        // run the query to get the stats in the same transaction.
        final Result<? extends Record> statsRecords = dsl.fetch(query.get());
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
     * for each entity.
     *
     * <p>This method returns individual records. It is the caller's responsibility
     * to accumulate them.</p>
     *
     * <p>This requires looking up the entity type for each entity id in the list, and and then iterating
     * over the time-based tables for that entity type.<p/>
     *
     * <p>The time interval is widened to ensure we capture past stats when startTime == endTime.</p>
     *
     * @param entityIds   a list of primary-level entities to gather stats from; groups have been
     *                    expanded before we get here
     * @param statsFilter stats filter constructed for the query
     * @return a list of records records, one for each stats information row retrieved
     * @throws DataAccessException if a DB error occurs
     */
    @Nonnull
    public List<Record> getRecords(
            @Nonnull final Set<String> entityIds,
            @Nonnull final StatsFilter statsFilter)
            throws DataAccessException {
        return getRecords(entityIds, statsFilter, Collections.emptyList());
    }

    /**
     * Fetch rows from the stats tables based on the date range, and looking in the appropriate table
     * for each entity.
     *
     * <p>This method returns individual records. It is the caller's responsibility
     * to accumulate them.</p>
     *
     * <p>This requires looking up the entity type for each entity id in the list, and and then iterating
     * over the time-based tables for that entity type.<p/>
     *
     * <p>The time interval is widened to ensure we capture past stats when startTime == endTime.</p>
     *
     * @param entityIds   a list of primary-level entities to gather stats from; groups have been
     *                    expanded before we get here
     * @param statsFilter stats filter constructed for the query
     * @param derivedEntityTypes related entity types for which scope expansion was attempted.
     * @return a list of records records, one for each stats information row retrieved
     * @throws DataAccessException if a DB error occurs
     */
    @Nonnull
    public List<Record> getRecords(
            @Nonnull final Set<String> entityIds,
            @Nonnull final StatsFilter statsFilter,
            @Nonnull final List<Integer> derivedEntityTypes)
            throws DataAccessException {

        final DataMetricTimer timer = GET_STATS_RECORDS_DURATION_SUMMARY.startTimer();

        final List<CommodityRequest> commodityRequests = statsFilter.getCommodityRequestsList();

        // find out of which types those entities are ...(Map<oid, typeName>)
        final Map<String, String> entityIdToTypeMap = historydbIO.getTypesForEntities(entityIds);
        // handle any missing records
        final Set<String> notFoundIds = Sets.difference(entityIds, entityIdToTypeMap.keySet());
        if (!notFoundIds.isEmpty()) {
            logger.warn(
                    "Entity OIDs not found in DB (probably for an entity type that is not saved): {}",
                    notFoundIds);
        }
        // ... and create a reverse map using resolved EntityTypes (type -> list of oid), plus
        // a map keeping track of class names that did not map to entity types
        final ListMultimap<EntityType, String> entityTypeToIdsMap = ArrayListMultimap.create();
        final ListMultimap<String, String> missingTypeNameToIdsMap = ArrayListMultimap.create();
        entityIdToTypeMap.forEach((id, className) -> {
            final Optional<EntityType> entityType = EntityType.named(className);
            // add this id to the list from the found or missing map, as appropriate
            entityType.map(entityTypeToIdsMap::get)
                    .orElse(missingTypeNameToIdsMap.get(className))
                    .add(id);
        });
        missingTypeNameToIdsMap.asMap().forEach((className, ids) ->
                logger.warn("DB Entity type not found for entity class name {}, oids {}",
                        className, ids));

        Optional<TimeRange> timeRangeOpt = Optional.empty();

        // Accumulate stats records, iterating by entity type at the top level
        final List<Record> answer = new ArrayList<>();
        for (EntityType entityType : entityTypeToIdsMap.keySet()) {
            logger.debug("fetch stats for entity type {}", entityType);

            // get the entities of that type
            final List<String> entityIdsForType = entityTypeToIdsMap.get(entityType);
            logger.debug("entity count for {} = {}", entityType, entityIdsForType.size());

            // create a timerange, given the start/end range in the filter
            // use also the entities in order to be more exact on the timerange calculation
            timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter, Optional.of(entityIdsForType),
                    Optional.of(entityType), Optional.empty(), Optional.empty());
            if (!timeRangeOpt.isPresent()) {
                // no data persisted yet; just return an empty answer
                logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps."
                        + " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate());
                continue;
            }
            final TimeRange timeRange = timeRangeOpt.get();

            final Instant start = Instant.now();

            for (List<String> entityIdChunk : Lists.partition(entityIdsForType, entitiesPerChunk)) {
                final Optional<Table<?>> table = entityType.getTimeFrameTable(timeRange.getTimeFrame());
                if (!table.isPresent()) {
                    continue;
                }
                final Optional<ResultQuery<?>> query = statsQueryFactory.createStatsQuery(
                        entityIdChunk, table.get(),
                        commodityRequests, timeRange, AGGREGATE.NO_AGG);
                if (!query.isPresent()) {
                    continue;
                }
                final Result<? extends Record> statsRecords = dsl.fetch(query.get());
                final int answerSize = statsRecords.size();
                if (logger.isDebugEnabled() && answerSize == 0) {
                    logger.debug("zero answers returned from: {}, time range: {}",
                            query.get(), timeRange);
                }
                answer.addAll(statsRecords);
                logger.debug("  chunk size {}, statsRecords {}", entityIdChunk.size(), answerSize);
            }
            final Duration elapsed = Duration.between(start, Instant.now());
            if (logger.isDebugEnabled()) {
                logger.debug(" answer size {}, fetch time: {}", answer.size(), elapsed);
            }
        }

        // in thise case the timerange will be the value retrieved from the last loop
        // else we didn't manage to retrieve anything
        if (timeRangeOpt.isPresent()) {
            final List<PropertyType> requestedProperties = commodityRequests.stream()
                    .map(CommodityRequest::getCommodityName)
                    .map(PropertyType::named)
                    .collect(Collectors.toList());
            answer.addAll(getCountStats(timeRangeOpt.get().getMostRecentSnapshotTime(),
                    entityTypeToIdsMap,
                    requestedProperties.isEmpty()
                            ? PropertyType.getMetricPropertyTypes().stream()
                            : requestedProperties.stream().filter(PropertyType::isCountMetric),
                    derivedEntityTypes));

            answer.addAll(getComputedStats(timeRangeOpt.get().getMostRecentSnapshotTime(),
                    entityTypeToIdsMap,
                    requestedProperties.stream().filter(PropertyType::isComputed),
                    derivedEntityTypes));
        }

        answer.addAll(histUtilizationReader.getRecords(entityIds, statsFilter));

        final double elapsedSeconds = timer.observe();
        logger.debug("total stats returned: {}, overall elapsed: {}", answer.size(), elapsedSeconds);
        return answer;
    }

    /**
     * Get the full-market stats table to use, based on the time frame we're looking at.
     *
     * @param timeFrame The time frame.
     * @return The table to use.
     */
    private @Nonnull
    Table<?> getMarketStatsTable(@Nonnull final TimeFrame timeFrame) {
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
     * @param statsFilter  The filter to use to get the stats.
     * @param globalFilter The global filter to apply to all returned stats.
     * @return an ImmutableList of DB Stats Records containing the result from searching all the stats tables
     * for the given time range and commodity names
     * @throws DataAccessException if there's an exception querying the data
     */
    public @Nonnull
    List<Record> getFullMarketStatsRecords(@Nonnull final StatsFilter statsFilter,
            @Nonnull GlobalFilter globalFilter)
            throws DataAccessException {
        final Optional<TimeRange> timeRangeOpt = timeRangeFactory.resolveTimeRange(statsFilter,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        if (!timeRangeOpt.isPresent()) {
            // no data persisted yet; just return an empty answer
            logger.warn("Stats filter with start {} and end {} does not resolve to any timestamps."
                    + " There may not be any data.", statsFilter.getStartDate(), statsFilter.getEndDate());
            return Collections.emptyList();
        }
        final TimeRange timeRange = timeRangeOpt.get();

        logger.debug("getting stats for full market");

        final Instant overallStart = Instant.now();

        final Table<?> table = getMarketStatsTable(timeRange.getTimeFrame());

        // accumulate the conditions for this query
        List<Condition> whereConditions = new ArrayList<>();

        // add where clause for time range; null if the timeframe cannot be determined
        final Condition timeRangeCondition = HistoryStatsUtils.betweenStartEndTimestampCond(getTimestampField(table, SNAPSHOT_TIME),
                timeRange.getTimeFrame(), timeRange.getStartTime(), timeRange.getEndTime());
        if (timeRangeCondition != null) {
            whereConditions.add(timeRangeCondition);
        }

        final ComputedPropertiesProcessor computedPropertiesProcessor =
                computedPropertiesProcessorFactory.getProcessor(
                        statsFilter, new StatsRecordsProcessor(MarketStatsLatestRecord::new));


        // add select on the given commodity requests; if no commodityRequests specified,
        // leave out the where clause and thereby include all commodities.
        final Optional<Condition> commodityRequestsCond =
                statsQueryFactory.createCommodityRequestsCond(
                        computedPropertiesProcessor
                                .getAugmentedFilter().getCommodityRequestsList(), table);
        commodityRequestsCond.ifPresent(whereConditions::add);

        // add conditions to exclude zero-valued count metrics if no commodities are
        // explicitly requested
        final Optional<Condition> excludeZeroCountRecords =
                statsQueryFactory.createExcludeZeroCountRecordsCond(statsFilter, table);
        excludeZeroCountRecords.ifPresent(whereConditions::add);

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
        final ResultQuery<?> query = dsl.select(Arrays.asList(table.fields()))
                .from(table)
                .where(whereConditions);

        logger.debug("Running query... {}", query);
        final Result<? extends Record> result = dsl.fetch(query);

        // Fill in the ratio counts and add/remove entity counts based on the requested commodities
        final List<Record> answer = computedPropertiesProcessor
                .processResults(result, timeRange.getMostRecentSnapshotTime());

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
     * <p>If the commodityRequests list is null or empty, then include all count-based stats.</p>
     *
     * @param snapshotTimestamp  the time of the current snapshot
     * @param entityTypeToIdsMap multimap relating entity types to entity OIDs
     * @param requestedMetrics   metric property type that needs to be computed
     * @param derivedEntityTypes related entity types for which scope expansion was attempted.
     * @return records with the needed metrics
     */
    private List<Record> getCountStats(@Nonnull final Timestamp snapshotTimestamp,
            final @Nonnull ListMultimap<EntityType, String> entityTypeToIdsMap,
            final @Nullable Stream<PropertyType> requestedMetrics,
            final @Nonnull List<Integer> derivedEntityTypes) {

        List<Record> metricRecords = new ArrayList<>();
        requestedMetrics.forEach(metric -> {
            final EntityType entityType = metric.getCountedEntityType();
            if (canCountEntityType(entityType, entityTypeToIdsMap, derivedEntityTypes)) {
                StatsRecordsProcessor recordsProcessor =
                        new StatsRecordsProcessor(entityType.getLatestTable().get());
                metricRecords.add(recordsProcessor.createRecord(snapshotTimestamp, metric,
                        entityTypeToIdsMap.containsKey(entityType)
                                ? entityTypeToIdsMap.get(entityType).size()
                                : 0));
            }
        });
        return metricRecords;
    }

    private List<Record> getComputedStats(final Timestamp snapshotTimestamp,
            final @Nonnull ListMultimap<EntityType, String> entityTypeToIdsMap,
            final @Nonnull Stream<PropertyType> computedProperties,
            final @Nonnull List<Integer> derivedEntityTypes) {
        List<Record> computedRecords = new ArrayList<>();
        computedProperties.forEach(computed -> {
            // we don't really know what record type we're dealing with here, but it's some sort of
            // stats table, and the GENERIC_STATS_TABLE will provide all the necessary field access
            StatsRecordsProcessor recordProcessor = new StatsRecordsProcessor(HistorydbIO.GENERIC_STATS_TABLE);
            final List<Double> values = computed.getOperands().stream()
                    .map(prereq -> {
                        EntityType type = prereq.getCountedEntityType();
                        if (type == null || !canCountEntityType(type, entityTypeToIdsMap, derivedEntityTypes)) {
                            return null;
                        }
                        return entityTypeToIdsMap.containsKey(type)
                                ? entityTypeToIdsMap.get(type).size()
                                : 0.0;
                    })
                    .collect(Collectors.toList());
            if (!values.contains(null)) {
                computedRecords.add(recordProcessor.createRecord(
                        snapshotTimestamp, computed, computed.compute(values)));
            }
        });
        return computedRecords;
    }

    /**
     * Return the display name for the Entity for the given entity ID (OID).
     *
     * <p>If the entity ID is null or not known, then return null.</p>
     *
     * <p>TODO: Should probably be moved to a new EntityReader class.</p>
     *
     * @param entityOID the OID for the entity to look up
     * @return the display name for the given OID, if found, otherwise null
     */
    @Nullable
    public String getEntityDisplayNameForId(@Nullable Long entityOID) {
        return entityOID != null ? historydbIO.getEntityDisplayNameForId(entityOID) : null;
    }

    /**
     * Determines whether the given entity type can be counted in the current context.
     *
     * <p>We need to check whether it makes sense to count this entity type given the current scope
     * It makes sense to count this entity type if at least one of the following criteria is met:
     *    - The entity type is present in the current scope
     *    - Scope expansion for the given entity type has been attempted as part of this request
     * In the latter case, the entity type is not present in the current scope despite having expanded the scope
     * to potentially include this entity type. Therefore, we can count a zero in this case.</p>
     *
     * @param entityType the type to check for countability
     * @param entityTypeToIdsMap multimap relating entity types to entity OIDs
     * @param derivedEntityTypes related entity types for which scope expansion was attempted.
     * @return true, if the given entity type can be counted in the current context.
     */
    private boolean canCountEntityType(EntityType entityType, ListMultimap<EntityType, String> entityTypeToIdsMap, List<Integer> derivedEntityTypes) {
        return entityTypeToIdsMap.containsKey(entityType)
                || derivedEntityTypes.contains(entityType.getSdkEntityType().get().getNumber());
    }

}
