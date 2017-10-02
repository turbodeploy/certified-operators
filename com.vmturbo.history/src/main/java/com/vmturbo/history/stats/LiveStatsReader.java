package com.vmturbo.history.stats;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.history.utils.HistoryStatsUtils.betweenStartEndTimestampCond;
import static com.vmturbo.history.utils.HistoryStatsUtils.countPerSEsMetrics;
import static com.vmturbo.history.utils.HistoryStatsUtils.countSEsMetrics;
import static com.vmturbo.reports.db.StringConstants.AVG_VALUE;
import static com.vmturbo.reports.db.StringConstants.CAPACITY;
import static com.vmturbo.reports.db.StringConstants.COMMODITY_KEY;
import static com.vmturbo.reports.db.StringConstants.CONTAINER;
import static com.vmturbo.reports.db.StringConstants.MAX_VALUE;
import static com.vmturbo.reports.db.StringConstants.MIN_VALUE;
import static com.vmturbo.reports.db.StringConstants.NUM_CNT_PER_HOST;
import static com.vmturbo.reports.db.StringConstants.NUM_CNT_PER_STORAGE;
import static com.vmturbo.reports.db.StringConstants.NUM_CONTAINERS;
import static com.vmturbo.reports.db.StringConstants.NUM_HOSTS;
import static com.vmturbo.reports.db.StringConstants.NUM_STORAGES;
import static com.vmturbo.reports.db.StringConstants.NUM_VMS;
import static com.vmturbo.reports.db.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.reports.db.StringConstants.NUM_VMS_PER_STORAGE;
import static com.vmturbo.reports.db.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.reports.db.StringConstants.PRODUCER_UUID;
import static com.vmturbo.reports.db.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.reports.db.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.reports.db.StringConstants.RELATION;
import static com.vmturbo.reports.db.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.reports.db.StringConstants.STORAGE;
import static com.vmturbo.reports.db.StringConstants.UUID;
import static com.vmturbo.reports.db.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.reports.db.jooq.JooqUtils.betweenStartEndCond;
import static com.vmturbo.reports.db.jooq.JooqUtils.dField;
import static com.vmturbo.reports.db.jooq.JooqUtils.floorDateTime;
import static com.vmturbo.reports.db.jooq.JooqUtils.number;
import static com.vmturbo.reports.db.jooq.JooqUtils.statsTableByTimeFrame;
import static com.vmturbo.reports.db.jooq.JooqUtils.str;
import static com.vmturbo.reports.db.jooq.JooqUtils.timestamp;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.min;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.reports.db.BasedbIO;
import com.vmturbo.reports.db.BasedbIO.Style;
import com.vmturbo.reports.db.EntityType;
import com.vmturbo.reports.db.RelationType;
import com.vmturbo.reports.db.TimeFrame;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.Tables;
import com.vmturbo.reports.db.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.reports.db.abstraction.tables.records.PmStatsLatestRecord;

/**
 * Read from the stats database tables for the "live", i.e. real, discovered topology.
 **/
public class LiveStatsReader {

    private final static long MINUTE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private final static long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);

    private static final Logger logger = LogManager.getLogger();

    // Partition the list of entities to read into chunks of this size in order not to flood the DB.
    private static final int ENTITIES_PER_CHUNK = 50000;

    private final HistorydbIO historydbIO;

    private final int numRetainedMinutes;
    private final int numRetainedHours;
    private final int numRetainedDays;


    public LiveStatsReader(HistorydbIO historydbIO, int numRetainedMinutes, int numRetainedHours,
                           int numRetainedDays) {
        this.historydbIO = historydbIO;
        this.numRetainedMinutes = numRetainedMinutes;
        this.numRetainedHours = numRetainedHours;
        this.numRetainedDays = numRetainedDays;
    }

    /**
     * Indicate an aggregation style for this query; defined in legacy.
     */
    public enum AGGREGATE {NO_AGG, AVG_ALL, AVG_MIN_MAX}

    /**
     * Fetch rows from the stats tables based on the date range, and looking in the appropriate table
     * for each entity.
     *
     * Takes a list of entities, any of which might be a group; groups are expanded, and stats are
     * accumulated by individual serviceEntity.
     *
     * This requires looking up the entity type for each entity id in the list, and and then iterating
     * over the time-based tables for that entity type.
     *
     * @param entityIds a list of primary-level entities to gather stats from; groups have been
     *                  expanded before we get here
     * @param startTime the timestamp of the oldest stats to gather; the default is "now"
     * @param endTime the timestamp of the most recent stats to gather; the default is "now"
     * @param commodityNames a list of commodities to gather; the default is "all commodities known"
     * @return a list of Jooq records, one for each stats information row retrieved
     */
    public @Nonnull List<Record> getStatsRecords(@Nonnull List<String> entityIds,
                                                 @Nullable Long startTime,
                                                 @Nullable Long endTime,
                                                 @Nonnull List<String> commodityNames)
            throws VmtDbException {


        // get most recent date from _latest database
        final Optional<Timestamp> mostRecentTimestamp = historydbIO.getMostRecentTimestamp();
        if (!mostRecentTimestamp.isPresent()) {
            // no data persisted yet; just return an empty answer
            return Collections.emptyList();
        }
        long now = mostRecentTimestamp.get().getTime();
        startTime = applyTimeDefault(startTime, now);
        endTime = applyTimeDefault(endTime, now);

        Map<String, String> entityClsMap = historydbIO.getTypesForEntities(entityIds);

        Multimap<String, String> entityIdsByType = HashMultimap.create();
        for (String serviceEntityId : entityIds) {
            String entityClass = entityClsMap.get(serviceEntityId);
            entityIdsByType.put(entityClass, serviceEntityId);
        }

        // Accumulate stats records, iterating by entity type at the top level
        List<Record> answer = new ArrayList<>();
        Instant overallStart = Instant.now();
        for (Map.Entry<String, Collection<String>> entityTypeAndId : entityIdsByType.asMap().entrySet()) {
            String entityClsName = entityTypeAndId.getKey();
            logger.debug("fetch stats for entity type {}", entityClsName);

            Optional<EntityType> entityType = EntityType.getTypeForName(entityClsName);
            if (!entityType.isPresent()) {
                // no entity type found for this class name; not supposed to happen
                logger.warn("DB Entity type not found for clsName {}", entityClsName);
                continue;
            }
            List<String> entityIdsForType  = Lists.newArrayList(entityTypeAndId.getValue());
            final int numberOfEntitiesToPersist = entityIdsForType.size();
            logger.debug("entity count for {} = {}", entityClsName, numberOfEntitiesToPersist);
            Instant start = Instant.now();

            int entityIndex = 0;
            while(entityIndex < numberOfEntitiesToPersist) {
                int nextIndex = Math.min(entityIndex+ENTITIES_PER_CHUNK, numberOfEntitiesToPersist);
                List<String> entityIdChunk = entityIdsForType.subList(entityIndex, nextIndex);
                Optional<Select<?>> query = getQueryString(entityIdChunk, entityType.get(),
                        commodityNames, startTime, endTime, AGGREGATE.NO_AGG);
                if (!query.isPresent()) {
                    continue;
                }
                Result<? extends Record> statsRecords = historydbIO.execute(BasedbIO.Style.FORCED,
                        query.get());
                int answerSize = statsRecords.size();
                answer.addAll(statsRecords);
                logger.debug("  chunk size {}, statsRecords {}", entityIdChunk.size(), answerSize);
                entityIndex = entityIndex + ENTITIES_PER_CHUNK;
            }
            Duration elapsed = Duration.between(start, Instant.now());
            if (logger.isDebugEnabled()) {
                logger.debug(" answer size {}, fetch time: {}", answer.size(), elapsed);
            }
        }

        // if requested, add counts
        addCountStats(startTime, entityClsMap, commodityNames, answer);
        Duration overallElapsed = Duration.between(overallStart, Instant.now());
        logger.debug("total stats returned: {}, overall elapsed: {}", answer.size(), overallElapsed);
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
     * @param startTime beginning time range to query
     * @param endTime end time range
     * @param commodityNames the list of commodities to search
     * @return an ImmutableList of DB Stats Records containing the result from searching all the stats tables
     * for the given time range and commodity names
     * @throws VmtDbException if there's an exception querying the data
     */
    public @Nonnull List<Record> getFullMarketStatsRecords(
                                                 @Nullable Long startTime,
                                                 @Nullable Long endTime,
                                                 @Nonnull List<String> commodityNames)
            throws VmtDbException {

        // get most recent date from _latest database
        final Optional<Timestamp> mostRecentTimestamp = historydbIO.getMostRecentTimestamp();
        if (!mostRecentTimestamp.isPresent()) {
            // no data persisted yet; just return an empty answer
            return ImmutableList.of();
        }
        long now = mostRecentTimestamp.get().getTime();
        startTime = applyTimeDefault(startTime, now);
        endTime = applyTimeDefault(endTime, now);

        logger.debug("getting stats for full market");

        Instant overallStart = Instant.now();

        final TimeFrame tFrame = millis2TimeFrame(startTime);
        final Table<?> table = getMarketStatsTable(tFrame);

        // accumulate the conditions for this query
        List<Condition> whereConditions = new ArrayList<>();

        // add where clause for time range; null if the timeframe cannot be determined
        final Condition timeRangeCondition = betweenStartEndTimestampCond(dField(table, SNAPSHOT_TIME),
                tFrame, startTime, endTime);
        if (timeRangeCondition != null) {
            whereConditions.add(timeRangeCondition);
        }
        // add select on the given commodity names; if no commodityNames specified,
        // leave out the were clause and thereby include all commodities.
        Optional<Condition> commodityNamesCond = commodityNamesCond(commodityNames, table);
        commodityNamesCond.ifPresent(whereConditions::add);

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
        if (!commodityNames.isEmpty()) {
            // This does countStats.size() lookups in commodityNames, which is a list.
            // This is acceptable because the asked-for commodityNames is supposed to be
            // a small ( < 10) list.
            requestedRatioProps.retainAll(commodityNames);
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
                    Timestamp statTime = record.getValue(timestamp(dField(table, SNAPSHOT_TIME)));
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
            entityCountsByTime.entrySet().forEach(snapshotEntityCounts -> {
                final Timestamp snapshotTime = snapshotEntityCounts.getKey();

                final Map<String, Float> entityCounts = snapshotEntityCounts.getValue();
                requestedRatioProps.forEach(ratioPropName -> {
                    double ratio = 0;
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
                                entityCounts.getOrDefault(NUM_VMS, 0f) / numStorages: 0;
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
                });
            });
        }


        final Duration overallElapsed = Duration.between(overallStart, Instant.now());
        logger.debug("total stats returned: {}, overall elapsed: {}", answer.size(),
                overallElapsed.toMillis() / 1000.0);
        return Collections.unmodifiableList(answer);
    }

    /**
     * Check a time parameter, e.g. 'startTime' or 'endTime' to see if a default should be applied.
     * If the 'timeParam' is null or zero, then use 'default'.
     *
     * @param timeParam the time parameter to check for default usage
     * @param defaultTime the default value to apply if the original timeParam is zero or null
     * @return the time value to use, either the original 'timeParam' or 'defaultTime' if timeParam
     * is zero or null
     */
    private Long applyTimeDefault(@Nullable Long timeParam, long defaultTime) {
        return (timeParam != null &&  timeParam != 0) ? timeParam : defaultTime;
    }

    /**
     * If any stats requiring entity counts are requested, then count entity types and
     * add stats records to the snapshot.
     *
     * If the commodityNames list is null or empty, then include all count-based stats.
     *
     * @param snapshot_time the time of the current snapshot
     * @param entityClassMap map from entity OID to entity class name
     * @param commodityNames a list of the commodity names to filter on
     * @param countStatsRecords the list that count stats will be added to
     */
    private void addCountStats(long snapshot_time,
                               @Nonnull Map<String, String> entityClassMap,
                               @Nullable List<String> commodityNames,
                               @Nonnull List<Record> countStatsRecords) {

        // use the startTime for the all counted stats
        final Timestamp snapshotTime = new Timestamp(snapshot_time);

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
            countRecord.setSnapshotTime(snapshotTime);
            countRecord.setPropertyType(commodityName);
            countRecord.setAvgValue(statValue);
            countRecord.setRelation(RelationType.COMMODITIES_FROM_ATTRIBUTES);
            countStatsRecords.add(countRecord);
        }
    }

    /**
     * Formulate a query string for commodities of a given entity
     * during time interval between startTime and endTime.
     *
     * <p>The database table is determined from the EntityType of the given entity.
     *
     * <p>Copied from VMtdbio.java in OpsManager
     *
     * <p>note: when the stats roll-up is implemented, the time-frame must be used to iterate over
     * the different stats tables, e.g. _latest, _hourly, _daily, etc.
     *
     * @param entityIdChunk the Entity OID to which the commodities belong
     * @param entityType the EntityType
     * @param commodityNames a list of commodity names to gather; default is all known commodities
     * @param startTime the oldest snapshot time to include
     * @param endTime the most recent snapshot time to include
     * @param agg whether or not to aggregate results
     * @return a optional with a Jooq query ready to capture the desired stats, or empty()
     * if there is no db table for this entity type and time frame, e.g.
     * CLUSTER has no Hourly and Latest tables.
     */
    private @Nonnull Optional<Select<?>> getQueryString(@Nonnull List<String> entityIdChunk,
                             EntityType entityType,
                             List<String> commodityNames,
                             long startTime,
                             long endTime,
                             AGGREGATE agg) {

        TimeFrame tFrame = millis2TimeFrame(startTime);

        Table<?> table = statsTableByTimeFrame(entityType, tFrame);
        // check there is a table for this entityType and tFrame; and it has a SNAPSHOT_TIME column
        if (table == null || table.field(SNAPSHOT_TIME) == null) {
            return Optional.empty();
        }

        // accumulate the conditions for this query
        List<Condition> whereConditions = new ArrayList<>();

        // add where clause for time range; null if the timeframe cannot be determined
        final Condition timeRangeCondition = betweenStartEndCond(dField(table, SNAPSHOT_TIME),
                tFrame, startTime, endTime);
        if (timeRangeCondition != null) {
            whereConditions.add(timeRangeCondition);
        }

        // include an "in()" clause for uuids, if any
        if (entityIdChunk.size() > 0) {
            Condition uuidCond = uuidCond(table, entityIdChunk);
            whereConditions.add(uuidCond);
        }

        // note: the legacy DB code defines expression conditions that are not used by new UI

        // add select on the given commodity names; if no commodityNames specified,
        // leave out the were clause and thereby include all commodities.
        Optional<Condition> commodityNamesCond = commodityNamesCond(commodityNames, table);
        commodityNamesCond.ifPresent(whereConditions::add);

        // whereConditions.add(propertyExprCond);  // TODO: implement expression conditions

        // the fields to return
        List<Field<?>> selectFields = Lists.newArrayList(
                floorDateTime(dField(table, SNAPSHOT_TIME), tFrame).as(SNAPSHOT_TIME),
                dField(table, PROPERTY_TYPE),
                dField(table, PROPERTY_SUBTYPE),
                dField(table, PRODUCER_UUID),
                dField(table, CAPACITY),
                dField(table, RELATION),
                dField(table, COMMODITY_KEY));

        // the fields to order by and group by
        Field<?>[] orderGroupFields = new Field<?>[]{
                dField(table, SNAPSHOT_TIME),
                dField(table, UUID),
                dField(table, PROPERTY_TYPE),
                dField(table, PROPERTY_SUBTYPE),
                dField(table, RELATION)
        };

        Select<?> statsQueryString;
        switch (agg) {
            case NO_AGG:
                selectFields.add(0, dField(table, UUID));
                selectFields.add(0, dField(table, AVG_VALUE));
                selectFields.add(0, dField(table, MIN_VALUE));
                selectFields.add(0, dField(table, MAX_VALUE));

                statsQueryString = historydbIO.getStatsSelect(table, selectFields, whereConditions,
                        orderGroupFields);
                break;

            case AVG_ALL:
                selectFields.add(0, avg(number(dField(table, AVG_VALUE))).as(AVG_VALUE));
                selectFields.add(0, avg(number(dField(table, MIN_VALUE))).as(MIN_VALUE));
                selectFields.add(0, avg(number(dField(table, MAX_VALUE))).as(MAX_VALUE));

                statsQueryString = historydbIO.getStatsSelectWithGrouping(table, selectFields,
                        whereConditions, orderGroupFields);
                break;

            case AVG_MIN_MAX:
                selectFields.add(0, avg(number(dField(table, AVG_VALUE))).as(AVG_VALUE));
                selectFields.add(0, min(number(dField(table, MIN_VALUE))).as(MIN_VALUE));
                selectFields.add(0, max(number(dField(table, MAX_VALUE))).as(MAX_VALUE));

                statsQueryString = historydbIO.getStatsSelectWithGrouping(table, selectFields,
                        whereConditions, orderGroupFields);
                break;

            default:
                throw new IllegalArgumentException("Illegal value for AGG: " + agg);
        }
        return Optional.of(statsQueryString);
    }

    /**
     * Create a Jooq conditional clause to include only the desired commodity names.
     *
     * If commodityNames is null or empty, return an empty {@link Optional}
     * indicating there should be no selection condition on the commodity name. In other words,
     * all commodities will be returned.
     *
     * @param commodityNames a list of commodity names to include in the result set; null or empty
     *                       list implies no commodity names condition at all
     * @param table the DB table from which these stats will be collected
     * @return an Optional containing a Jooq conditional to only include the desired commodities,
     * or Optional.empty() if no commodity selection is desired
     */
    private @Nonnull Optional<Condition> commodityNamesCond(@Nullable List<String> commodityNames,
                                                            @Nonnull Table<?> table) {
        if (commodityNames == null || commodityNames.isEmpty()) {
            return Optional.empty();
        }
        final Condition whereInCommodityNames = str(dField(table, PROPERTY_TYPE))
                .in(commodityNames);
        return Optional.of(whereInCommodityNames);
    }

    /**
     * Clip a millisecond epoch number to a {@link TimeFrame}, e.g. LATEST, HOUR, DAY, etc. ago.
     *
     * @param millis a millisecond epoch number in the past
     * @return a {@link TimeFrame} representing how far in the past the given ms epoch number is.
     */
    private  TimeFrame millis2TimeFrame(long millis) {
        return millisAgo2TimeFrame(System.currentTimeMillis() - millis);
    }


    /**
     * Convert a time interval, milliseconds in the past, to a {@link TimeFrame},
     * e.g. LATEST, HOUR, DAY, MONTH.
     *
     * <p>note that the parameter timeBackMillis is a positive number.
     *
     * @param timeBackMillis how far in the past to look
     * @return a {@link TimeFrame} denoting how far in the past the given time interval is
     */
    private TimeFrame millisAgo2TimeFrame(long timeBackMillis) {
        checkArgument(timeBackMillis > 0);
        long tMinutesBack = timeBackMillis / MINUTE_MILLIS;
        if (tMinutesBack <= getNumRetainedMinutes()) {
            return TimeFrame.LATEST;
        }
        long tHoursBack = timeBackMillis / HOUR_MILLIS;
        if (tHoursBack <= getNumRetainedHours()) {
            return TimeFrame.HOUR;
        }
        if (tHoursBack / 24 <= getNumRetainedDays()) {
            return TimeFrame.DAY;
        }
        return TimeFrame.MONTH;
    }

    /**
     * Create a Jooq conditional to query for any of the given UUIDs
     *
     * @param table the DB table which will be queried
     * @param uuids a list of UUID values to be included in the result
     * @return a Jooq Conditional to add to a query which will select for a UUID in the given list.
     */
    private static Condition uuidCond(Table<?> table, List<String> uuids) {
        return str(dField(table, UUID)).in(uuids);
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

    /**
     * How long should stats values be retained in the _latest table.
     *
     * @return the time, in minutes, that stats should be retained in the _latest table
     */
    private int getNumRetainedMinutes() {
        return numRetainedMinutes;
    }

    /**
     * How long should stats values be retained in the by_hour table
     * @return the time, in hours, that stats should be retained in the by_hour table
     */
    private int getNumRetainedHours() {
        return numRetainedHours;
    }

    /**
     * How long should stats values be retained in the by_days table.
     *
     * @return the time, in days, that stats should be retained in the by_days table
     */
    private int getNumRetainedDays() {
        return numRetainedDays;
    }

}
