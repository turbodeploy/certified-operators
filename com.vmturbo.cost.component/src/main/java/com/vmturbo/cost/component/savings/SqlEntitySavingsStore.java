package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.AGGREGATION_META_DATA;
import static com.vmturbo.cost.component.db.Tables.ENTITY_CLOUD_SCOPE;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_MONTH;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.trueCondition;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.Routines;
import com.vmturbo.cost.component.db.tables.records.AggregationMetaDataRecord;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsByHourRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Implementation of store that accesses savings hourly/daily/monthly DB tables.
 */
public class SqlEntitySavingsStore implements EntitySavingsStore {
    /**
     * Minimal info logging.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * JOOQ access.
     */
    private final DSLContext dsl;

    /**
     * Used for timestamp conversions before storing/reading DB values.
     */
    private final Clock clock;

    /**
     * Used for inserts, to enable batch insert. Default: 1000 ?
     */
    private final int chunkSize;

    /**
     * Need some dummy init time.
     */
    private static final LocalDateTime INIT_TIME = LocalDateTime.now();

    /**
     * Stats table field info by rollup type.
     */
    private static final Map<RollupDurationType, StatsTypeFields> statsFieldsByRollup =
            new HashMap<>();

    static {
        StatsTypeFields hourFields = new StatsTypeFields();
        hourFields.table = ENTITY_SAVINGS_BY_HOUR;
        hourFields.oidField = ENTITY_SAVINGS_BY_HOUR.ENTITY_OID;
        hourFields.timeField = ENTITY_SAVINGS_BY_HOUR.STATS_TIME;
        hourFields.typeField = ENTITY_SAVINGS_BY_HOUR.STATS_TYPE;
        hourFields.valueField = ENTITY_SAVINGS_BY_HOUR.STATS_VALUE;
        statsFieldsByRollup.put(RollupDurationType.HOURLY, hourFields);

        StatsTypeFields dayFields = new StatsTypeFields();
        dayFields.table = ENTITY_SAVINGS_BY_DAY;
        dayFields.oidField = ENTITY_SAVINGS_BY_DAY.ENTITY_OID;
        dayFields.timeField = ENTITY_SAVINGS_BY_DAY.STATS_TIME;
        dayFields.typeField = ENTITY_SAVINGS_BY_DAY.STATS_TYPE;
        dayFields.valueField = ENTITY_SAVINGS_BY_DAY.STATS_VALUE;
        statsFieldsByRollup.put(RollupDurationType.DAILY, dayFields);

        StatsTypeFields monthFields = new StatsTypeFields();
        monthFields.table = ENTITY_SAVINGS_BY_MONTH;
        monthFields.oidField = ENTITY_SAVINGS_BY_MONTH.ENTITY_OID;
        monthFields.timeField = ENTITY_SAVINGS_BY_MONTH.STATS_TIME;
        monthFields.typeField = ENTITY_SAVINGS_BY_MONTH.STATS_TYPE;
        monthFields.valueField = ENTITY_SAVINGS_BY_MONTH.STATS_VALUE;
        statsFieldsByRollup.put(RollupDurationType.MONTHLY, monthFields);
    }

    /**
     * Map of EntityType to DB column in scope table, so that we can query for right scope.
     */
    private static final Map<EntityType, TableField<EntityCloudScopeRecord, Long>> TYPE_TO_DB_MAP =
            ImmutableMap.of(
                    EntityType.BUSINESS_ACCOUNT, ENTITY_CLOUD_SCOPE.ACCOUNT_OID,
                    EntityType.REGION, ENTITY_CLOUD_SCOPE.REGION_OID,
                    EntityType.AVAILABILITY_ZONE, ENTITY_CLOUD_SCOPE.AVAILABILITY_ZONE_OID,
                    EntityType.SERVICE_PROVIDER, ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID
            );

    /**
     * New one creation.
     *
     * @param dsl JOOQ access.
     * @param clock Used for timestamp conversions before storing/reading DB values.
     * @param chunkSize Used for inserts, to enable batch insert.
     */
    public SqlEntitySavingsStore(@Nonnull final DSLContext dsl, @Nonnull final Clock clock,
            final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
        logger.info("Created new Entity Savings Store with chunk size {} and clock {}.",
                this.chunkSize, this.clock);
    }

    @Override
    public void addHourlyStats(@Nonnull Set<EntitySavingsStats> hourlyStats)
            throws EntitySavingsException {
        try {
            // Create insert statement.
            InsertReturningStep<EntitySavingsByHourRecord> insert = dsl
                    .insertInto(ENTITY_SAVINGS_BY_HOUR)
                    .set(ENTITY_SAVINGS_BY_HOUR.ENTITY_OID, 0L)
                    .set(ENTITY_SAVINGS_BY_HOUR.STATS_TIME, INIT_TIME)
                    .set(ENTITY_SAVINGS_BY_HOUR.STATS_TYPE, 1)
                    .set(ENTITY_SAVINGS_BY_HOUR.STATS_VALUE, 0d)
                    .onDuplicateKeyIgnore();

            // Put all records within a single transaction, irrespective of the chunk size.
            dsl.transaction(transaction -> {
                final DSLContext transactionContext = DSL.using(transaction);
                final BatchBindStep batch = transactionContext.batch(insert);

                // Add to batch and bind in chunks based on chunk size.
                Iterators.partition(hourlyStats.iterator(), chunkSize)
                        .forEachRemaining(chunk ->
                                chunk.forEach(stats ->
                                        batch.bind(stats.getEntityId(),
                                                SavingsUtil.getLocalDateTime(
                                                        stats.getTimestamp(), clock),
                                                stats.getType().getNumber(),
                                                stats.getValue())));
                if (batch.size() > 0) {
                    int[] insertCounts = batch.execute();
                    int totalInserted = IntStream.of(insertCounts).sum();
                    if (totalInserted < batch.size()) {
                        logger.warn("Hourly entity savings stats: Could only insert {} out of "
                                        + "batch size of {}. Total input stats count: {}. "
                                        + "Chunk size: {}", totalInserted, batch.size(),
                                hourlyStats.size(), chunkSize);
                    }
                }
            });
        } catch (Exception e) {
            throw new EntitySavingsException("Could not add " + hourlyStats.size()
                    + " hourly entity savings stats to DB.", e);
        }
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.HOURLY, statsTypes, startTime, endTime,
                entitiesByType);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getDailyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.DAILY, statsTypes, startTime, endTime,
                entitiesByType);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getMonthlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.MONTHLY, statsTypes, startTime, endTime,
                entitiesByType);
    }

    @Override
    @Nonnull
    public LastRollupTimes getLastRollupTimes() {
        final LastRollupTimes rollupTimes = new LastRollupTimes();
        try {
            final AggregationMetaDataRecord record = dsl.selectFrom(AGGREGATION_META_DATA)
                    .where(AGGREGATION_META_DATA.AGGREGATE_TABLE.eq(LastRollupTimes.getTableName()))
                    .fetchOne();
            if (record == null) {
                return rollupTimes;
            }
            if (record.getLastAggregated() != null) {
                rollupTimes.setLastTimeUpdated(record.getLastAggregated().getTime());
            }
            if (record.getLastAggregatedByHour() != null) {
                rollupTimes.setLastTimeByHour(record.getLastAggregatedByHour().getTime());
            }
            if (record.getLastAggregatedByDay() != null) {
                rollupTimes.setLastTimeByDay(record.getLastAggregatedByDay().getTime());
            }
            if (record.getLastAggregatedByMonth() != null) {
                rollupTimes.setLastTimeByMonth(record.getLastAggregatedByMonth().getTime());
            }
        } catch (Exception e) {
            logger.warn("Unable to fetch last rollup times from DB.", e);
        }
        return rollupTimes;
    }

    @Override
    public void setLastRollupTimes(@Nonnull final LastRollupTimes rollupTimes) {
        try {
            final AggregationMetaDataRecord record = new AggregationMetaDataRecord();
            record.setAggregateTable(LastRollupTimes.getTableName());
            record.setLastAggregated(new Timestamp(rollupTimes.getLastTimeUpdated()));
            if (rollupTimes.hasLastTimeByHour()) {
                record.setLastAggregatedByHour(new Timestamp(rollupTimes.getLastTimeByHour()));
            }
            if (rollupTimes.hasLastTimeByDay()) {
                record.setLastAggregatedByDay(new Timestamp(rollupTimes.getLastTimeByDay()));
            }
            if (rollupTimes.hasLastTimeByMonth()) {
                record.setLastAggregatedByMonth(new Timestamp(rollupTimes.getLastTimeByMonth()));
            }

            dsl.insertInto(AGGREGATION_META_DATA)
                    .set(record)
                    .onDuplicateKeyUpdate()
                    .set(record)
                    .execute();
        } catch (Exception e) {
            logger.warn("Unable to set last rollup times to DB: {}", rollupTimes, e);
        }
    }

    @Override
    public void performRollup(@Nonnull final RollupTimeInfo rollupInfo) {
        try {
            Routines.entitySavingsRollup(dsl.configuration(),
                    rollupInfo.isDaily() ? ENTITY_SAVINGS_BY_HOUR.getName()
                            : ENTITY_SAVINGS_BY_DAY.getName(),
                    rollupInfo.isDaily() ? ENTITY_SAVINGS_BY_DAY.getName()
                            : ENTITY_SAVINGS_BY_MONTH.getName(),
                    SavingsUtil.getLocalDateTime(rollupInfo.fromTime(), clock),
                    SavingsUtil.getLocalDateTime(rollupInfo.toTime(), clock));
            logger.trace("Completed rollup {}.", rollupInfo);
        } catch (Exception e) {
            logger.warn("Unable to perform rollup: {}.", rollupInfo, e);
        }
    }

    /**
     * Used to store info about stats tables (hourly/daily/monthly fields).
     */
    private static final class StatsTypeFields {
        Table<?> table;
        TableField<?, Long> oidField;
        TableField<?, LocalDateTime> timeField;
        TableField<?, Integer> typeField;
        TableField<?, Double> valueField;
    }

    private List<AggregatedSavingsStats> querySavingsStats(RollupDurationType durationType,
            @Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
            throws EntitySavingsException {
        if (statsTypes.isEmpty() || entitiesByType.isEmpty()) {
            throw new EntitySavingsException("Cannot get " + durationType.name()
                    + " entity savings stats: Type count: " + statsTypes.size()
                    + ", Entity type count: " + entitiesByType.size());
        }
        if (startTime > endTime) {
            throw new EntitySavingsException("Cannot get " + durationType.name()
                    + " entity savings stats: Start time: "
                    + SavingsUtil.getLocalDateTime(startTime, clock) + ", End time: "
                    + SavingsUtil.getLocalDateTime(endTime, clock));
        }
        try {
            final StatsTypeFields fieldInfo = statsFieldsByRollup.get(durationType);
            final Set<Integer> statsTypeCodes = statsTypes.stream()
                    .map(EntitySavingsStatsType::getNumber)
                    .collect(Collectors.toSet());
            final Result<Record3<LocalDateTime, Integer, BigDecimal>> records = dsl
                    .select(fieldInfo.timeField,
                            fieldInfo.typeField,
                            sum(fieldInfo.valueField).as(fieldInfo.valueField))
                    .from(fieldInfo.table)
                    .join(ENTITY_CLOUD_SCOPE)
                    .on(fieldInfo.oidField.eq(ENTITY_CLOUD_SCOPE.ENTITY_OID))
                    .where(getCondition(entitiesByType))
                    .and(fieldInfo.typeField.in(statsTypeCodes))
                    .and(fieldInfo.timeField
                            .ge(SavingsUtil.getLocalDateTime(startTime, clock))
                            .and(fieldInfo.timeField
                                    .lt(SavingsUtil.getLocalDateTime(endTime, clock))))
                    .groupBy(fieldInfo.timeField,
                            fieldInfo.typeField)
                    .orderBy(fieldInfo.timeField.asc())
                    .fetch();
            return records.map(this::convertStatsDbRecord);
        } catch (Exception e) {
            throw new EntitySavingsException("Could not get " + durationType.name()
                    + " entity savings stats for "
                    + entitiesByType.size() + " entity types from DB between "
                    + SavingsUtil.getLocalDateTime(startTime, clock)
                    + " and " + SavingsUtil.getLocalDateTime(endTime, clock), e);
        }
    }

    /**
     * Makes up Condition based on entityType to OID map.
     *
     * @param entitiesByType EntityType to OIDs.
     * @return Set of conditions.
     */
    private List<Condition> getCondition(@Nonnull MultiValuedMap<EntityType, Long> entitiesByType) {
        final List<Condition> conditions = new ArrayList<>();
        final Set<Long> rawEntityOids = new HashSet<>();
        entitiesByType.keySet().forEach(entityType -> {
            TableField<EntityCloudScopeRecord, Long> dbField = TYPE_TO_DB_MAP.get(entityType);
            if (dbField == null) {
                // If no higher scope type (like account/region) provided, then assume entity.
                rawEntityOids.addAll(entitiesByType.get(entityType));
            } else {
                conditions.add(getCondition(entitiesByType.get(entityType), dbField));
            }
        });
        conditions.add(getCondition(rawEntityOids, ENTITY_CLOUD_SCOPE.ENTITY_OID));
        return conditions;
    }

    /**
     * Makes up one condition based on OID inputs.
     *
     * @param oidValues Values of entity OIDs.
     * @param dbField DB column for condition.
     * @return Condition created.
     */
    @Nonnull
    private Condition getCondition(@Nonnull final Collection<Long> oidValues,
            @Nonnull final TableField<EntityCloudScopeRecord, Long> dbField) {
        if (oidValues.isEmpty()) {
            return trueCondition();
        }
        if (oidValues.size() == 1) {
            return dbField.eq(oidValues.iterator().next());
        }
        return dbField.in(oidValues);
    }

    /**
     * Gets stats DB record into Stats instance.
     *
     * @param rec DB record.
     * @return Stats instance will filled in values read from DB.
     */
    @Nonnull
    private AggregatedSavingsStats convertStatsDbRecord(
            @Nonnull final Record3<LocalDateTime, Integer, BigDecimal> rec) {
        return new AggregatedSavingsStats(TimeUtil.localDateTimeToMilli(rec.value1(), clock),
                EntitySavingsStatsType.forNumber(rec.value2()),
                rec.value3().doubleValue());
    }

    /**
     * Only for testing.
     *
     * @return DSL context.
     */
    @Nonnull
    @VisibleForTesting
    DSLContext getDsl() {
        return dsl;
    }

    /**
     * Gets the timestamp of the last stats record of ENTITY_SAVINGS_BY_HOUR table.
     *
     * @return the max stats time. Return null if table is empty.
     */
    @Nullable
    public Long getMaxStatsTime() {
        final Result<Record1<LocalDateTime>> records =
                dsl.select(max(ENTITY_SAVINGS_BY_HOUR.STATS_TIME))
                        .from(ENTITY_SAVINGS_BY_HOUR).fetch();
        if (records.size() > 0) {
            LocalDateTime statsTime = records.get(0).value1();
            if (statsTime != null) {
                return TimeUtil.localDateTimeToMilli(statsTime, clock);
            }
        }

        // If the table is empty, return null.
        return null;
    }
}
