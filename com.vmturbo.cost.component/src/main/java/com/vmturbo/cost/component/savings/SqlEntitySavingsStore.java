package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.AGGREGATION_META_DATA;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_MONTH;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.Routines;
import com.vmturbo.cost.component.db.tables.records.AggregationMetaDataRecord;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsByHourRecord;

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
    public List<AggregatedSavingsStats> getSavingsStats(final TimeFrame timeFrame,
            @Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids)
            throws EntitySavingsException {
        RollupDurationType durationType = RollupDurationType.HOURLY;
        switch (timeFrame) {
            case DAY:
                durationType = RollupDurationType.DAILY;
                break;
            case MONTH:
            case YEAR:
                durationType = RollupDurationType.MONTHLY;
                break;
        }
        return querySavingsStats(durationType, statsTypes, startTime, endTime, entityOids);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.HOURLY, statsTypes, startTime, endTime,
                entityOids);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getDailyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.DAILY, statsTypes, startTime, endTime,
                entityOids);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getMonthlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.MONTHLY, statsTypes, startTime, endTime,
                entityOids);
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
            @Nonnull Collection<Long> entityOids)
            throws EntitySavingsException {
        if (statsTypes.isEmpty() || entityOids.isEmpty()) {
            throw new EntitySavingsException("Cannot get " + durationType.name()
                    + " entity savings stats: Type count: " + statsTypes.size()
                    + ", Entity OID count: " + entityOids.size());
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
                    .where(fieldInfo.oidField.in(entityOids))
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
                    + entityOids.size() + " entity OIDs from DB between "
                    + SavingsUtil.getLocalDateTime(startTime, clock)
                    + " and " + SavingsUtil.getLocalDateTime(endTime, clock), e);
        }
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
}
