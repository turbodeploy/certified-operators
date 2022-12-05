package com.vmturbo.cost.component.savings.bottomup;

import static com.vmturbo.cost.component.db.Tables.BILLED_SAVINGS_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_CLOUD_SCOPE;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_MONTH;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.month;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
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
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.InsertValuesStepN;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Record5;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord.SavingsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.tables.EntitySavingsByHour;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsByHourRecord;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.SavingsStore;
import com.vmturbo.cost.component.savings.SavingsUtil;
import com.vmturbo.cost.component.savings.calculator.SavingsValues;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.jooq.JooqUtil;

/**
 * Implementation of store that accesses savings hourly/daily/monthly DB tables.
 */
public class SqlEntitySavingsStore implements EntitySavingsStore<DSLContext>, SavingsStore {
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
     * Whether user has enabled feature flag to view billed savings stats or not.
     */
    private final boolean readBilledSavings;

    /**
     * Entity types that is a logical grouping of cloud workloads.
     * The entity_cloud_scope table allows resolving workload OIDs using OIDs of these scopes.
     */
    private static final Set<Integer> CLOUD_GROUP_SCOPES = ImmutableSet.of(
            EntityType.BUSINESS_ACCOUNT_VALUE,
            EntityType.REGION_VALUE,
            EntityType.AVAILABILITY_ZONE_VALUE,
            EntityType.SERVICE_PROVIDER_VALUE);

    /**
     * Map entity type to the column in the entity_cloud_scope table.
     */
    private static final Map<Integer, TableField<?, Long>> SCOPE_TYPE_TO_TABLE_FIELD_MAP =
            new HashMap<>();

    static {
        SCOPE_TYPE_TO_TABLE_FIELD_MAP.put(EntityType.BUSINESS_ACCOUNT_VALUE, ENTITY_CLOUD_SCOPE.ACCOUNT_OID);
        SCOPE_TYPE_TO_TABLE_FIELD_MAP.put(EntityType.REGION_VALUE, ENTITY_CLOUD_SCOPE.REGION_OID);
        SCOPE_TYPE_TO_TABLE_FIELD_MAP.put(EntityType.AVAILABILITY_ZONE_VALUE, ENTITY_CLOUD_SCOPE.AVAILABILITY_ZONE_OID);
        SCOPE_TYPE_TO_TABLE_FIELD_MAP.put(EntityType.SERVICE_PROVIDER_VALUE, ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID);
    }

    /**
     * Stats table field info by rollup type.
     */
    private static final Map<RollupDurationType, StatsTypeFields> statsFieldsByRollup =
            new HashMap<>();

    /**
     * New stats table for write billed savings daily stats, similar to savings stats by day table.
     */
    private static final StatsTypeFields billedStatsDayFields = new StatsTypeFields();

    static {
        StatsTypeFields hourFields = new StatsTypeFields();
        hourFields.table = ENTITY_SAVINGS_BY_HOUR;
        hourFields.oidField = ENTITY_SAVINGS_BY_HOUR.ENTITY_OID;
        hourFields.timeField = ENTITY_SAVINGS_BY_HOUR.STATS_TIME;
        hourFields.typeField = ENTITY_SAVINGS_BY_HOUR.STATS_TYPE;
        hourFields.valueField = ENTITY_SAVINGS_BY_HOUR.STATS_VALUE;
        hourFields.samplesField = null;
        statsFieldsByRollup.put(RollupDurationType.HOURLY, hourFields);

        StatsTypeFields dayFields = new StatsTypeFields();
        dayFields.table = ENTITY_SAVINGS_BY_DAY;
        dayFields.oidField = ENTITY_SAVINGS_BY_DAY.ENTITY_OID;
        dayFields.timeField = ENTITY_SAVINGS_BY_DAY.STATS_TIME;
        dayFields.typeField = ENTITY_SAVINGS_BY_DAY.STATS_TYPE;
        dayFields.valueField = ENTITY_SAVINGS_BY_DAY.STATS_VALUE;
        dayFields.samplesField = ENTITY_SAVINGS_BY_DAY.SAMPLES;
        statsFieldsByRollup.put(RollupDurationType.DAILY, dayFields);

        StatsTypeFields monthFields = new StatsTypeFields();
        monthFields.table = ENTITY_SAVINGS_BY_MONTH;
        monthFields.oidField = ENTITY_SAVINGS_BY_MONTH.ENTITY_OID;
        monthFields.timeField = ENTITY_SAVINGS_BY_MONTH.STATS_TIME;
        monthFields.typeField = ENTITY_SAVINGS_BY_MONTH.STATS_TYPE;
        monthFields.valueField = ENTITY_SAVINGS_BY_MONTH.STATS_VALUE;
        monthFields.samplesField = ENTITY_SAVINGS_BY_MONTH.SAMPLES;
        statsFieldsByRollup.put(RollupDurationType.MONTHLY, monthFields);

        billedStatsDayFields.table = BILLED_SAVINGS_BY_DAY;
        billedStatsDayFields.oidField = BILLED_SAVINGS_BY_DAY.ENTITY_OID;
        billedStatsDayFields.timeField = BILLED_SAVINGS_BY_DAY.STATS_TIME;
        billedStatsDayFields.typeField = BILLED_SAVINGS_BY_DAY.STATS_TYPE;
        billedStatsDayFields.valueField = BILLED_SAVINGS_BY_DAY.STATS_VALUE;
        billedStatsDayFields.samplesField = BILLED_SAVINGS_BY_DAY.SAMPLES;
    }

    /**
     * New one creation.
     *
     * @param dsl JOOQ access.
     * @param clock Used for timestamp conversions before storing/reading DB values.
     * @param chunkSize Used for inserts, to enable batch insert.
     * @param readBilledSavings Whether stats need to be read from billed savings DB table.
     */
    public SqlEntitySavingsStore(@Nonnull final DSLContext dsl, @Nonnull final Clock clock,
            final int chunkSize, boolean readBilledSavings) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
        this.readBilledSavings = readBilledSavings;
        logger.info("Created new Savings Store (read billed stats? {}) with chunk "
                + "size {} and clock {}.", this.readBilledSavings, this.chunkSize, this.clock);
    }

    @Override
    public void addHourlyStats(@Nonnull Set<EntitySavingsStats> hourlyStats, DSLContext dsl)
            throws EntitySavingsException {
        addStats(RollupDurationType.HOURLY, hourlyStats, dsl, false);
    }

    @Override
    public void addDailyStats(@Nonnull Set<EntitySavingsStats> dailyStats, DSLContext dsl,
            boolean isBillBasedSavings)
            throws EntitySavingsException {
        addStats(RollupDurationType.DAILY, dailyStats, dsl, isBillBasedSavings);
    }

    /**
     * Adds stats to hourly or daily stats tables. Rollup if any needs to be done separately.
     *
     * @param tableType Table type (hourly/daily) to add stats to.
     * @param savingsStats Stats to insert.
     * @param dsl DB handle.
     * @param isBillBasedSavings Whether to add to the bill-based stats table.
     * @throws EntitySavingsException Thrown on insert error.
     */
    private void addStats(RollupDurationType tableType,
            @Nonnull Set<EntitySavingsStats> savingsStats, DSLContext dsl, boolean isBillBasedSavings)
            throws EntitySavingsException {
        // Bill based stats are only written to daily table currently.
        final StatsTypeFields fieldTypes = isBillBasedSavings ? billedStatsDayFields
                : statsFieldsByRollup.get(tableType);
        try {
            boolean isDaily = tableType == RollupDurationType.DAILY;
            final List<TableField<?, ?>> commonFields = ImmutableList.of(
                    fieldTypes.oidField,
                    fieldTypes.timeField,
                    fieldTypes.typeField);
            final List<TableField<?, ?>> allFields = new ArrayList<>(commonFields);
            allFields.add(fieldTypes.valueField);
            if (isDaily) {
                allFields.add(fieldTypes.samplesField);
            }
            // Add to batch and bind in chunks based on chunk size.
            Iterators.partition(savingsStats.iterator(), chunkSize)
                    .forEachRemaining(chunk -> {
                        final InsertValuesStepN<?> insert = dsl
                                .insertInto(fieldTypes.table)
                                .columns(allFields);
                        chunk.forEach(stats -> {
                            // If daily, we insert with 12:00 AM day time always.
                            final LocalDateTime statsTime = isDaily
                                    ? SavingsUtil.getDayStartTime(stats.getTimestamp(), clock)
                                    : SavingsUtil.getLocalDateTime(stats.getTimestamp(), clock);
                            final List<Object> baseValues = ImmutableList.of(stats.getEntityId(),
                                    statsTime, stats.getType().getNumber(), stats.getValue());
                            final List<Object> values = new ArrayList<>(baseValues);
                            if (isDaily) {
                                // Set the 'samples' field value.
                                // for daily table that we are directly inserting to, we don't really
                                // know for how many hours this is for. So we assume 24 hours.
                                values.add(24);
                            }

                            // If dup, update the stats value.
                            insert.values(values)
                                    .onDuplicateKeyUpdate()
                                    .set(fieldTypes.valueField,
                                            JooqUtil.upsertValue(fieldTypes.valueField, dsl.dialect()));
                        });
                        insert.execute();
                    });
        } catch (Exception e) {
            throw new EntitySavingsException("Could not add " + savingsStats.size()
                    + (isBillBasedSavings ? " bill-based" : " bottom-up") + " savings "
                    + tableType + " stats to DB.", e);
        }
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getSavingsStats(final TimeFrame timeFrame,
            @Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
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
        return querySavingsStats(durationType, statsTypes, startTime, endTime, entityOids,
                entityTypes, resourceGroups);
    }

    @Override
    @Nonnull
    public Stream<EntitySavingsStatsRecord> getSavingsStats(long startTime, long endTime)
            throws EntitySavingsException {
        final LocalDateTime startDate = SavingsUtil.getLocalDateTime(startTime, clock);
        final LocalDateTime endDate = SavingsUtil.getLocalDateTime(endTime, clock);
        if (startTime > endTime) {
            throw new EntitySavingsException("Cannot get raw entity savings stats: "
                    + "Start time: " + startDate + " > End time: " + endDate);
        }
        return dsl.selectFrom(EntitySavingsByHour.ENTITY_SAVINGS_BY_HOUR)
                .where(ENTITY_SAVINGS_BY_HOUR.STATS_TIME.ge(startDate)
                        .and(ENTITY_SAVINGS_BY_HOUR.STATS_TIME.lt(endDate)))
                .stream()
                .map(this::convertStatsDbRecord)
                .filter(Objects::nonNull);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getHourlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.HOURLY, statsTypes, startTime, endTime,
                entityOids, entityTypes, resourceGroups);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getDailyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.DAILY, statsTypes, startTime, endTime,
                entityOids, entityTypes, resourceGroups);
    }

    @Nonnull
    @Override
    public List<AggregatedSavingsStats> getMonthlyStats(@Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids,
            @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException {
        return querySavingsStats(RollupDurationType.MONTHLY, statsTypes, startTime, endTime,
                entityOids, entityTypes, resourceGroups);
    }

    @Override
    public int deleteOlderThanHourly(long timestamp) {
        return deleteOlderThan(timestamp, ENTITY_SAVINGS_BY_HOUR,
                ENTITY_SAVINGS_BY_HOUR.STATS_TIME);
    }

    @Override
    public int deleteOlderThanDaily(long timestamp, boolean isBillBasedSavings) {
        if (isBillBasedSavings) {
            return deleteOlderThan(timestamp, billedStatsDayFields.table,
                    billedStatsDayFields.timeField);
        }
        return deleteOlderThan(timestamp, ENTITY_SAVINGS_BY_DAY,
                ENTITY_SAVINGS_BY_DAY.STATS_TIME);
    }

    @Override
    public int deleteOlderThanMonthly(long timestamp) {
        return deleteOlderThan(timestamp, ENTITY_SAVINGS_BY_MONTH,
                ENTITY_SAVINGS_BY_MONTH.STATS_TIME);
    }

    /**
     * Util method to delete old entries from stats tables.
     *
     * @param timestamp Timestamp stats older than which will get cleaned up.
     * @param table Table ref.
     * @param field Field ref.
     * @return Count of deleted rows.
     */
    private int deleteOlderThan(long timestamp, Table<?> table, TableField<?, LocalDateTime> field) {
        final LocalDateTime minDate = SavingsUtil.getLocalDateTime(timestamp, clock);
        return dsl.deleteFrom(table)
                .where(field.lt(minDate))
                .execute();
    }

    /**
     * Delete all stats from the hourly, daily, and monthly tables.
     *
     * @param uuids list of UUIDs for which to delete stats.
     */
    @Override
    public void deleteStatsForUuids(@Nonnull Set<Long> uuids) {
        dsl.deleteFrom(ENTITY_SAVINGS_BY_HOUR)
                .where(ENTITY_SAVINGS_BY_HOUR.ENTITY_OID.in(uuids))
                .execute();
        dsl.deleteFrom(ENTITY_SAVINGS_BY_DAY)
                .where(ENTITY_SAVINGS_BY_DAY.ENTITY_OID.in(uuids))
                .execute();
        dsl.deleteFrom(ENTITY_SAVINGS_BY_MONTH)
                .where(ENTITY_SAVINGS_BY_MONTH.ENTITY_OID.in(uuids))
                .execute();
        dsl.deleteFrom(BILLED_SAVINGS_BY_DAY)
                .where(BILLED_SAVINGS_BY_DAY.ENTITY_OID.in(uuids))
                .execute();
    }

    @Override
    public void performRollup(@Nonnull final RollupDurationType durationType,
            final long toTime, @Nonnull final List<Long> fromTimes) {
        final LocalDateTime toDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(toTime), clock.getZone());
        final List<LocalDateTime> fromDateTimes = fromTimes.stream()
                .map(millis -> LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), clock.getZone()))
                .collect(Collectors.toList());
        getRollupUpsert(durationType, toDateTime, fromDateTimes).execute();
    }

    /**
     * Create an UPSERT statement to perform roll-ups of for given rollup type (hourly -> daily or
     * daily -> monthly).
     *
     * @param rollupDuration duration of rollup - DAILY or MONTHLY
     * @param toDateTime     the timestamp for rolled-up data in the rollup table
     * @param fromDateTimes  timestamps for records rolled up from the source table
     * @return jOOQ UPSERT statement
     */
    private InsertOnDuplicateSetMoreStep<?> getRollupUpsert(@Nonnull final RollupDurationType rollupDuration,
            @Nonnull final LocalDateTime toDateTime, @Nonnull final List<LocalDateTime> fromDateTimes) {
        final Table<?> sourceTable = rollupDuration == RollupDurationType.DAILY
                ? ENTITY_SAVINGS_BY_HOUR
                : ENTITY_SAVINGS_BY_DAY;
        final Table<?> rollupTable = rollupDuration == RollupDurationType.DAILY
                ? ENTITY_SAVINGS_BY_DAY
                : ENTITY_SAVINGS_BY_MONTH;
        final RollupDurationType sourceDuration = rollupDuration == RollupDurationType.DAILY
                ? RollupDurationType.HOURLY
                : RollupDurationType.DAILY;
        final StatsTypeFields sourceFields = statsFieldsByRollup.get(sourceDuration);
        final StatsTypeFields rollupFields = statsFieldsByRollup.get(rollupDuration);
        final SelectHavingStep<Record5<LocalDateTime, Long, Integer, Double, Integer>> embeddedSelect =
                DSL.select(DSL.val(toDateTime).as("stats_time"), sourceFields.oidField,
                                sourceFields.typeField,
                                DSL.sum(sourceFields.valueField).coerce(Double.class),
                                DSL.count())
                        .from(sourceTable)
                        .where(sourceFields.timeField.in(fromDateTimes))
                        .groupBy(sourceFields.oidField, sourceFields.typeField);
        return dsl.insertInto(rollupTable)
                .columns(rollupFields.timeField, rollupFields.oidField, rollupFields.typeField,
                        rollupFields.valueField, rollupFields.samplesField)
                .select(embeddedSelect)
                .onDuplicateKeyUpdate()
                .set(rollupFields.valueField, rollupFields.valueField.plus(
                        JooqUtil.upsertValue(rollupFields.valueField, dsl.dialect())))
                .set(rollupFields.samplesField, rollupFields.samplesField.plus(
                        JooqUtil.upsertValue(rollupFields.samplesField, dsl.dialect())));
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
        TableField<?, Integer> samplesField;
    }

    @Nonnull
    private List<AggregatedSavingsStats> querySavingsStats(RollupDurationType durationType,
            @Nonnull Set<EntitySavingsStatsType> statsTypes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids, @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups)
            throws EntitySavingsException {
        if (statsTypes.isEmpty() || (entityOids.isEmpty() && resourceGroups.isEmpty())) {
            //There are no entities or filters that support savings stats, returning an empty list
            return new ArrayList<>();
        }
        if (startTime > endTime) {
            throw new EntitySavingsException("Cannot get " + durationType.name()
                    + " entity savings stats: Start time: "
                    + SavingsUtil.getLocalDateTime(startTime, clock) + ", End time: "
                    + SavingsUtil.getLocalDateTime(endTime, clock));
        }
        try {
            final StatsTypeFields fieldInfo = readBilledSavings ? billedStatsDayFields
                    : statsFieldsByRollup.get(durationType);
            final Set<Integer> statsTypeCodes = statsTypes.stream()
                    .map(EntitySavingsStatsType::getNumber)
                    .collect(Collectors.toSet());
            // Use Daily Table for Yearly/Monthly stats until OM-87933 is addressed
            if (durationType.equals(RollupDurationType.MONTHLY) && readBilledSavings) {
                return aggregateDailyBillBasedStats(statsTypeCodes, startTime, endTime,
                        entityOids, entityTypes, resourceGroups);
            }
            // Check if entity type is one of those that can be used to resolve for members using the
            // entity_cloud_scope table. Checking for only one entity type in the type list because
            // we expect all entities in the list have the same type. e.g. we cannot have VMs and accounts
            // in the list.
            boolean isCloudScopeEntity = entityTypes.size() == 1 && CLOUD_GROUP_SCOPES.containsAll(entityTypes);
            boolean isResourceGroups = !resourceGroups.isEmpty();

            SelectJoinStep<Record3<LocalDateTime, Integer, BigDecimal>> selectStatsStatement =
                    dsl.select(fieldInfo.timeField,
                                    fieldInfo.typeField,
                                    DSL.sum(fieldInfo.valueField).as(fieldInfo.valueField))
                    .from(fieldInfo.table);

            if (isCloudScopeEntity || isResourceGroups) {
                selectStatsStatement = selectStatsStatement.join(ENTITY_CLOUD_SCOPE)
                        .on(fieldInfo.oidField.eq(ENTITY_CLOUD_SCOPE.ENTITY_OID));
            }

            final Result<Record3<LocalDateTime, Integer, BigDecimal>> records = selectStatsStatement
                    .where(generateEntityOidCondition(fieldInfo, entityOids, entityTypes,
                            resourceGroups, isCloudScopeEntity, isResourceGroups))
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
                    + (readBilledSavings ? " billed" : " entity") + " savings stats for "
                    + entityOids.size() + " entity OIDs from DB between "
                    + SavingsUtil.getLocalDateTime(startTime, clock)
                    + " and " + SavingsUtil.getLocalDateTime(endTime, clock), e);
        }
    }

    /**
     * For BillBasedSavings, aggregating the daily table to monthly.
     */
    @Nonnull
    private List<AggregatedSavingsStats> aggregateDailyBillBasedStats(@Nonnull Set<Integer> statsTypeCodes,
            @Nonnull Long startTime, @Nonnull Long endTime,
            @Nonnull Collection<Long> entityOids, @Nonnull Collection<Integer> entityTypes,
            @Nonnull Collection<Long> resourceGroups) {
        final StatsTypeFields fieldInfo = billedStatsDayFields;
        boolean isCloudScopeEntity = entityTypes.size() == 1 && CLOUD_GROUP_SCOPES.containsAll(entityTypes);
        boolean isResourceGroups = !resourceGroups.isEmpty();

        SelectJoinStep<Record3<LocalDateTime, Integer, BigDecimal>> selectStatsStatement =
                dsl.select(lastDayOfMonth(fieldInfo.timeField),
                                fieldInfo.typeField,
                                DSL.sum(fieldInfo.valueField).as(fieldInfo.valueField))
                        .from(fieldInfo.table);

        if (isCloudScopeEntity || isResourceGroups) {
            selectStatsStatement = selectStatsStatement.join(ENTITY_CLOUD_SCOPE)
                    .on(fieldInfo.oidField.eq(ENTITY_CLOUD_SCOPE.ENTITY_OID));
        }
        final Result<Record3<LocalDateTime, Integer, BigDecimal>> records =
                selectStatsStatement
                        .where(generateEntityOidCondition(fieldInfo, entityOids, entityTypes,
                                resourceGroups, isCloudScopeEntity, isResourceGroups))
                        .and(fieldInfo.typeField.in(statsTypeCodes))
                        .and(fieldInfo.timeField
                                .ge(SavingsUtil.getLocalDateTime(startTime, clock))
                                .and(fieldInfo.timeField
                                        .lt(SavingsUtil.getLocalDateTime(endTime, clock))))
                        .groupBy(fieldInfo.typeField, groupByMonth(fieldInfo.timeField))
                        .orderBy(groupByMonth(fieldInfo.timeField).asc()).fetch();
        return records.map(this::convertStatsDbRecord);
    }

    //// get the last day of the month (only for bill based)
    private Field<LocalDateTime> lastDayOfMonth(Field<LocalDateTime> field) {
        return dsl.dialect() == SQLDialect.POSTGRES ? field(
                "date_trunc('month', {0}) + '1 month - 1 day'::interval", field.getType(), field)
                : field("last_day({0})", field.getType(), field);
    }

    // group the records by month (only for bill based)
    private Field<?> groupByMonth(Field<?> field) {
        return dsl.dialect() == SQLDialect.POSTGRES ? field(
                "date_trunc('month', {0})", Integer.class, field)
                : month(field);
    }

    @Nonnull
    private Condition generateEntityOidCondition(@Nonnull StatsTypeFields fieldInfo,
                                                 @Nonnull Collection<Long> entityOids,
                                                 @Nonnull Collection<Integer> entityTypes,
                                                 @Nonnull Collection<Long> resourceGroups,
                                                 boolean isCloudScopeEntity,
                                                 boolean isResourceGroups) {
        if (isCloudScopeEntity) {
            Integer entityType = entityTypes.iterator().next();
            TableField<?, Long> scopeColumn = SCOPE_TYPE_TO_TABLE_FIELD_MAP.get(entityType);
            if (scopeColumn != null) {
                return scopeColumn.in(entityOids);
            }
        }
        if (isResourceGroups) {
            return ENTITY_CLOUD_SCOPE.RESOURCE_GROUP_OID.in(resourceGroups);
        }
        return fieldInfo.oidField.in(entityOids);
    }

    /**
     * Gets stats DB record into Stats instance.
     *
     * @param rec DB record.
     * @return Stats instance with values obtained from DB.
     */
    @Nonnull
    private AggregatedSavingsStats convertStatsDbRecord(
            @Nonnull final Record3<LocalDateTime, Integer, BigDecimal> rec) {
        return new AggregatedSavingsStats(TimeUtil.localDateTimeToMilli(rec.value1(), clock),
                EntitySavingsStatsType.forNumber(rec.value2()),
                rec.value3().doubleValue());
    }

    /**
     * Converts DB record to protobuf savings stats record.
     *
     * @param dbRecord DB record read from savings hourly table.
     * @return Stats record or null if there is a mapping issue.
     */
    @Nullable
    private EntitySavingsStatsRecord convertStatsDbRecord(@Nonnull final EntitySavingsByHourRecord dbRecord) {
        final EntitySavingsStatsRecord.Builder builder = EntitySavingsStatsRecord.newBuilder()
                .setEntityOid(dbRecord.getEntityOid())
                .setSnapshotDate(TimeUtil.localDateTimeToMilli(dbRecord.getStatsTime(), clock));
        final EntitySavingsStatsType statsType = EntitySavingsStatsType.forNumber(
                dbRecord.getStatsType());
        if (statsType == null) {
            return null;
        }
        builder.addStatRecords(SavingsRecord.newBuilder()
                .setName(statsType.name())
                .setValue(dbRecord.getStatsValue().floatValue()));
        return builder.build();
    }

    @Override
    public Set<Long> writeDailyStats(@Nonnull final List<SavingsValues> values)
            throws EntitySavingsException {
        final Set<EntitySavingsStats> dailyStats = new HashSet<>();
        final Set<Long> uniqueDailyTimestamps = new HashSet<>();
        values.forEach(value -> {
            long timestamp = TimeUtil.localDateTimeToMilli(value.getTimestamp(), clock);
            uniqueDailyTimestamps.add(timestamp);

            dailyStats.add(new EntitySavingsStats(value.getEntityOid(),
                    timestamp, EntitySavingsStatsType.REALIZED_SAVINGS, value.getSavings()));

            dailyStats.add(new EntitySavingsStats(value.getEntityOid(),
                    timestamp, EntitySavingsStatsType.REALIZED_INVESTMENTS, value.getInvestments()));
        });
        // Add to the billed savings stats table.
        addDailyStats(dailyStats, this.dsl, true);
        return uniqueDailyTimestamps;
    }

    @Override
    public Set<Long> getEntitiesWithoutScopeRecords(final Set<Long> entityOids) {
        List<Record1<Long>> scopeRecords =
                dsl.select(ENTITY_CLOUD_SCOPE.ENTITY_OID)
                        .from(ENTITY_CLOUD_SCOPE)
                        .where(ENTITY_CLOUD_SCOPE.ENTITY_OID.in(entityOids))
                        .fetch();
        Set<Long> entitiesWithScope = scopeRecords.stream().map(Record1::value1).collect(Collectors.toSet());
        Set<Long> entitiesWithoutScope = new HashSet<>(entityOids);
        entitiesWithoutScope.removeAll(entitiesWithScope);
        return entitiesWithoutScope;
    }

    @Override
    public void deleteStats(@Nonnull Set<Long> uuids) {
        deleteStatsForUuids(uuids);
    }
}
