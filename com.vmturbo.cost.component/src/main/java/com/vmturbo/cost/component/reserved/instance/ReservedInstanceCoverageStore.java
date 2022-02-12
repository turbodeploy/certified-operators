package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
import static com.vmturbo.cost.component.db.tables.ReservedInstanceCoverageLatest.RESERVED_INSTANCE_COVERAGE_LATEST;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.createSelectFieldsForRIUtilizationCoverage;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.ReservedInstanceCoverageLatest;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageByDayRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageByHourRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageByMonthRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.sql.utils.jooq.UpsertBuilder;

/**
 * This class is used to store entity reserved instance coupons coverage information into database.
 * And it used the data which comes from real time topology which contains the total coupons for
 * each entity, and also used the {@link EntityReservedInstanceMappingStore} which has the latest
 * used coupons for each entity. And it will combine these data and store into database.
 */
public class ReservedInstanceCoverageStore implements MultiStoreDiagnosable {

    private static final Logger logger = LogManager.getLogger();

    //TODO: set this chunk config through consul.
    private final static int chunkSize = 1000;

    private final DSLContext dsl;

    private final ReservedInstancesCoverageByHourDiagsHelper reservedInstancesCoverageByHourDiagsHelper;

    private final ReservedInstancesCoverageByDayDiagsHelper reservedInstancesCoverageByDayDiagsHelper;

    private final ReservedInstancesCoverageByMonthDiagsHelper reservedInstancesCoverageByMonthDiagsHelper;

    private final LatestReservedInstanceCoverageDiagsHelper latestReservedInstanceCoverageDiagsHelper;

    private final boolean normalizeRollupSamples;

    private final float sampleNormalizationFactor;

    public ReservedInstanceCoverageStore(@Nonnull final DSLContext dsl,
                                         boolean normalizeRollupSamples,
                                         float sampleNormalizationFactor) {
        this.dsl = dsl;
        this.normalizeRollupSamples = normalizeRollupSamples;
        this.sampleNormalizationFactor = sampleNormalizationFactor;
        this.reservedInstancesCoverageByHourDiagsHelper = new ReservedInstancesCoverageByHourDiagsHelper(dsl);
        this.reservedInstancesCoverageByDayDiagsHelper = new ReservedInstancesCoverageByDayDiagsHelper(dsl);
        this.reservedInstancesCoverageByMonthDiagsHelper = new ReservedInstancesCoverageByMonthDiagsHelper(dsl);
        this.latestReservedInstanceCoverageDiagsHelper = new LatestReservedInstanceCoverageDiagsHelper(dsl);
    }

    /**
     * Given a list of {@link ServiceEntityReservedInstanceCoverageRecord} and current entity reserved
     * instance mapping information, combine them together and store into database.
     *
     * @param context {@link DSLContext} transactional context.
     * @param entityRiCoverages a list {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    public void updateReservedInstanceCoverageStore(
            @Nonnull final DSLContext context,
            @Nonnull Instant topologyCreationTime,
            @Nonnull final List<ServiceEntityReservedInstanceCoverageRecord> entityRiCoverages) {
        final LocalDateTime currentTime = topologyCreationTime.atOffset(ZoneOffset.UTC).toLocalDateTime();
        List<ReservedInstanceCoverageLatestRecord> riCoverageRecords = entityRiCoverages.stream()
                .map(entityRiCoverage -> createReservedInstanceCoverageRecord(context, currentTime,
                        entityRiCoverage))
                .collect(Collectors.toList());
        Lists.partition(riCoverageRecords, chunkSize).forEach(entityChunk ->
                context.batchInsert(entityChunk).execute());
    }

    /**
     * Get the list of {@link ReservedInstanceStatsRecord} which aggregates data from reserved instance
     * coverage table.
     *
     * @param filter a {@link ReservedInstanceCoverageFilter}.
     * @return a list of {@link ReservedInstanceStatsRecord}.
     */
    public List<ReservedInstanceStatsRecord> getReservedInstanceCoverageStatsRecords(
            @Nonnull final ReservedInstanceCoverageFilter filter) {
        final Table<?> table = filter.getTableName();
        final Result<Record> records = dsl.select(createSelectFieldsForRIUtilizationCoverage(table, normalizeRollupSamples))
                .from(table)
                .where(filter.generateConditions(dsl))
                .groupBy(table.field(SNAPSHOT_TIME))
                .fetch();
        return records.stream()
                .map(r -> ReservedInstanceUtil.convertRIUtilizationCoverageRecordToRIStatsRecord(r, sampleNormalizationFactor, table))
                .collect(Collectors.toList());
    }

    /**
     * Create {@link ReservedInstanceCoverageLatestRecord}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param currentTime the current time.
     * @param entityRiCoverage {@link ServiceEntityReservedInstanceCoverageRecord}.
     * @return {@link ReservedInstanceCoverageLatestRecord}.
     */
    private ReservedInstanceCoverageLatestRecord createReservedInstanceCoverageRecord(
            @Nonnull final DSLContext context,
            @Nonnull final LocalDateTime currentTime,
            @Nonnull final ServiceEntityReservedInstanceCoverageRecord entityRiCoverage) {

        String hourKey = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? ReservedInstanceUtil.createHourKey(currentTime, entityRiCoverage.getId(),
                entityRiCoverage.getRegionId(), entityRiCoverage.getAvailabilityZoneId(),
                entityRiCoverage.getBusinessAccountId()) : null;

        String dayKey = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? ReservedInstanceUtil.createDayKey(currentTime, entityRiCoverage.getId(),
                entityRiCoverage.getRegionId(), entityRiCoverage.getAvailabilityZoneId(),
                entityRiCoverage.getBusinessAccountId()) : null;

        String monthKey = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? ReservedInstanceUtil.createMonthKey(currentTime, entityRiCoverage.getId(),
                entityRiCoverage.getRegionId(), entityRiCoverage.getAvailabilityZoneId(),
                entityRiCoverage.getBusinessAccountId()) : null;

        return context.newRecord(Tables.RESERVED_INSTANCE_COVERAGE_LATEST,
                new ReservedInstanceCoverageLatestRecord(currentTime, entityRiCoverage.getId(),
                        entityRiCoverage.getRegionId(), entityRiCoverage.getAvailabilityZoneId(),
                        entityRiCoverage.getBusinessAccountId(), entityRiCoverage.getTotalCoupons(),
                        entityRiCoverage.getUsedCoupons(), hourKey, dayKey, monthKey));

    }

    @Nonnull
    public Map<Long, Double> getEntitiesCouponCapacity(ReservedInstanceCoverageFilter filter) {
        Map<Long, Double> entitiesCouponCapacity = new HashMap<>();
        dsl.select(RESERVED_INSTANCE_COVERAGE_LATEST.ENTITY_ID, RESERVED_INSTANCE_COVERAGE_LATEST.TOTAL_COUPONS)
                .from(Tables.RESERVED_INSTANCE_COVERAGE_LATEST)
                .where((filter.generateConditions(dsl))).and(RESERVED_INSTANCE_COVERAGE_LATEST.SNAPSHOT_TIME.eq(
                        dsl.select(RESERVED_INSTANCE_COVERAGE_LATEST.SNAPSHOT_TIME.max())
                                .from(Tables.RESERVED_INSTANCE_COVERAGE_LATEST)))
                .fetch()
                .forEach(record -> entitiesCouponCapacity.put(record.value1(), record.value2()));
        return entitiesCouponCapacity;
    }

    @Override
    public Set<Diagnosable> getDiagnosables(final boolean collectHistoricalStats) {
        HashSet<Diagnosable> storesToSave = new HashSet<>();
        storesToSave.add(latestReservedInstanceCoverageDiagsHelper);
        if (collectHistoricalStats) {
            storesToSave.add(reservedInstancesCoverageByDayDiagsHelper);
            storesToSave.add(reservedInstancesCoverageByMonthDiagsHelper);
            storesToSave.add(reservedInstancesCoverageByHourDiagsHelper);
        }
        return storesToSave;
    }

    /**
     * Perform a rollup.
     * @param durationType the duration type of the rollup
     * @param fromTimes the time of the rollup
     */
    public void performRollup(@Nonnull RollupDurationType durationType,
            @Nonnull List<LocalDateTime> fromTimes) {
        fromTimes.forEach(fromTime ->
                getRollupUpsert(durationType, fromTime));
    }

    private void getRollupUpsert(RollupDurationType rollupDuration, LocalDateTime fromDateTimes) {
        final ReservedInstanceCoverageLatest source = Tables.RESERVED_INSTANCE_COVERAGE_LATEST;
        Table<?> targetTable = null;
        LocalDateTime toDateTime = null;
        Field<?> rollupKey = null;
        switch (rollupDuration) {
            case HOURLY: {
                targetTable = Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
                toDateTime = fromDateTimes.truncatedTo(ChronoUnit.HOURS);
                rollupKey = Tables.RESERVED_INSTANCE_COVERAGE_LATEST.HOUR_KEY;
                break;
            }
            case DAILY: {
                targetTable = RESERVED_INSTANCE_COVERAGE_BY_DAY;
                toDateTime = fromDateTimes.truncatedTo(ChronoUnit.DAYS);
                rollupKey = Tables.RESERVED_INSTANCE_COVERAGE_LATEST.DAY_KEY;
                break;
            }
            case MONTHLY: {
                targetTable = Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
                YearMonth month = YearMonth.from(fromDateTimes);
                toDateTime = month.atEndOfMonth().atStartOfDay();
                rollupKey = Tables.RESERVED_INSTANCE_COVERAGE_LATEST.MONTH_KEY;
                break;
            }
        }
        final StatsTypeFields rollupFields = statsFieldsByRollup.get(rollupDuration);
        final UpsertBuilder upsert = new UpsertBuilder()
                .withTargetTable(targetTable)
                .withInsertFields(
                        rollupFields.samples,
                        rollupFields.snapshotTime,
                        rollupFields.entityId,
                        rollupFields.regionId,
                        rollupFields.availabilityZoneId,
                        rollupFields.businessAccountId,
                        rollupFields.totalCoupons,
                        rollupFields.usedCoupons,
                        rollupFields.rollupKey
                );

        upsert
                .withTargetTable(targetTable)
                .withSourceTable(source)
                .withInsertValue(
                        rollupFields.samples, DSL.count().as("samples")
                )
                .withInsertValue(
                        rollupFields.snapshotTime, DSL.val(toDateTime)
                )
                .withInsertValue(rollupFields.entityId, DSL.max(source.REGION_ID).as("entity_id"))
                .withInsertValue(rollupFields.regionId, DSL.max(source.REGION_ID).as("region_id"))
                .withInsertValue(rollupFields.availabilityZoneId,
                        DSL.max(source.AVAILABILITY_ZONE_ID).as("availability_zone_id"))
                .withInsertValue(rollupFields.businessAccountId,
                        DSL.max(source.BUSINESS_ACCOUNT_ID).as("business_account_id"))
                .withInsertValue(rollupFields.totalCoupons, DSL.avg(source.TOTAL_COUPONS).cast(Double.class).as(
                        "total_coupons"))
                .withInsertValue(rollupFields.usedCoupons, DSL.avg(source.USED_COUPONS).cast(Double.class).as(
                        "used_coupons"))
                .withUpdateValue(rollupFields.totalCoupons, UpsertBuilder.avg(rollupFields.samples))
                .withUpdateValue(rollupFields.usedCoupons, UpsertBuilder.avg(rollupFields.samples))
                .withSourceCondition(source.SNAPSHOT_TIME.eq(fromDateTimes))
                .withSourceGroupBy(rollupKey);
        upsert.getUpsert(dsl).execute();
    }

    /**
     * Stats table field info by rollup type.
     */
    private static final Map<RollupDurationType, StatsTypeFields> statsFieldsByRollup =
            new HashMap<>();

    static {
        StatsTypeFields
                hourFields = new StatsTypeFields();
        hourFields.table = RESERVED_INSTANCE_COVERAGE_BY_HOUR;
        hourFields.snapshotTime = RESERVED_INSTANCE_COVERAGE_BY_HOUR.SNAPSHOT_TIME;
        hourFields.entityId = RESERVED_INSTANCE_COVERAGE_BY_HOUR.ENTITY_ID;
        hourFields.regionId = RESERVED_INSTANCE_COVERAGE_BY_HOUR.REGION_ID;
        hourFields.availabilityZoneId = RESERVED_INSTANCE_COVERAGE_BY_HOUR.AVAILABILITY_ZONE_ID;
        hourFields.businessAccountId = RESERVED_INSTANCE_COVERAGE_BY_HOUR.BUSINESS_ACCOUNT_ID;
        hourFields.totalCoupons = RESERVED_INSTANCE_COVERAGE_BY_HOUR.TOTAL_COUPONS;
        hourFields.usedCoupons = RESERVED_INSTANCE_COVERAGE_BY_HOUR.USED_COUPONS;
        hourFields.samples = RESERVED_INSTANCE_COVERAGE_BY_HOUR.SAMPLES;
        hourFields.rollupKey = RESERVED_INSTANCE_COVERAGE_BY_HOUR.HOUR_KEY;

        statsFieldsByRollup.put(RollupDurationType.HOURLY, hourFields);

        StatsTypeFields
                dayFields = new StatsTypeFields();
        dayFields.table = RESERVED_INSTANCE_COVERAGE_BY_DAY;
        dayFields.snapshotTime = RESERVED_INSTANCE_COVERAGE_BY_DAY.SNAPSHOT_TIME;
        dayFields.entityId = RESERVED_INSTANCE_COVERAGE_BY_DAY.ENTITY_ID;
        dayFields.regionId = RESERVED_INSTANCE_COVERAGE_BY_DAY.REGION_ID;
        dayFields.availabilityZoneId = RESERVED_INSTANCE_COVERAGE_BY_DAY.AVAILABILITY_ZONE_ID;
        dayFields.businessAccountId = RESERVED_INSTANCE_COVERAGE_BY_DAY.BUSINESS_ACCOUNT_ID;
        dayFields.totalCoupons = RESERVED_INSTANCE_COVERAGE_BY_DAY.TOTAL_COUPONS;
        dayFields.usedCoupons = RESERVED_INSTANCE_COVERAGE_BY_DAY.USED_COUPONS;
        dayFields.samples = RESERVED_INSTANCE_COVERAGE_BY_DAY.SAMPLES;
        dayFields.rollupKey = RESERVED_INSTANCE_COVERAGE_BY_DAY.DAY_KEY;


        statsFieldsByRollup.put(RollupDurationType.DAILY, dayFields);

        StatsTypeFields
                monthlyFields = new StatsTypeFields();
        monthlyFields.table = RESERVED_INSTANCE_COVERAGE_BY_MONTH;
        monthlyFields.snapshotTime = RESERVED_INSTANCE_COVERAGE_BY_MONTH.SNAPSHOT_TIME;
        monthlyFields.entityId = RESERVED_INSTANCE_COVERAGE_BY_MONTH.ENTITY_ID;
        monthlyFields.regionId = RESERVED_INSTANCE_COVERAGE_BY_MONTH.REGION_ID;
        monthlyFields.availabilityZoneId = RESERVED_INSTANCE_COVERAGE_BY_MONTH.AVAILABILITY_ZONE_ID;
        monthlyFields.businessAccountId = RESERVED_INSTANCE_COVERAGE_BY_MONTH.BUSINESS_ACCOUNT_ID;
        monthlyFields.totalCoupons = RESERVED_INSTANCE_COVERAGE_BY_MONTH.TOTAL_COUPONS;
        monthlyFields.usedCoupons = RESERVED_INSTANCE_COVERAGE_BY_MONTH.USED_COUPONS;
        monthlyFields.samples = RESERVED_INSTANCE_COVERAGE_BY_MONTH.SAMPLES;
        monthlyFields.rollupKey = RESERVED_INSTANCE_COVERAGE_BY_MONTH.MONTH_KEY;


        statsFieldsByRollup.put(RollupDurationType.MONTHLY, monthlyFields);
    }
    /**
     * Used to store info about stats tables (hourly/daily/monthly fields).
     */
    private static final class StatsTypeFields {
        Table<?> table;
        TableField<?, LocalDateTime> snapshotTime;
        TableField<?, Long> entityId;
        TableField<?, Long> regionId;
        TableField<?, Long> availabilityZoneId;
        TableField<?, Long> businessAccountId;
        TableField<?, Double> totalCoupons;
        TableField<?, Double> usedCoupons;
        TableField<?, Integer> samples;
        TableField<?, String> rollupKey;
    }

    /**
     * Helper class for dumping monthly RI coverage db records to exported topology.
     */
    private static final class ReservedInstancesCoverageByMonthDiagsHelper implements
            TableDiagsRestorable<Void, ReservedInstanceCoverageByMonthRecord> {
        private static final String reservedInstanceCoverageByMonthDumpFile = "reservedInstanceCoverageByMonth_dump";

        private final DSLContext dsl;

        ReservedInstancesCoverageByMonthDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceCoverageByMonthRecord> getTable() {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return reservedInstanceCoverageByMonthDumpFile;
        }
    }

    /**
     * Helper class for dumping daily RI coverage db records to exported topology.
     */
    private static final class ReservedInstancesCoverageByDayDiagsHelper implements TableDiagsRestorable<Void, ReservedInstanceCoverageByDayRecord> {
        private static final String reservedInstanceCoverageByDayDumpFile = "reservedInstanceCoverageByDay_dump";

        private final DSLContext dsl;

        ReservedInstancesCoverageByDayDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceCoverageByDayRecord> getTable() {
            return RESERVED_INSTANCE_COVERAGE_BY_DAY;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return reservedInstanceCoverageByDayDumpFile;
        }
    }

    /**
     * Helper class for dumping hourly RI coverage db records to exported topology.
     */
    private static final class ReservedInstancesCoverageByHourDiagsHelper implements TableDiagsRestorable<Void, ReservedInstanceCoverageByHourRecord> {
        private static final String reservedInstanceCoverageByHourDumpFile = "reservedInstanceCoverageByHour_dump";

        private final DSLContext dsl;

        ReservedInstancesCoverageByHourDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceCoverageByHourRecord> getTable() {
            return RESERVED_INSTANCE_COVERAGE_BY_HOUR;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return reservedInstanceCoverageByHourDumpFile;
        }
    }

    /**
     * Helper class for dumping latest RI coverage db records to exported topology.
     */
    private static final class LatestReservedInstanceCoverageDiagsHelper implements TableDiagsRestorable<Void, ReservedInstanceCoverageLatestRecord> {
        private static final String latestReservedInstanceCoverageDumpFile = "latestReservedInstanceCoverage_dump";

        private final DSLContext dsl;

        LatestReservedInstanceCoverageDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceCoverageLatestRecord> getTable() {
            return Tables.RESERVED_INSTANCE_COVERAGE_LATEST;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return latestReservedInstanceCoverageDumpFile;
        }
    }
}

