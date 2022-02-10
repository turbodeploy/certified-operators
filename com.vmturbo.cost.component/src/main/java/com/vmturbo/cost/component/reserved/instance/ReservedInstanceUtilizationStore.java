package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.createSelectFieldsForRIUtilizationCoverage;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.ReservedInstanceUtilizationLatest;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationByDayRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationByHourRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationByMonthRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationLatestRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.sql.utils.jooq.UpsertBuilder;

/**
 * This class is used to store reserved instance utilization information into databases. And it
 * used the {@link EntityReservedInstanceMappingStore} which has the latest used coupons for each
 * reserved instance, and also used the {@link ReservedInstanceBoughtStore} which has the latest
 * reserved instance information. And it will combine these data and store into database.
 */
public class ReservedInstanceUtilizationStore implements MultiStoreDiagnosable {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final ReservedInstanceUtilizationByHourDiagsHelper reservedInstanceUtilizationByHourDiagsHelper;

    private final ReservedInstanceUtilizationByMonthDiagsHelper reservedInstanceUtilizationByMonthDiagsHelper;

    private final ReservedInstanceUtilizationByDayDiagsHelper reservedInstanceUtilizationByDayDiagsHelper;

    private final LatestReservedInstanceUtilizationDiagsHelper latestReservedInstanceUtilizationDiagsHelper;

    private final boolean normalizeRollupSamples;

    private final float sampleNormalizationFactor;

    public ReservedInstanceUtilizationStore(
            @Nonnull final DSLContext dsl,
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            boolean normalizeRollupSamples,
            float sampleNormalizationFactor) {
        this.dsl = dsl;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.normalizeRollupSamples = normalizeRollupSamples;
        this.sampleNormalizationFactor = sampleNormalizationFactor;
        this.reservedInstanceUtilizationByHourDiagsHelper = new ReservedInstanceUtilizationByHourDiagsHelper(dsl);
        this.reservedInstanceUtilizationByDayDiagsHelper = new ReservedInstanceUtilizationByDayDiagsHelper(dsl);
        this.reservedInstanceUtilizationByMonthDiagsHelper = new ReservedInstanceUtilizationByMonthDiagsHelper(dsl);
        this.latestReservedInstanceUtilizationDiagsHelper = new LatestReservedInstanceUtilizationDiagsHelper(dsl);
    }

    /**
     * Read data from {@link ReservedInstanceBoughtStore} and {@link EntityReservedInstanceMappingStore}
     * and combine data together and store into database.
     * @param context {@link DSLContext} transactional context.
     */
    public void updateReservedInstanceUtilization(@Nonnull final DSLContext context,
                                                  @Nonnull Instant topologyCreationTime) {
        final LocalDateTime currentTime = topologyCreationTime.atOffset(ZoneOffset.UTC).toLocalDateTime();
        final List<ReservedInstanceBought> allReservedInstancesBought =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                        ReservedInstanceBoughtFilter.newBuilder().build());
        final Set<Long> riSpecIds = allReservedInstancesBought.stream()
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(Collectors.toSet());
        final List<ReservedInstanceSpec> reservedInstanceSpecs =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds);
        final Map<Long, Long> riSpecIdToRegionMap = reservedInstanceSpecs.stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId,
                        riSpec -> riSpec.getReservedInstanceSpecInfo().getRegionId()));
        final Map<Long, Double> riUsedCouponsMap =
                reservedInstanceBoughtStore.getNumberOfUsedCouponsForReservedInstances(context, Collections
                        .emptyList());
        final List<ReservedInstanceUtilizationLatestRecord> riUtilizationRecords =
                allReservedInstancesBought.stream()
                        .map(ri -> createReservedInstanceUtilizationRecord(context, ri, currentTime,
                                riSpecIdToRegionMap, riUsedCouponsMap))
                        .collect(Collectors.toList());

        final Query[] insertsWithDuplicates = riUtilizationRecords.stream().map(
                                       record -> DSL.using(context.configuration())
                                           .insertInto(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST)
                                           .set(record)
                                           .onDuplicateKeyUpdate()
                                           .set(record))
                               .toArray(Query[]::new);
        context.batch(insertsWithDuplicates).execute();
    }

    /**
     * Get the list of {@link ReservedInstanceStatsRecord} which aggregates data from reserved instance
     * utilization table.
     *
     * @param filter a {@link ReservedInstanceUtilizationFilter}.
     * @return a list of {@link ReservedInstanceStatsRecord}.
     */
    public List<ReservedInstanceStatsRecord> getReservedInstanceUtilizationStatsRecords(
            @Nonnull final ReservedInstanceUtilizationFilter filter) {
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
        final ReservedInstanceUtilizationLatest source = Tables.RESERVED_INSTANCE_UTILIZATION_LATEST;
        Table<?> targetTable = null;
        LocalDateTime toDateTime = null;
        Field<?> rollupKey = null;
        switch (rollupDuration) {
            case HOURLY: {
                targetTable = Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
                toDateTime = fromDateTimes.truncatedTo(ChronoUnit.HOURS);
                rollupKey = Tables.RESERVED_INSTANCE_UTILIZATION_LATEST.HOUR_KEY;
                break;
            }
            case DAILY: {
                targetTable = Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY;
                toDateTime = fromDateTimes.truncatedTo(ChronoUnit.DAYS);
                rollupKey = Tables.RESERVED_INSTANCE_UTILIZATION_LATEST.DAY_KEY;
                break;
            }
            case MONTHLY: {
                targetTable = Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
                YearMonth month = YearMonth.from(fromDateTimes);
                toDateTime = month.atEndOfMonth().atStartOfDay();
                rollupKey = Tables.RESERVED_INSTANCE_UTILIZATION_LATEST.MONTH_KEY;
                break;
            }
        }
        final StatsTypeFields rollupFields = statsFieldsByRollup.get(rollupDuration);
        final UpsertBuilder upsert = new UpsertBuilder();
        upsert.withInsertFields(
            rollupFields.samples,
            rollupFields.snapshotTime,
            rollupFields.id,
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
            .withInsertValue(rollupFields.id, DSL.max(source.ID).as("id"))
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
        hourFields.table = RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
        hourFields.snapshotTime = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.SNAPSHOT_TIME;
        hourFields.id = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.ID;
        hourFields.regionId = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.REGION_ID;
        hourFields.availabilityZoneId = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.AVAILABILITY_ZONE_ID;
        hourFields.businessAccountId = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.BUSINESS_ACCOUNT_ID;
        hourFields.totalCoupons = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.TOTAL_COUPONS;
        hourFields.usedCoupons = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.USED_COUPONS;
        hourFields.samples = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.SAMPLES;
        hourFields.rollupKey = RESERVED_INSTANCE_UTILIZATION_BY_HOUR.HOUR_KEY;

        statsFieldsByRollup.put(RollupDurationType.HOURLY, hourFields);

        StatsTypeFields
                dayFields = new StatsTypeFields();
        dayFields.table = RESERVED_INSTANCE_UTILIZATION_BY_DAY;
        dayFields.snapshotTime = RESERVED_INSTANCE_UTILIZATION_BY_DAY.SNAPSHOT_TIME;
        dayFields.id = RESERVED_INSTANCE_UTILIZATION_BY_DAY.ID;
        dayFields.regionId = RESERVED_INSTANCE_UTILIZATION_BY_DAY.REGION_ID;
        dayFields.availabilityZoneId = RESERVED_INSTANCE_UTILIZATION_BY_DAY.AVAILABILITY_ZONE_ID;
        dayFields.businessAccountId = RESERVED_INSTANCE_UTILIZATION_BY_DAY.BUSINESS_ACCOUNT_ID;
        dayFields.totalCoupons = RESERVED_INSTANCE_UTILIZATION_BY_DAY.TOTAL_COUPONS;
        dayFields.usedCoupons = RESERVED_INSTANCE_UTILIZATION_BY_DAY.USED_COUPONS;
        dayFields.samples = RESERVED_INSTANCE_UTILIZATION_BY_DAY.SAMPLES;
        dayFields.rollupKey = RESERVED_INSTANCE_UTILIZATION_BY_DAY.DAY_KEY;


        statsFieldsByRollup.put(RollupDurationType.DAILY, dayFields);

        StatsTypeFields
                monthlyFields = new StatsTypeFields();
        monthlyFields.table = RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
        monthlyFields.snapshotTime = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.SNAPSHOT_TIME;
        monthlyFields.id = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.ID;
        monthlyFields.regionId = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.REGION_ID;
        monthlyFields.availabilityZoneId = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.AVAILABILITY_ZONE_ID;
        monthlyFields.businessAccountId = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.BUSINESS_ACCOUNT_ID;
        monthlyFields.totalCoupons = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.TOTAL_COUPONS;
        monthlyFields.usedCoupons = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.USED_COUPONS;
        monthlyFields.samples = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.SAMPLES;
        monthlyFields.rollupKey = RESERVED_INSTANCE_UTILIZATION_BY_MONTH.MONTH_KEY;


        statsFieldsByRollup.put(RollupDurationType.MONTHLY, monthlyFields);
    }

    /**
     * Used to store info about stats tables (hourly/daily/monthly fields).
     */
    private static final class StatsTypeFields {
        Table<?> table;
        TableField<?, LocalDateTime> snapshotTime;
        TableField<?, Long> id;
        TableField<?, Long> regionId;
        TableField<?, Long> availabilityZoneId;
        TableField<?, Long> businessAccountId;
        TableField<?, Double> totalCoupons;
        TableField<?, Double> usedCoupons;
        TableField<?, Integer> samples;
        TableField<?, String> rollupKey;
    }

    /**
     * Create a {@link ReservedInstanceUtilizationLatestRecord}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param reservedInstanceBought {@link ReservedInstanceBought}.
     * @param curTime the current time.
     * @param riSpecIdToRegionMap a map which key is reserved instance spec id, value is the region id.
     * @param riUsedCouponsMap a map which key is the reserved instance id, the value is the used coupons.
     * @return a {@link ReservedInstanceUtilizationLatestRecord}.
     */
    private ReservedInstanceUtilizationLatestRecord createReservedInstanceUtilizationRecord(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBought reservedInstanceBought,
            @Nonnull final LocalDateTime curTime,
            @Nonnull final Map<Long, Long> riSpecIdToRegionMap,
            @Nonnull final Map<Long, Double> riUsedCouponsMap) {
        final long riId = reservedInstanceBought.getId();
        final ReservedInstanceBoughtInfo riBoughtInfo = reservedInstanceBought.getReservedInstanceBoughtInfo();
        final long riSpecId = riBoughtInfo.getReservedInstanceSpec();
        final double riTotalCoupons = riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons();
        final double usedCoupons = riUsedCouponsMap.getOrDefault(riId, 0.0);
        if (usedCoupons > riTotalCoupons) {
            logger.warn(
                    "Used coupons {} exceeds total coupons {} while creating Reserved Instance utilization record for RI {}.",
                    usedCoupons, riTotalCoupons, riId);
        }
        String hourKey = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? ReservedInstanceUtil.createHourKey(curTime, riId,
                riSpecIdToRegionMap.get(riSpecId), riBoughtInfo.getAvailabilityZoneId(),
                riBoughtInfo.getBusinessAccountId()) : null;

        String dayKey = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? ReservedInstanceUtil.createDayKey(curTime, riId,
                riSpecIdToRegionMap.get(riSpecId), riBoughtInfo.getAvailabilityZoneId(),
                riBoughtInfo.getBusinessAccountId()) : null;

        String monthKey = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? ReservedInstanceUtil.createMonthKey(curTime, riId,
                riSpecIdToRegionMap.get(riSpecId), riBoughtInfo.getAvailabilityZoneId(),
                riBoughtInfo.getBusinessAccountId()) : null;

        return context.newRecord(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST,
                new ReservedInstanceUtilizationLatestRecord(curTime, riId,
                        riSpecIdToRegionMap.get(riSpecId), riBoughtInfo.getAvailabilityZoneId(),
                        riBoughtInfo.getBusinessAccountId(), riTotalCoupons, usedCoupons, hourKey,
                        dayKey, monthKey));
    }

    @Override
    public Set<Diagnosable> getDiagnosables(final boolean collectHistoricalStats) {
        HashSet<Diagnosable> storesToSave = new HashSet<>();
        storesToSave.add(latestReservedInstanceUtilizationDiagsHelper);
        if (collectHistoricalStats) {
            storesToSave.add(reservedInstanceUtilizationByMonthDiagsHelper);
            storesToSave.add(reservedInstanceUtilizationByDayDiagsHelper);
            storesToSave.add(reservedInstanceUtilizationByHourDiagsHelper);
        }
        return storesToSave;
    }

    /**
     * Helper class for dumping monthly RI utilization db records to exported topology.
     */
    private static final class ReservedInstanceUtilizationByMonthDiagsHelper implements
            TableDiagsRestorable<Void, ReservedInstanceUtilizationByMonthRecord> {
        private static final String reservedInstanceUtilizationByMonthDumpFile = "reservedInstanceUtilizationByMonth_dump";

        private final DSLContext dsl;

        ReservedInstanceUtilizationByMonthDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceUtilizationByMonthRecord> getTable() {
            return Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return reservedInstanceUtilizationByMonthDumpFile;
        }
    }

    /**
     * Helper class for dumping daily RI utilization db records to exported topology.
     */
    private static final class ReservedInstanceUtilizationByDayDiagsHelper implements TableDiagsRestorable<Void, ReservedInstanceUtilizationByDayRecord> {
        private static final String reservedInstanceCoverageByDayDumpFile = "reservedInstanceUtilizationByDay_dump";

        private final DSLContext dsl;

        ReservedInstanceUtilizationByDayDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceUtilizationByDayRecord> getTable() {
            return Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return reservedInstanceCoverageByDayDumpFile;
        }
    }

    /**
     * Helper class for dumping hourly RI utilization db records to exported topology.
     */
    private static final class ReservedInstanceUtilizationByHourDiagsHelper implements TableDiagsRestorable<Void, ReservedInstanceUtilizationByHourRecord> {
        private static final String reservedInstanceCoverageByHourDumpFile = "reservedInstanceUtilizationByHour_dump";

        private final DSLContext dsl;

        ReservedInstanceUtilizationByHourDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceUtilizationByHourRecord> getTable() {
            return Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
        }


        @Nonnull
        @Override
        public String getFileName() {
            return reservedInstanceCoverageByHourDumpFile;
        }
    }

    /**
     * Helper class for dumping latest RI utilization db records to exported topology.
     */
    private static final class LatestReservedInstanceUtilizationDiagsHelper implements TableDiagsRestorable<Void, ReservedInstanceUtilizationLatestRecord> {
        private static final String latestReservedInstanceUtilizationDumpFile = "latestReservedInstanceUtilization_dump";

        private final DSLContext dsl;

        LatestReservedInstanceUtilizationDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<ReservedInstanceUtilizationLatestRecord> getTable() {
            return Tables.RESERVED_INSTANCE_UTILIZATION_LATEST;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return latestReservedInstanceUtilizationDumpFile;
        }
    }
}
