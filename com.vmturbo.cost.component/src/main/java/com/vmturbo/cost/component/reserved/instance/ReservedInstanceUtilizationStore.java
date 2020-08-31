package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.createSelectFieldsForRIUtilizationCoverage;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationByDayRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationByHourRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationByMonthRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationLatestRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;

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

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final ReservedInstanceUtilizationByHourDiagsHelper reservedInstanceUtilizationByHourDiagsHelper;

    private final ReservedInstanceUtilizationByMonthDiagsHelper reservedInstanceUtilizationByMonthDiagsHelper;

    private final ReservedInstanceUtilizationByDayDiagsHelper reservedInstanceUtilizationByDayDiagsHelper;

    private final LatestReservedInstanceUtilizationDiagsHelper latestReservedInstanceUtilizationDiagsHelper;

    public ReservedInstanceUtilizationStore(
            @Nonnull final DSLContext dsl,
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore) {
        this.dsl = dsl;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
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
    public void updateReservedInstanceUtilization(@Nonnull final DSLContext context) {
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
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
                entityReservedInstanceMappingStore.getReservedInstanceUsedCouponsMap(context);
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

    public List<ReservedInstanceStatsRecord> getReservedInstanceUtilizationStatsRecordsForExport() {
        final Result<Record> records =
                dsl.select(createSelectFieldsForRIUtilizationCoverage(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST))
                        .from(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).fetch();
        return records.stream()
                .map(ReservedInstanceUtil::convertRIUtilizationCoverageRecordToRIStatsRecord)
                .collect(Collectors.toList());
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
        final Result<Record> records = dsl.select(createSelectFieldsForRIUtilizationCoverage(table))
                .from(table)
                .where(filter.generateConditions(dsl))
                .groupBy(table.field(SNAPSHOT_TIME))
                .fetch();
        return records.stream()
                .map(ReservedInstanceUtil::convertRIUtilizationCoverageRecordToRIStatsRecord)
                .collect(Collectors.toList());
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
        return context.newRecord(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST,
                new ReservedInstanceUtilizationLatestRecord(curTime, riId, riSpecIdToRegionMap.get(riSpecId),
                        riBoughtInfo.getAvailabilityZoneId(), riBoughtInfo.getBusinessAccountId(),
                        riTotalCoupons, riUsedCouponsMap.getOrDefault(riId, 0.0), null,null,null));
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
    private static final class ReservedInstanceUtilizationByMonthDiagsHelper implements DiagsRestorable<Void> {
        private static final String reservedInstanceUtilizationByMonthDumpFile = "reservedInstanceUtilizationByMonth_dump";

        private final DSLContext dsl;

        ReservedInstanceUtilizationByMonthDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
            // TODO to be implemented as part of OM-58627
        }

        @Override
        public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
            dsl.transaction(transactionContext -> {
                final DSLContext transaction = DSL.using(transactionContext);
                Stream<ReservedInstanceUtilizationByMonthRecord> monthlyRecords = transaction.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH).stream();
                monthlyRecords.forEach(s -> {
                    try {
                        appender.appendString(s.formatJSON());
                    } catch (DiagnosticsException e) {
                        logger.error("Exception encountered while appending RI utilization by month records" +
                                " to the diags dump", e);
                    }
                });
            });
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
    private static final class ReservedInstanceUtilizationByDayDiagsHelper implements DiagsRestorable<Void> {
        private static final String reservedInstanceCoverageByDayDumpFile = "reservedInstanceUtilizationByDay_dump";

        private final DSLContext dsl;

        ReservedInstanceUtilizationByDayDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
            // TODO to be implemented as part of OM-58627
        }

        @Override
        public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
            dsl.transaction(transactionContext -> {
                final DSLContext transaction = DSL.using(transactionContext);
                Stream<ReservedInstanceUtilizationByDayRecord> dailyRecords = transaction.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).stream();
                dailyRecords.forEach(s -> {
                    try {
                        appender.appendString(s.formatJSON());
                    } catch (DiagnosticsException e) {
                        logger.error("Exception encountered while appending RI utilization by day records" +
                                " to the diags dump", e);
                    }
                });
            });
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
    private static final class ReservedInstanceUtilizationByHourDiagsHelper implements DiagsRestorable<Void> {
        private static final String reservedInstanceCoverageByHourDumpFile = "reservedInstanceUtilizationByHour_dump";

        private final DSLContext dsl;

        ReservedInstanceUtilizationByHourDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
            // TODO to be implemented as part of OM-58627
        }

        @Override
        public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
            dsl.transaction(transactionContext -> {
                final DSLContext transaction = DSL.using(transactionContext);
                Stream<ReservedInstanceUtilizationByHourRecord> hourlyRecords = transaction.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR).stream();
                hourlyRecords.forEach(s -> {
                    try {
                        appender.appendString(s.formatJSON());
                    } catch (DiagnosticsException e) {
                        logger.error("Exception encountered while appending RI utilization records by hour" +
                                " to the diags dump", e);
                    }
                });
            });
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
    private static final class LatestReservedInstanceUtilizationDiagsHelper implements DiagsRestorable<Void> {
        private static final String latestReservedInstanceUtilizationDumpFile = "latestReservedInstanceUtilization_dump";

        private final DSLContext dsl;

        LatestReservedInstanceUtilizationDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
            // TODO to be implemented as part of OM-58627
        }

        @Override
        public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
            dsl.transaction(transactionContext -> {
                final DSLContext transaction = DSL.using(transactionContext);
                Stream<ReservedInstanceUtilizationLatestRecord> latestRecords = transaction.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).stream();
                latestRecords.forEach(s -> {
                    try {
                        appender.appendString(s.formatJSON());
                    } catch (DiagnosticsException e) {
                        logger.error("Exception encountered while appending latest RI utilization records" +
                                " to the diags dump", e);
                    }
                });
            });
        }

        @Nonnull
        @Override
        public String getFileName() {
            return latestReservedInstanceUtilizationDumpFile;
        }
    }
}
