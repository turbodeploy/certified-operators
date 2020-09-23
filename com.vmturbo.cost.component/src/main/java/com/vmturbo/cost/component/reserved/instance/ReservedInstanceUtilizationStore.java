package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.createSelectFieldsForRIUtilizationCoverage;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cost.component.TableDiagsRestorable;
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

    private final ReservedInstanceUtilizationByHourDiagsHelper reservedInstanceUtilizationByHourDiagsHelper;

    private final ReservedInstanceUtilizationByMonthDiagsHelper reservedInstanceUtilizationByMonthDiagsHelper;

    private final ReservedInstanceUtilizationByDayDiagsHelper reservedInstanceUtilizationByDayDiagsHelper;

    private final LatestReservedInstanceUtilizationDiagsHelper latestReservedInstanceUtilizationDiagsHelper;

    public ReservedInstanceUtilizationStore(
            @Nonnull final DSLContext dsl,
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore) {
        this.dsl = dsl;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
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
