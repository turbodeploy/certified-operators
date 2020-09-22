package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.tables.ReservedInstanceCoverageLatest.RESERVED_INSTANCE_COVERAGE_LATEST;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.createSelectFieldsForRIUtilizationCoverage;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.TableImpl;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageByDayRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageByHourRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageByMonthRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;

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

    public ReservedInstanceCoverageStore(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
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
            @Nonnull final List<ServiceEntityReservedInstanceCoverageRecord> entityRiCoverages) {
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
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
        return context.newRecord(Tables.RESERVED_INSTANCE_COVERAGE_LATEST,
                new ReservedInstanceCoverageLatestRecord(currentTime, entityRiCoverage.getId(),
                        entityRiCoverage.getRegionId(), entityRiCoverage.getAvailabilityZoneId(),
                        entityRiCoverage.getBusinessAccountId(), entityRiCoverage.getTotalCoupons(),
                        entityRiCoverage.getUsedCoupons(),null,null,null));

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
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY;
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
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
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

