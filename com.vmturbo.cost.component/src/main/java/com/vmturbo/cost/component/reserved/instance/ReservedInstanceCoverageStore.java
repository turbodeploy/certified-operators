package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.tables.ReservedInstanceCoverageLatest.RESERVED_INSTANCE_COVERAGE_LATEST;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.createSelectFieldsForRIUtilizationCoverage;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;

/**
 * This class is used to store entity reserved instance coupons coverage information into database.
 * And it used the data which comes from real time topology which contains the total coupons for
 * each entity, and also used the {@link EntityReservedInstanceMappingStore} which has the latest
 * used coupons for each entity. And it will combine these data and store into database.
 */
public class ReservedInstanceCoverageStore {

    //TODO: set this chunk config through consul.
    private final static int chunkSize = 1000;

    private final static ReservedInstanceCoverageFilter reservedInstanceCoverageFilter = ReservedInstanceCoverageFilter
            .newBuilder().build();

    private final DSLContext dsl;

    public ReservedInstanceCoverageStore(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
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
}
