package com.vmturbo.cost.component.reserved.instance;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.google.common.collect.Lists;

import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;

/**
 * This class is used to store entity reserved instance coupons coverage information into database.
 * And it used the data which comes from real time topology which contains the total coupons for
 * each entity, and also used the {@link EntityReservedInstanceMappingStore} which has the latest
 * used coupons for each entity. And it will combine these data and store into database.
 */
public class ReservedInstanceCoverageStore {

    private final static Logger logger = LogManager.getLogger();

    //TODO: set this chunk config through consul.
    private final static int chunkSize = 1000;

    private final DSLContext dsl;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    public ReservedInstanceCoverageStore(
            @Nonnull final DSLContext dsl,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore) {
        this.dsl = dsl;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
    }

    /**
     * Given a list of {@link ServiceEntityReservedInstanceCoverage} and current entity reserved
     * instance mapping information, combine them together and store into database.
     *
     * @param context {@link DSLContext} transactional context.
     * @param entityRiCoverages a list {@link ServiceEntityReservedInstanceCoverage}.
     */
    public void updateReservedInstanceCoverageStore(
            @Nonnull final DSLContext context,
            @Nonnull final List<ServiceEntityReservedInstanceCoverage> entityRiCoverages) {
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        final Map<Long, Double> entityRiUsedCouponsMap =
                entityReservedInstanceMappingStore.getEntityUsedCouponsMap(context);
        List<ReservedInstanceCoverageLatestRecord> riCoverageRecords = entityRiCoverages.stream()
                .map(entityRiCoverage -> createReservedInstanceCoverageRecord(context, currentTime,
                        entityRiCoverage, entityRiUsedCouponsMap))
                .collect(Collectors.toList());
        Lists.partition(riCoverageRecords, chunkSize).forEach(entityChunk ->
                context.batchInsert(entityChunk).execute());
    }

    /**
     * Create {@link ReservedInstanceCoverageLatestRecord}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param currentTime the current time.
     * @param entityRiCoverage {@link ServiceEntityReservedInstanceCoverage}.
     * @param entityRiUsedCouponsMap a map which key is entity id, value is the used coupons of the entity.
     * @return
     */
    private ReservedInstanceCoverageLatestRecord createReservedInstanceCoverageRecord(
            @Nonnull final DSLContext context,
            @Nonnull final LocalDateTime currentTime,
            @Nonnull final ServiceEntityReservedInstanceCoverage entityRiCoverage,
            @Nonnull final Map<Long, Double> entityRiUsedCouponsMap) {
        return context.newRecord(Tables.RESERVED_INSTANCE_COVERAGE_LATEST,
                new ReservedInstanceCoverageLatestRecord(currentTime, entityRiCoverage.getId(),
                        entityRiCoverage.getRegionId(), entityRiCoverage.getAvailabilityZoneId(),
                        entityRiCoverage.getBusinessAccountId(), entityRiCoverage.getTotalCoupons(),
                        entityRiUsedCouponsMap.getOrDefault(entityRiCoverage.getId(), 0.0),null,null,null));
    }
}
