package com.vmturbo.cost.component.reserved.instance;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationLatestRecord;

/**
 * This class is used to store reserved instance utilization information into databases. And it
 * used the {@link EntityReservedInstanceMappingStore} which has the latest used coupons for each
 * reserved instance, and also used the {@link ReservedInstanceBoughtStore} which has the latest
 * reserved instance information. And it will combine these data and store into database.
 */
public class ReservedInstanceUtilizationStore {

    private final static Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    public ReservedInstanceUtilizationStore(
            @Nonnull final DSLContext dsl,
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore) {
        this.dsl = dsl;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
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
        context.batchInsert(riUtilizationRecords).execute();
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
    private ReservedInstanceUtilizationLatestRecord createReservedInstanceUtilizationRecord (
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
                        riTotalCoupons, riUsedCouponsMap.get(riId)));
    }
}
