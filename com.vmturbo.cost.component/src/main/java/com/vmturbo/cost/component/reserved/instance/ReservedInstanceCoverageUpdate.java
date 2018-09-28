package com.vmturbo.cost.component.reserved.instance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage.Coverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class used to handle reserved instance coverage update. Because for reserved instance
 * coverage, it needs to get both {@link EntityReservedInstanceCoverage} data and real time
 * topology data. It used a {@link Cache} to store the received {@link EntityReservedInstanceCoverage}
 * first, then if it receives the real time topology with same topology id, it will store the data
 * into the database.
 */
public class ReservedInstanceCoverageUpdate {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final Cache<Long, List<EntityReservedInstanceCoverage>> riCoverageEntityCache;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    public ReservedInstanceCoverageUpdate(
            @Nonnull final DSLContext dsl,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            final long riCoverageCacheExpireMinutes) {
        this.dsl = dsl;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.riCoverageEntityCache = CacheBuilder.newBuilder()
                .expireAfterAccess(riCoverageCacheExpireMinutes, TimeUnit.MINUTES)
                .build();
    }

    /**
     * Store a list {@link EntityReservedInstanceCoverage} into {@link Cache}.
     *
     * @param topologyId the id of topology.
     * @param entityRICoverageList a list {@link EntityReservedInstanceCoverage}.
     */
    public void storeEntityRICoverageOnlyIntoCache(
            final long topologyId,
            @Nonnull final List<EntityReservedInstanceCoverage> entityRICoverageList) {
        riCoverageEntityCache.put(topologyId, entityRICoverageList);
    }

    /**
     * Input a real time topology map, if there are matched {@link EntityReservedInstanceCoverage}
     * in the cache, it will store them into reserved instance coverage table. It used the topolgy id
     * to match the input real time topology with cached {@link EntityReservedInstanceCoverage}.
     *
     * @param topologyId the id of topology.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     */
    public void updateAllEntityRICoverageIntoDB(
            final long topologyId,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final List<EntityReservedInstanceCoverage> entityRICoverageList =
                riCoverageEntityCache.getIfPresent(topologyId);
        if (entityRICoverageList == null) {
            logger.info("Reserved instance coverage cache doesn't have {} data.", topologyId);
            return;
        }

        final List<ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecord =
                createServiceEntityReservedInstanceCoverageRecords(entityRICoverageList, cloudTopology);

        dsl.transaction(configuration -> {
            final DSLContext transactionContext = DSL.using(configuration);
            // need to update entity reserved instance mapping first, because reserved instance
            // utilization data will use them later.
            entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(transactionContext,
                    entityRICoverageList);
            reservedInstanceUtilizationStore.updateReservedInstanceUtilization(transactionContext);
            reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(transactionContext,
                    seRICoverageRecord);
        });
        // delete the entry for topology id in cache.
        riCoverageEntityCache.invalidate(topologyId);
    }

    /**
     * Generate a list of {@link ServiceEntityReservedInstanceCoverageRecord}, it will be stored into
     * reserved instance coverage tables.
     *
     * @param entityRICoverageList a list of {@link EntityReservedInstanceCoverage}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a list of {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    @VisibleForTesting
    List<ServiceEntityReservedInstanceCoverageRecord>
            createServiceEntityReservedInstanceCoverageRecords(
                    @Nonnull List<EntityReservedInstanceCoverage> entityRICoverageList,
                    @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final Map<Long, ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecords =
                entityRICoverageList.stream()
                        .filter(entityRICoverage ->
                                cloudTopology.getEntity(entityRICoverage.getEntityId()).isPresent())
                        .collect(Collectors.toMap(EntityReservedInstanceCoverage::getEntityId,
                                entityRICoverage ->
                                        createRecordFromEntityRICoverage(entityRICoverage, cloudTopology)));
        return createAllEntityRICoverageRecord(seRICoverageRecords, cloudTopology);
    }

    /**
     * Generate a {@link ServiceEntityReservedInstanceCoverageRecord} based on input
     * {@link EntityReservedInstanceCoverage} and {@link TopologyEntityCloudTopology}.
     *
     * @param entityRICoverage a {@link EntityReservedInstanceCoverage}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a  {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    private ServiceEntityReservedInstanceCoverageRecord
        createRecordFromEntityRICoverage(
                @Nonnull final EntityReservedInstanceCoverage entityRICoverage,
                @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        return ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                .setId(entityRICoverage.getEntityId())
                .setAvailabilityZoneId(
                        cloudTopology.getConnectedAvailabilityZone(entityRICoverage.getEntityId())
                                .map(TopologyEntityDTO::getOid)
                                .orElse(0L))
                .setRegionId(
                        cloudTopology.getConnectedRegion(entityRICoverage.getEntityId())
                                .map(TopologyEntityDTO::getOid)
                                .orElse(0L))
                .setBusinessAccountId(
                        cloudTopology.getOwner(entityRICoverage.getEntityId())
                                .map(TopologyEntityDTO::getOid)
                                .orElse(0L))
                .setTotalCoupons(entityRICoverage.getTotalCouponsRequired())
                .setUsedCoupons(entityRICoverage.getCoverageList().stream()
                        .mapToDouble(Coverage::getCoveredCoupons)
                        .sum())
                .build();
    }

    /**
     * Generate a list of {@link ServiceEntityReservedInstanceCoverageRecord} based on the input
     * real time topology, note that, for now, it only consider the VM entity.
     * @param seRICoverageRecords a Map which key is entity id, value is
     *                            {@link ServiceEntityReservedInstanceCoverageRecord} which created
     *                            by {@link EntityReservedInstanceCoverage}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a list of {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    private List<ServiceEntityReservedInstanceCoverageRecord> createAllEntityRICoverageRecord(
            @Nonnull final Map<Long, ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecords,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final List<ServiceEntityReservedInstanceCoverageRecord> allEntityRICoverageRecords =
                cloudTopology.getEntities().values().stream()
                        // only keep VM entity for now.
                         .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                         .filter(entity -> !seRICoverageRecords.containsKey(entity.getOid()))
                         .map(entity -> ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                                 .setId(entity.getOid())
                                 .setAvailabilityZoneId(
                                         cloudTopology.getConnectedAvailabilityZone(entity.getOid())
                                                 .map(TopologyEntityDTO::getOid)
                                                 .orElse(0L))
                                 .setRegionId(
                                         cloudTopology.getConnectedRegion(entity.getOid())
                                                 .map(TopologyEntityDTO::getOid)
                                                 .orElse(0L))
                                 .setBusinessAccountId(
                                         cloudTopology.getOwner(entity.getOid())
                                                 .map(TopologyEntityDTO::getOid)
                                                 .orElse(0L))
                                 .setTotalCoupons(cloudTopology.getComputeTier(entity.getOid())
                                         .map(computeTier ->
                                                 // Right now, we don't have coupons number for
                                                 // ComputerTier entity, after we keep the coupons
                                                 // information for ComputerTier in TopologyEntityDTO,
                                                 // we can remove this logic, and use it directly from
                                                 // TopologyEntityDTO.
                                                 AwsReservedInstanceCoupon.convertInstanceTypeToCoupons(
                                                         computeTier.getDisplayName()))
                                         .orElse(0))
                                 .setUsedCoupons(0.0)
                                 .build())
                .collect(Collectors.toList());
        // add all records which come from entity reserved coverage data.
        allEntityRICoverageRecords.addAll(seRICoverageRecords.values());
        return allEntityRICoverageRecords;
    }
}
