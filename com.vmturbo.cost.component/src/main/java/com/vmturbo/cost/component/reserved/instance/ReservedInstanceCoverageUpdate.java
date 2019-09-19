package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
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

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class used to handle reserved instance coverage update. Because for reserved instance
 * coverage, it needs to get both {@link EntityRICoverageUpload} data and real time
 * topology data. It used a {@link Cache} to store the received {@link EntityRICoverageUpload}
 * first, then if it receives the real time topology with same topology id, it will store the data
 * into the database.
 */
public class ReservedInstanceCoverageUpdate {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final Cache<Long, List<EntityRICoverageUpload>> riCoverageEntityCache;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    @Nonnull
    private final ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory;


    public ReservedInstanceCoverageUpdate(
            @Nonnull final DSLContext dsl,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory,
            final long riCoverageCacheExpireMinutes) {
        this.dsl = dsl;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.reservedInstanceCoverageValidatorFactory = reservedInstanceCoverageValidatorFactory;
        this.riCoverageEntityCache = CacheBuilder.newBuilder()
                .expireAfterAccess(riCoverageCacheExpireMinutes, TimeUnit.MINUTES)
                .build();
    }

    /**
     * Store a list {@link EntityRICoverageUpload} into {@link Cache}.
     *
     * @param topologyId the id of topology.
     * @param entityRICoverageList a list {@link EntityRICoverageUpload}.
     */
    public void storeEntityRICoverageOnlyIntoCache(
            final long topologyId,
            @Nonnull final List<EntityRICoverageUpload> entityRICoverageList) {
        riCoverageEntityCache.put(topologyId, entityRICoverageList);
    }

    /**
     * Input a real time topology map, if there are matched {@link EntityRICoverageUpload}
     * in the cache, it will store them into reserved instance coverage table. It used the topolgy id
     * to match the input real time topology with cached {@link EntityRICoverageUpload}.
     *
     * @param topologyId the id of topology.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     */
    public void updateAllEntityRICoverageIntoDB(
            final long topologyId,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final List<EntityRICoverageUpload> entityRICoverageList =
                riCoverageEntityCache.getIfPresent(topologyId);
        if (entityRICoverageList == null) {
            logger.info("Reserved instance coverage cache doesn't have {} data.", topologyId);
            return;
        }


        // Before storing the RI coverage, validate the entries, removing any that are deemed invalid.
        // Invalid entries may occur if the entity's state has recently changed (e.g. the tier has changed or
        // it's been shutdown, but not terminated), the RI's state has recently changed (switched from ISF to
        // non-isf), or the RI has expired.
        final ReservedInstanceCoverageValidator coverageValidator =
                reservedInstanceCoverageValidatorFactory.newValidator(cloudTopology);
        final List<EntityRICoverageUpload> validEntityRICoverageEntries =
                coverageValidator.validateCoverageUploads(
                        // The validator requires accurate coupon capacity to be reflected
                        // in the EntityRICoverageUpload::totalCouponsRequired field. The
                        // topology-processor uploads the capacity received from the cloud
                        // provider. However, for Azure, this value will always be 0. For AWS,
                        // it may reflect stale data (e.g. if the VM has changed its compute tier)
                        stitchCoverageCouponCapacityToTier(cloudTopology, entityRICoverageList));

        final List<ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecord =
                createServiceEntityReservedInstanceCoverageRecords(validEntityRICoverageEntries, cloudTopology);

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
     * @param entityRICoverageList a list of {@link EntityRICoverageUpload}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a list of {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    @VisibleForTesting
    List<ServiceEntityReservedInstanceCoverageRecord>
            createServiceEntityReservedInstanceCoverageRecords(
                    @Nonnull List<EntityRICoverageUpload> entityRICoverageList,
                    @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final Map<Long, ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecords =
                entityRICoverageList.stream()
                        .collect(Collectors.toMap(EntityRICoverageUpload::getEntityId,
                                entityRICoverage ->
                                        createRecordFromEntityRICoverage(entityRICoverage, cloudTopology)));
        return createAllEntityRICoverageRecord(seRICoverageRecords, cloudTopology);
    }

    /**
     * Generate a {@link ServiceEntityReservedInstanceCoverageRecord} based on input
     * {@link EntityRICoverageUpload} and {@link TopologyEntityCloudTopology}.
     *
     * @param entityRICoverage a {@link EntityRICoverageUpload}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a  {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    private ServiceEntityReservedInstanceCoverageRecord
        createRecordFromEntityRICoverage(
                @Nonnull final EntityRICoverageUpload entityRICoverage,
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
                        .mapToDouble(EntityRICoverageUpload.Coverage::getCoveredCoupons)
                        .sum())
                .build();
    }

    /**
     * Generate a list of {@link ServiceEntityReservedInstanceCoverageRecord} based on the input
     * real time topology, note that, for now, it only consider the VM entity.
     *
     *
     * @param seRICoverageRecords a Map which key is entity id, value is
     *                            {@link ServiceEntityReservedInstanceCoverageRecord} which created
     *                            by {@link EntityRICoverageUpload}.
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
                         .map(entity -> {
                             // totalCoupons will only reflect the computeTier capacity, if the entity
                             // is in a state in which it can be covered by an RI.
                             final EntityState entityState = entity.getEntityState();
                             final double totalCoupons = entityState == EntityState.POWERED_ON ?
                                     cloudTopology.getComputeTier(entity.getOid())
                                             .map(computeTier -> computeTier.getTypeSpecificInfo()
                                                     .getComputeTier().getNumCoupons())
                                             .orElse(0) : 0.0;

                             return ServiceEntityReservedInstanceCoverageRecord.newBuilder()
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
                                     .setTotalCoupons(totalCoupons)
                                     .setUsedCoupons(0.0)
                                     .build();
                         })
                .collect(Collectors.toList());
        // add all records which come from entity reserved coverage data.
        allEntityRICoverageRecords.addAll(seRICoverageRecords.values());
        return allEntityRICoverageRecords;
    }

    /**
     * Based on <code>cloudTopology</code>, updates the total coupons of each coverage instance, matching
     * the capacity to the tier covering the references entity. Currently, only virtual machine ->
     * compute tier is the only supported relationship (all others will be ignored).
     *
     * @param cloudTopology An instance of {@link CloudTopology}. The topology is expected to contain
     *                      both the direct entities of each {@link EntityRICoverageUpload} and their
     *                      corresponding compute tiers
     * @param entityRICoverageList A {@link Collection} of {@link EntityRICoverageUpload} instances
     * @return A {@link List} of copied and modified {@link EntityRICoverageUpload} instances
     */
    private List<EntityRICoverageUpload> stitchCoverageCouponCapacityToTier(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Collection<EntityRICoverageUpload> entityRICoverageList) {

        return entityRICoverageList.stream()
                .filter(entityRICoverage -> cloudTopology.getEntity(entityRICoverage.getEntityId())
                        .map(entityDto -> entityDto.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .orElse(false))
                .map(entityRICoverage -> EntityRICoverageUpload.newBuilder(entityRICoverage))
                .peek(entityRICoverageBuilder -> entityRICoverageBuilder.setTotalCouponsRequired(
                        cloudTopology.getComputeTier(entityRICoverageBuilder.getEntityId())
                                .map(computeTier -> computeTier.getTypeSpecificInfo()
                                        .getComputeTier().getNumCoupons())
                                .orElse(0)))
                .map(EntityRICoverageUpload.Builder::build)
                .collect(Collectors.toList());

    }

}
