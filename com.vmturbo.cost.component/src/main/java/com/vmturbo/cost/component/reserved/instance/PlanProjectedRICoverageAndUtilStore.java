package com.vmturbo.cost.component.reserved.instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceCoverageRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceUtilizationRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.RepositoryListener;

public class PlanProjectedRICoverageAndUtilStore implements RepositoryListener {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext context;

    private final RepositoryServiceBlockingStub repositoryServiceBlockingStub;

    private final RepositoryClient repositoryClient;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;

    private final int chunkSize;

    private final Object newLock = new Object();

    private final int projectedTopologyTimeOut;

    private final Map<Long, Boolean> projectedTopologyAvailable = new HashMap<Long, Boolean>();

    private final Map<Long, Param> cachedRICoverage = new HashMap<Long, Param>();

    private final long realtimeTopologyContextId;

    private final Set<Integer> entityTypeSet = ImmutableSet.of(EntityType.REGION_VALUE,
                                                                  EntityType.BUSINESS_ACCOUNT_VALUE,
                                                                  EntityType.VIRTUAL_MACHINE_VALUE,
                                                                  EntityType.AVAILABILITY_ZONE_VALUE,
                                                                  EntityType.COMPUTE_TIER_VALUE);
    private class Param {
        private final TopologyInfo topoInfo;
        private final List<EntityReservedInstanceCoverage> coverage;
        public Param (TopologyInfo topoInfo, List<EntityReservedInstanceCoverage> coverage) {
            this.topoInfo = topoInfo;
            this.coverage = coverage;
        }
        public TopologyInfo getTopologyInfo () {
            return topoInfo;
        }
        public List<EntityReservedInstanceCoverage> getCoverage () {
            return coverage;
        }
    }

    public PlanProjectedRICoverageAndUtilStore(@Nonnull final DSLContext context,
                                    int projectedTopologyTimeOut,
                                    @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub,
                                    @Nonnull final RepositoryClient repositoryClient,
                                    @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                                    @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
                                    @Nonnull SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
                                    final int chunkSize,
                                    final long realtimeTopologyContextId) {
        this.context = context;
        this.projectedTopologyTimeOut = projectedTopologyTimeOut;
        this.repositoryServiceBlockingStub = Objects.requireNonNull(repositoryServiceBlockingStub);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.supplyChainServiceBlockingStub = supplyChainServiceBlockingStub;
        this.chunkSize = chunkSize;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Update projected RI coverage based on plan analysis result.
     *
     * @param topoInfo
     * @param entityRICoverage
     */
    public void updateProjectedRICoverageTableForPlan(@Nonnull final long projectedTopologyId,
                                                      @Nonnull final TopologyInfo topoInfo,
                                                      @Nonnull final List<EntityReservedInstanceCoverage>
                                                      entityRICoverage) {
        synchronized(newLock) {
            if (!projectedTopologyAvailable.containsKey(projectedTopologyId)) {
                // the projected topology is not ready in repository yet,
                // cached the entityRICoverage until onProjectedTopologyAvailable
                cachedRICoverage.put(projectedTopologyId, new Param(topoInfo, entityRICoverage));
                return;
            } else if (!projectedTopologyAvailable.get(projectedTopologyId)) {
                // projected topology uploading in repository is failed
                logger.error("Abort ri coverage data persistence for plan {}",
                             topoInfo.getTopologyContextId());
                projectedTopologyAvailable.remove(projectedTopologyId);
                return;
            } else {
                logger.info("Projected topology {} for plan {} is ready", projectedTopologyId,
                            topoInfo.getTopologyContextId());
                projectedTopologyAvailable.remove(projectedTopologyId);
            }
        }
        insertRecordsToTable(projectedTopologyId, topoInfo, entityRICoverage);
    }

    /**
     * Construct entity RI coverage records and insert into projected_reserved_instance_coverage table.
     *
     * @param projectedTopologyId
     * @param topoInfo
     * @param entityRICoverage
     */
    private void insertRecordsToTable(long projectedTopologyId, @Nonnull TopologyInfo topoInfo,
                                       @Nonnull List<EntityReservedInstanceCoverage> entityRICoverage) {
        // get plan projected topology entity DTO from repository.
        long topologyContextId = topoInfo.getTopologyContextId();
        Map<Long, TopologyEntityDTO> entityMap = RepositoryDTOUtil.topologyEntityStream(repositoryServiceBlockingStub
            .retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topoInfo.getTopologyContextId())
                .setTopologyId(projectedTopologyId)
                .setReturnType(Type.FULL)
                .setTopologyType(TopologyType.PROJECTED)
                .addAllEntityType(entityTypeSet)
                .build()))
            .map(PartialEntity::getFullEntity)
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        Set<TopologyEntityDTO> allRegion = entityMap.values()
                .stream().filter( v -> v.getEntityType() == EntityType.REGION_VALUE)
                .collect(Collectors.toSet());
        Set<TopologyEntityDTO> allBa = entityMap.values()
                        .stream().filter( v -> v.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                        .collect(Collectors.toSet());
        List<PlanProjectedReservedInstanceCoverageRecord> coverageRcd = new ArrayList<>();
        Builder reservedInstanceBoughtFilterBuilder = ReservedInstanceBoughtFilter.newBuilder();
        final List<ReservedInstanceBought> allReservedInstancesBought =
                        reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                                              reservedInstanceBoughtFilterBuilder
                                              .cloudScopeTuples(
                           repositoryClient.getEntityOidsByType(topoInfo.getScopeSeedOidsList(),
                                                           realtimeTopologyContextId,
                                                           this.supplyChainServiceBlockingStub))
                                                  .build());
        Iterator<EntityReservedInstanceCoverage> it = entityRICoverage.iterator();
        while(it.hasNext()) {
            EntityReservedInstanceCoverage riCoverage = it.next();
            long entityId = riCoverage.getEntityId();
            TopologyEntityDTO entity = entityMap.get(entityId);
            if (entity == null || entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
                logger.error("Updating projecte RI coverage for an entity {} which is not found in "
                             + "topology with topologyContextId {}.", entityId, topologyContextId);
                continue;
            }
            // find az connected with entity
            List<ConnectedEntity> az = entity.getConnectedEntityListList().stream()
                    .filter(c -> c.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                    .collect(Collectors.toList());
            if (az.size() != 1) {
                logger.warn("Entity {} connected to wrong number of availability zone!", entity.getOid());
                continue;
            }
            // find region connected with az
            List<TopologyEntityDTO> region =
                    getConnectedEntityofType(allRegion, EntityType.AVAILABILITY_ZONE_VALUE,
                                             az.get(0).getConnectedEntityId());
            if (region.size() != 1) {
                logger.warn("Entity {} connected to wrong number of region!", entity.getOid());
                continue;
            }
            // find ba connected with entity
            // TODO: can the number of ba connected with a VM is not 1?
            List<TopologyEntityDTO> ba =
                    getConnectedEntityofType(allBa, EntityType.VIRTUAL_MACHINE_VALUE, entity.getOid());
            // find compute tier consumed by entity
            List<TopologyEntityDTO> computeTiers = getEntityConsumedComputeTiers(entity, entityMap);
            if (computeTiers.size() != 1) {
                logger.warn("Entity {} consumes wrong number of compute tiers {}!", entity.getOid(),
                    computeTiers.size());
                continue;
            }
            final double totalCoupons =
                    computeTiers.iterator().next().getTypeSpecificInfo().getComputeTier()
                            .getNumCoupons();
            riCoverage.getCouponsCoveredByRiMap().forEach((key, value) -> {
                coverageRcd.add(context.newRecord(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE,
                        new PlanProjectedReservedInstanceCoverageRecord(
                                entityId, topologyContextId, region.get(0).getOid(),
                                az.get(0).getConnectedEntityId(), ba.get(0).getOid(),
                                totalCoupons, value)));
                logger.debug("Projected reserved instance coverage record with entityId {}, topologyContextId {}, "
                                + "region id {}, az id {}, ba id {}, total coupon {}, used coupon {}.",
                        entityId, topologyContextId, region.get(0).getOid(), az.get(0).getConnectedEntityId(),
                        ba.get(0).getOid(), totalCoupons, value);
            });
        }
        Lists.partition(coverageRcd, chunkSize).forEach(entityChunk -> context.batchInsert(coverageRcd).execute());
    }

    /**
     * A helper method to get the compute tiers consumes by a given entity.
     *
     * @param entity the given entity
     * @param entityMap a map of TopologyEntityDTOs
     * @return a list of {@link TopologyEntityDTO}
     */
    private List<TopologyEntityDTO> getEntityConsumedComputeTiers(TopologyEntityDTO entity,
                                                                  Map<Long, TopologyEntityDTO> entityMap) {
        return entity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .filter(commBought -> commBought.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .distinct()
                .map(providerId -> {
                    final Optional<TopologyEntityDTO> providerEntity = entityMap.get(providerId) == null ?
                                    Optional.empty() : Optional.of(entityMap.get(providerId));
                    if (!providerEntity.isPresent()) {
                        logger.warn("Unable to find compute tier {} for entity {} in topology.",
                                providerId, entity.getOid());
                    }
                   return providerEntity;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * A helper method to get entities which has a {@link ConnectedTo} entity matching the given
     * entity's type and oid.
     *
     * @param entitySet candidate entity set
     * @param type the given entity type
     * @param oid the given entity oid
     * @return a list of connected entities satisfy the requirement
     */
    private  List<TopologyEntityDTO> getConnectedEntityofType(Set<TopologyEntityDTO> entitySet, int type, long oid) {
        List<TopologyEntityDTO> sourceEntity = new ArrayList<>();
        entitySet.forEach(e-> {
             if (e.getConnectedEntityListList()
             .stream()
             .anyMatch(c -> c.getConnectedEntityType() == type && c.getConnectedEntityId() == oid)) {
                 sourceEntity.add(e);
             }
        });
        return sourceEntity;
    }

    /**
     * Update ProjectedReservedInstanceUtilizationTable with new plan records sent from market.
     *
     * @param topoInfo the topology information
     * @param entityRICoverage a stream of ri coupon usage by projected entity
     * @return
     */
    public void updateProjectedRIUtilTableForPlan(@Nonnull final TopologyInfo topoInfo,
                                           @Nonnull final List<EntityReservedInstanceCoverage>
                                           entityRICoverage) {
        long contextId = topoInfo.getTopologyContextId();
        Map<Long, Double> riUsedCouponMap= new HashMap<>();
        entityRICoverage.forEach(e -> {
            e.getCouponsCoveredByRiMap().entrySet().forEach(entry -> {
                long riId = entry.getKey();
                Double currentUtil = riUsedCouponMap.get(riId);
                if (currentUtil != null) {
                    riUsedCouponMap.put(riId, currentUtil + entry.getValue());
                } else {
                    riUsedCouponMap.put(riId, entry.getValue());
                }
            });
        });
        riUsedCouponMap.entrySet().forEach( e-> {
        });
        final List<ReservedInstanceBought> projectedReservedInstancesBought =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter
                        .newBuilder()
                        .cloudScopeTuples(
                        repositoryClient.getEntityOidsByType(topoInfo.getScopeSeedOidsList(),
                                realtimeTopologyContextId, this.supplyChainServiceBlockingStub))
                        .build())
                .stream()
                .filter(ri -> riUsedCouponMap.containsKey(ri.getId()))
                .collect(Collectors.toList());
        final Set<Long> riSpecIds = projectedReservedInstancesBought.stream()
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(Collectors.toSet());
        final List<ReservedInstanceSpec> reservedInstanceSpecs =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds);
        final Map<Long, Long> riSpecIdToRegionMap = reservedInstanceSpecs.stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId,
                        riSpec -> riSpec.getReservedInstanceSpecInfo().getRegionId()));
        List<PlanProjectedReservedInstanceUtilizationRecord> records = new ArrayList<>();
        projectedReservedInstancesBought.stream().forEach(riBought -> {
            final long riId = riBought.getId();
            final ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
            final long riSpecId = riBoughtInfo.getReservedInstanceSpec();
            final double riTotalCoupons = riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons();
            records.add(context.newRecord(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION,
                    new PlanProjectedReservedInstanceUtilizationRecord(riId, contextId,
                            riSpecIdToRegionMap.get(riSpecId), riBoughtInfo.getAvailabilityZoneId(),
                            riBoughtInfo.getBusinessAccountId(), riTotalCoupons, riUsedCouponMap.get(riId))));
        });
        Lists.partition(records, chunkSize).forEach(entityChunk -> context.batchInsert(records).execute());
    }

    /**
     * Create records based on projected RI coverage and insert into
     * entity_to_projected_reserved_instance_mapping table.
     *
     * @param projectedTopologyId the plan topology context id
     * @param entityRICoverage the projected RI coverage generated in plan analysis
     */
    public void updateProjectedEntityToRIMappingTableForPlan(@Nonnull final TopologyInfo topoInfo,
                                                      @Nonnull final List<EntityReservedInstanceCoverage>
                                                      entityRICoverage) {
        List<PlanProjectedEntityToReservedInstanceMappingRecord> records = new ArrayList<>();
        long contextId = topoInfo.getTopologyContextId();
        entityRICoverage.forEach(e -> {
            long entityId = e.getEntityId();
            e.getCouponsCoveredByRiMap().entrySet().forEach(entry -> {
                records.add(context.newRecord(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                        new PlanProjectedEntityToReservedInstanceMappingRecord(entityId, contextId,
                                                                               entry.getKey(), entry.getValue())));
            });
        });
        Lists.partition(records, chunkSize).forEach(entityChunk -> context.batchInsert(records).execute());
    }

    @Override
    public void onProjectedTopologyAvailable(long projectedTopologyId, long topologyContextId) {
        final TopologyInfo topoInfo;
        final List<EntityReservedInstanceCoverage> coverage;
        synchronized(newLock) {
            if (!cachedRICoverage.containsKey(projectedTopologyId)) {
                projectedTopologyAvailable.put(projectedTopologyId, true);
                logger.info("The projected topology {} is available in repository", projectedTopologyId);
                return;
            } else {
                // if updateProjectedRICoverageTableForPlan is already being triggered and cached
                // we can remove it in the cachedRICoverage and trigger insertRecordsToTable
                topoInfo = cachedRICoverage.get(projectedTopologyId).getTopologyInfo();
                coverage = cachedRICoverage.get(projectedTopologyId).getCoverage();
                cachedRICoverage.remove(projectedTopologyId);
            }
            insertRecordsToTable(projectedTopologyId, topoInfo, coverage);
        }
    }

    @Override
    public void onProjectedTopologyFailure(long projectedTopologyId, long topologyContextId,
            @Nonnull String failureDescription) {
        synchronized(newLock) {
            if (!cachedRICoverage.containsKey(projectedTopologyId)) {
                projectedTopologyAvailable.put(projectedTopologyId, false);
                logger.error("Uploading projected topology {} is failed for plan {} due to {}",
                        projectedTopologyId, topologyContextId, failureDescription);
                return;
            } else {
                logger.error("Uploading projected topology {} is failed for plan {} due to {}",
                        projectedTopologyId, topologyContextId, failureDescription);
                cachedRICoverage.remove(projectedTopologyId);
                return;
            }
        }
    }

    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) { }

    @Override
    public void onSourceTopologyFailure(long topologyId, long topologyContextId,
            @Nonnull String failureDescription) {}
}
