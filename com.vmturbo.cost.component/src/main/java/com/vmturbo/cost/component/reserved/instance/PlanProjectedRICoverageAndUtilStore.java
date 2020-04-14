package com.vmturbo.cost.component.reserved.instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
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
import com.vmturbo.cost.component.reserved.instance.filter.PlanProjectedEntityReservedInstanceMappingFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.RepositoryListener;

public class PlanProjectedRICoverageAndUtilStore implements RepositoryListener {

    private static final Logger logger = LogManager.getLogger();

    private static final String PLAN_ID = "plan_id";

    private static final String REGION_ID = "region_id";

    private final DSLContext context;

    private final RepositoryServiceBlockingStub repositoryServiceBlockingStub;

    private final PlanReservedInstanceServiceBlockingStub planReservedInstanceService;

    private final RepositoryClient repositoryClient;

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
                                    @Nonnull final PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
                                    @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
                                    @Nonnull SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
                                    final int chunkSize,
                                    final long realtimeTopologyContextId) {
        this.context = context;
        this.projectedTopologyTimeOut = projectedTopologyTimeOut;
        this.repositoryServiceBlockingStub = Objects.requireNonNull(repositoryServiceBlockingStub);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.planReservedInstanceService = planReservedInstanceService;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
        this.supplyChainServiceBlockingStub = supplyChainServiceBlockingStub;
        this.chunkSize = chunkSize;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Update projected RI coverage based on plan analysis result.
     *
     * @param projectedTopologyId the projected topology ID.
     * @param topoInfo   contains the plan id
     * @param entityRICoverage the RI coverage
     */
    public void updateProjectedRICoverageTableForPlan(final long projectedTopologyId,
                                                      @Nonnull final TopologyInfo topoInfo,
                                                      @Nonnull final List<EntityReservedInstanceCoverage>
                                                      entityRICoverage) {
        synchronized(newLock) {
            long planId = topoInfo.getTopologyContextId();
            if (!projectedTopologyAvailable.containsKey(projectedTopologyId)) {
                // the projected topology is not ready in repository yet,
                // cached the entityRICoverage until onProjectedTopologyAvailable
                logger.debug("Add projected topology {} in plan {} to cache",
                    projectedTopologyId, planId);
                cachedRICoverage.put(projectedTopologyId, new Param(topoInfo, entityRICoverage));
                return;
            } else if (!projectedTopologyAvailable.get(projectedTopologyId)) {
                // projected topology uploading in repository 'is failed'
                logger.error("Abort RI coverage data persistence for projected topology {} in plan {}",
                    projectedTopologyId, planId);
                projectedTopologyAvailable.remove(projectedTopologyId);
                // clear the cache, because aborted.
                cachedRICoverage.remove(projectedTopologyId);
                return;
            } else {
                logger.debug("Projected topology {} for plan {} is ready", projectedTopologyId,
                    planId);
                projectedTopologyAvailable.remove(projectedTopologyId);
            }
            logger.debug("The projected topology {} in plan {} is written to repository",
                projectedTopologyId, planId);
            insertRecordsToTable(projectedTopologyId, topoInfo, entityRICoverage);
            // clear the cache, because the data is written.
            cachedRICoverage.remove(projectedTopologyId);
        }
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
        Set<TopologyEntityDTO> allRegion = entityMap.values().stream()
                .filter(v -> v.getEntityType() == EntityType.REGION_VALUE)
                .collect(Collectors.toSet());
        Set<TopologyEntityDTO> allBa = entityMap.values()
                        .stream().filter( v -> v.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                        .collect(Collectors.toSet());
        List<PlanProjectedReservedInstanceCoverageRecord> coverageRcd = new ArrayList<>();
        // Get aggregated RI coverage for each entity.
        Map<Long, Double> aggregatedEntityRICoverages = getAggregatedEntityRICoverage(entityRICoverage);
        for (Map.Entry<Long, Double> aggregatedEntityRICoverage : aggregatedEntityRICoverages.entrySet()) {
            long entityId = aggregatedEntityRICoverage.getKey();
            TopologyEntityDTO entity = entityMap.get(entityId);
            if (entity == null || entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
                logger.error("Updating projected RI coverage for an entity {} which is not found in "
                        + "topology with topologyContextId {}.", entityId, topologyContextId);
                continue;
            }

            long baOid;
            long zoneOid;
            long regionOid;

            // find az connected with entity
            List<ConnectedEntity> az = entity.getConnectedEntityListList().stream()
                    .filter(c -> c.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                    .collect(Collectors.toList());
            if (az.size() == 1) {
                // get the region from the zone
                zoneOid = az.get(0).getConnectedEntityId();
                regionOid =
                        getConnectedEntityofType(allRegion, EntityType.AVAILABILITY_ZONE_VALUE, zoneOid).stream()
                                .map(e -> e.getOid())
                                .findFirst().orElse(0l);
            } else {
                // get the region directly from the entity
                regionOid = entity.getConnectedEntityListList().stream()
                        .filter(c -> c.getConnectedEntityType() == EntityType.REGION_VALUE)
                        .map(c -> c.getConnectedEntityId())
                        .findFirst().orElse(0l);
                // set the zone with a default value, like we do in real-time
                zoneOid = 0l;
            }
            if (regionOid == 0l) {
                logger.warn("Entity {} connected to wrong number of region!", entity.getOid());
                continue;
            }

            // find ba connected with entity
            baOid = getConnectedEntityofType(allBa, EntityType.VIRTUAL_MACHINE_VALUE, entity.getOid()).stream()
                    .map(e -> e.getOid())
                    .findFirst().orElse(0l);
            if (baOid == 0l) {
                logger.warn("Entity {} connected to wrong number of business account!", entity.getOid());
                continue;
            }

            // find compute tier consumed by entity
            final Optional<Integer> optionalCouponCapacity = entityRICoverage.stream().filter(s -> s.getEntityId() == entityId)
                    .map(a -> a.getEntityCouponCapacity()).findFirst();
            // The aggregated RI coverage of the entity
            final Double usedCoupons = aggregatedEntityRICoverage.getValue();
            if (optionalCouponCapacity.isPresent()) {
                Double totalCoupons = Double.valueOf(optionalCouponCapacity.get());
                if (usedCoupons > totalCoupons) {
                    // Used coupons should be less than or equals total coupons.
                    logger.error("Used coupons are greater than total coupons for " +
                                    "entityId {}, topologyContextId {}, region id {}, az id {}" +
                                    ", ba id {}, total coupon {}, used coupon {}.",
                            entityId, topologyContextId, regionOid, zoneOid,
                            baOid, totalCoupons, usedCoupons);
                } else {
                    // Used coupons are less than or equals total coupons.
                    coverageRcd.add(context.newRecord(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE,
                            new PlanProjectedReservedInstanceCoverageRecord(
                                    entityId, topologyContextId, regionOid, zoneOid, baOid, totalCoupons, usedCoupons)));
                    logger.debug("Projected reserved instance coverage record with entityId {}, topologyContextId {}, "
                                    + "region id {}, az id {}, ba id {}, total coupon {}, used coupon {}.",
                            entityId, topologyContextId, regionOid, zoneOid, baOid, totalCoupons, usedCoupons);
                }
            }
        }
        Lists.partition(coverageRcd, chunkSize).forEach(entityChunk -> context.batchInsert(coverageRcd).execute());
    }

    /**
     * Combines the RI coverages of each entity.
     *
     * @param entityRICoverage The list of the RI coverages
     * @return The aggregated list of the RI coverages of the entities
     */
    @Nonnull
    @VisibleForTesting
    Map<Long, Double> getAggregatedEntityRICoverage(@Nonnull final List<EntityReservedInstanceCoverage> entityRICoverage) {
        Map<Long, Double> aggregatedEntityRICoverages = new HashMap<>();
        for (final EntityReservedInstanceCoverage riCoverage : entityRICoverage) {
            long entityId = riCoverage.getEntityId();
            final double totalCouponsUsed = CollectionUtils.union(
                    riCoverage.getCouponsCoveredByRiMap().values(),
                    riCoverage.getCouponsCoveredByBuyRiMap().values())
                        .stream()
                        .reduce(0D, Double::sum);
            aggregatedEntityRICoverages.compute(entityId, (k,v) ->
            v == null ? totalCouponsUsed : totalCouponsUsed + v);
        }
        return aggregatedEntityRICoverages;
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
     * A helper method to get entities which has a {@link ConnectedEntity} entity matching the given
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
     * @param recommendedRis a collection of {@link ReservedInstanceBought} representing the RIs recommended for purchase
     * @return
     */
    public void updateProjectedRIUtilTableForPlan(@Nonnull final TopologyInfo topoInfo,
                                                  @Nonnull final List<EntityReservedInstanceCoverage> entityRICoverage,
                                                  @Nonnull final List<ReservedInstanceBought> recommendedRis) {
        long contextId = topoInfo.getTopologyContextId();
        Map<Long, Double> riUsedCouponMap= new HashMap<>();
        final Map<Long, Double> riRecommendedCouponMap = Maps.newHashMap();
        entityRICoverage.forEach(e -> {
            e.getCouponsCoveredByRiMap().forEach((riId, currentUsed) -> {
                riUsedCouponMap.merge(riId, currentUsed, Double::sum);
            });
            e.getCouponsCoveredByBuyRiMap().forEach((riId, currentCoverage) -> {
                riRecommendedCouponMap.merge(riId, currentCoverage, Double::sum);
            });
        });
        final GetPlanReservedInstanceBoughtRequest planSavedRiRequest =
                GetPlanReservedInstanceBoughtRequest
                        .newBuilder()
                        .setPlanId(topoInfo.getTopologyContextId())
                        .build();
        // Get RIs out of plan RI inventory config that user has selected.
        final List<ReservedInstanceBought> selectedRis =
                planReservedInstanceService
                        .getPlanReservedInstanceBought(planSavedRiRequest)
                        .getReservedInstanceBoughtsList();
        final List<ReservedInstanceBought> allPlanRis = ListUtils.union(selectedRis, recommendedRis);
        final Set<Long> riSpecIds = allPlanRis.stream()
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(Collectors.toSet());
        final List<ReservedInstanceSpec> reservedInstanceSpecs =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds);
        final Map<Long, Long> riSpecIdToRegionMap = reservedInstanceSpecs.stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId,
                        riSpec -> riSpec.getReservedInstanceSpecInfo().getRegionId()));
        List<PlanProjectedReservedInstanceUtilizationRecord> records = new ArrayList<>();
        allPlanRis.forEach(riBought -> {
            final long riId = riBought.getId();
            final ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
            final long riSpecId = riBoughtInfo.getReservedInstanceSpec();
            final double riTotalCoupons = riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons();
            records.add(context.newRecord(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION,
                    new PlanProjectedReservedInstanceUtilizationRecord(riId, contextId,
                            riSpecIdToRegionMap.get(riSpecId), riBoughtInfo.getAvailabilityZoneId(),
                            riBoughtInfo.getBusinessAccountId(), riTotalCoupons,
                            riUsedCouponMap.getOrDefault(riId, riRecommendedCouponMap.getOrDefault(riId, 0.0))
                    )));
        });
        Lists.partition(records, chunkSize).forEach(entityChunk -> context.batchInsert(records).execute());
    }

    /**
     * Create records based on projected RI coverage and insert into
     * entity_to_projected_reserved_instance_mapping table.
     *
     * @param topoInfo the plan topology info
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

    /**
     * Get the list of {@link ReservedInstanceStatsRecord} which aggregates data from plan projected reserved instance
     * utilization table.
     *
     * @param planId plan ID.
     * @param regions a list of regions.
     * @return a list of {@link ReservedInstanceStatsRecord}.
     */
    public List<ReservedInstanceStatsRecord> getPlanReservedInstanceUtilizationStatsRecords(long planId, List<Long> regions) {
        return getPlanRIStatsRecords(planId, Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION, regions);
    }

    private List<ReservedInstanceStatsRecord> getPlanRIStatsRecords(long planId, final Table<?> table, List<Long> regions) {
        Condition conditions = table.field(PLAN_ID, Long.class).eq(planId);
        // If there is no region filter, return data for all regions.
        if (regions != null && !regions.isEmpty()) {
            conditions = conditions.and(table.field(REGION_ID, Long.class).in(regions));
        }
        final Result<Record> records =
                        context.select(ReservedInstanceUtil.createSelectFieldsForPlanRIUtilizationCoverage(table))
                                        .from(table)
                                        .where(conditions)
                                        .fetch();
        return records.stream().filter(r -> r.getValue(0) != null)
                        .map(ReservedInstanceUtil::convertPlanRIUtilizationCoverageRecordToRIStatsRecord)
                        .collect(Collectors.toList());
    }

    /**
     * Interrogates the plan_projected_reserved_instance_coverage table to retrieve a Map of entity_id to total_coupons.
     *
     * @param planId the plan for which RI coverage details should be retrieved
     * @return a map of entity_id to the total_coupons covered
     */
    private Map<Long, Double> getPlanEntityToTotalCoupons(long planId) {
        Map<Long, Double> entityIdToTotalCoupons = Maps.newHashMap();
        context.select(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE.ENTITY_ID, Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE.TOTAL_COUPONS)
                .from(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE)
                .where(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE.PLAN_ID.eq(planId))
                .fetch()
                .forEach(record -> entityIdToTotalCoupons.put(record.value1(), record.value2()));
        return entityIdToTotalCoupons;
    }

    /**
     * Queries PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING to retrieve a map of entity_id to the number of
     * coupons covered.
     *
     * @param filter the {@link PlanProjectedEntityReservedInstanceMappingFilter} specifying the topologyContextId,
     *               entityId, riBoughtFilter, and conditions for a DB query to be executed
     * @return a map of entity_id to riBought to the number of covered coupons
     */
    private Map<Long, Map<Long, Double>> getPlanProjectedReservedInstanceUsedCouponsMapWithFilter(@Nonnull PlanProjectedEntityReservedInstanceMappingFilter filter) {
        final Map<Long, Map<Long, Double>> entityToRiToCoveredCoupons = Maps.newHashMap();
        context.select(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID,
                Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID,
                Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.USED_COUPONS)
                .from(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                .where(filter.getConditions())
                .fetch()
                .forEach(record -> {
                    Long entityId = record.value2();
                    entityToRiToCoveredCoupons.putIfAbsent(entityId, Maps.newHashMap());
                    entityToRiToCoveredCoupons.get(entityId)
                            .put(record.value1(), record.value3());
                });
        return entityToRiToCoveredCoupons;
    }

    /**
     * Calls getPlanEntityToTotalCoupons and getPlanProjectedReservedInstanceUsedCouponsMapWithFilter, and combines the
     * results to provide the projected RI coverage for a given plan, entity, RI combination specified by the args.
     *
     * @param planId the topologyContextId for which to query RI coverage
     * @param filter the {@link PlanProjectedEntityReservedInstanceMappingFilter} specifying the topologyContextId,
     *               entityId, riBoughtFilter, and conditions for a DB query to be executed
     * @return a map of entity_ud to {@link EntityReservedInstanceCoverage}
     */
    public Map<Long, EntityReservedInstanceCoverage> getPlanProjectedRiCoverage(
        Long planId, @Nonnull PlanProjectedEntityReservedInstanceMappingFilter filter) {

        final Map<Long, Double> entityToTotalCoupons = getPlanEntityToTotalCoupons(planId);
        final Map<Long, Map<Long, Double>> entityToRiToCoveredCoupons =
            getPlanProjectedReservedInstanceUsedCouponsMapWithFilter(filter);

        Map<Long, EntityReservedInstanceCoverage> entityToRiCoverage = entityToRiToCoveredCoupons.entrySet().stream()
            .map(entityEntry -> EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(entityEntry.getKey())
                .setEntityCouponCapacity(entityToTotalCoupons.getOrDefault(entityEntry.getKey(), 0D).intValue())
                .putAllCouponsCoveredByRi(entityEntry.getValue())
                .build())
            .collect(Collectors.toMap(EntityReservedInstanceCoverage::getEntityId, Function.identity()));

        return entityToRiCoverage;
    }

    /**
     * Get the list of {@link ReservedInstanceStatsRecord} which aggregates data from plan projected reserved instance
     * coverage table.
     *
     * @param planId plan ID.
     * @param regions a list of regions.
     * @return a list of {@link ReservedInstanceStatsRecord}.
     */
    public List<ReservedInstanceStatsRecord> getPlanReservedInstanceCoverageStatsRecords(long planId, List<Long> regions) {
        return getPlanRIStatsRecords(planId, Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE, regions);
    }

    @Override
    public void onProjectedTopologyAvailable(long projectedTopologyId, long planId) {
        final TopologyInfo topoInfo;
        final List<EntityReservedInstanceCoverage> coverage;
        synchronized(newLock) {
            if (!cachedRICoverage.containsKey(projectedTopologyId)) {
                projectedTopologyAvailable.put(projectedTopologyId, true);
                logger.debug("The projected topology {} in plan {} is available from repository",
                    projectedTopologyId, planId);
                return;
            } else {
                // if updateProjectedRICoverageTableForPlan is already being triggered and cached
                // we can remove it in the cachedRICoverage and trigger insertRecordsToTable
                topoInfo = cachedRICoverage.get(projectedTopologyId).getTopologyInfo();
                coverage = cachedRICoverage.get(projectedTopologyId).getCoverage();
            }
            logger.debug("The projected topology {} in plan {} written to repository from cache",
                projectedTopologyId, planId);
            insertRecordsToTable(projectedTopologyId, topoInfo, coverage);
            // clear the cache because the data is written.
            cachedRICoverage.remove(projectedTopologyId);
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
