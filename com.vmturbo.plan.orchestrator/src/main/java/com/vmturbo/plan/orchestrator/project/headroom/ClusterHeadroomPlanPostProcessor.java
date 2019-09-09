package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.springframework.util.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A post-processor to store cluster headroom for a particular cluster.
 */
@ThreadSafe
public class ClusterHeadroomPlanPostProcessor implements ProjectPlanPostProcessor {

    private static final Logger logger = LogManager.getLogger();

    private final long planId;

    /**
     * The clusters for which we're trying to calculate headroom.
     */
    private final List<Group> clusters;

    private final RepositoryServiceBlockingStub repositoryService;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    private final SupplyChainServiceBlockingStub supplyChainRpcService;

    private final GroupServiceGrpc.GroupServiceBlockingStub groupRpcService;

    private PlanDao planDao;

    private TemplatesDao templatesDao;

    private Consumer<ProjectPlanPostProcessor> onCompleteHandler;

    /**
     * Number of days to look back for vm growth from now.
     */
    private static final int PEAK_LOOK_BACK_DAYS = 7;

    /**
     * A constant holding a big number of days when exhaustion days is infinite
     */
    private static final int MORE_THAN_A_YEAR = 3650;

    // Milliseconds in a day
    private static final long DAY_MILLI_SECS = TimeUnit.DAYS.toMillis(1);

    /**
     * List of entities relevant for headroom calculation.
     */
    private static final Set<Integer> HEADROOM_ENTITY_TYPE =
                    ImmutableSet.of(EntityType.STORAGE_VALUE, EntityType.PHYSICAL_MACHINE_VALUE,
                                  EntityType.VIRTUAL_MACHINE_VALUE);

    /**
     * List of commodities used for CPU headroom calculation.
     */
    private static final Set<Integer> CPU_HEADROOM_COMMODITIES =
            ImmutableSet.of(CommodityType.CPU_VALUE, CommodityType.CPU_PROVISIONED_VALUE);

    /**
     * List of commodities used for Memory headroom calculation.
     */
    private static final Set<Integer> MEM_HEADROOM_COMMODITIES =
                    ImmutableSet.of(CommodityType.MEM_VALUE, CommodityType.MEM_PROVISIONED_VALUE);

    /**
     * List of commodities used for Storage headroom calculation.
     */
    private static final Set<Integer> STORAGE_HEADROOM_COMMODITIES =
            ImmutableSet.of(CommodityType.STORAGE_AMOUNT_VALUE, CommodityType.STORAGE_PROVISIONED_VALUE);

    private static final int DAYS_PER_MONTH = 30;

    /**
     * String representation of headroom entities.
     */
    private static final Set<String> HEADROOM_ENTITY_TYPES = ImmutableSet.of(StringConstants.VIRTUAL_MACHINE,
                    StringConstants.PHYSICAL_MACHINE, StringConstants.STORAGE);

    /**
     * Whether the actual calculation of headroom (kicked off by the projected topology being
     * available) has started.
     */
    private final AtomicBoolean calculationStarted = new AtomicBoolean(false);

    /**
     * Set default VM growth as 0. Use it in cases when growth can't be
     * calculated (due to insufficient history) or when it is negative.
     */
    private static final float DEFAULT_VM_GROWTH = 0.0f;

    public ClusterHeadroomPlanPostProcessor(final long planId,
                                            @Nonnull final Set<Long> clusterIds,
                                            @Nonnull final Channel repositoryChannel,
                                            @Nonnull final Channel historyChannel,
                                            @Nonnull final PlanDao planDao,
                                            @Nonnull final Channel groupChannel,
                                            @Nonnull TemplatesDao templatesDao) {
        this.planId = planId;
        this.repositoryService =
                RepositoryServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
        this.statsHistoryService =
                StatsHistoryServiceGrpc.newBlockingStub(Objects.requireNonNull(historyChannel));
        this.supplyChainRpcService =
                SupplyChainServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
        this.groupRpcService =
                GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.clusters = new ArrayList<>(clusterIds.size());
        groupRpcService.getGroups(
                GroupDTO.GetGroupsRequest.newBuilder().addAllId(clusterIds).build())
            .forEachRemaining(this.clusters::add);
    }

    @Override
    public long getPlanId() {
        return planId;
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) {
        if (plan.hasProjectedTopologyId()) {
            // We may have multiple updates to the plan status after the initial one that set
            // the projected topology. However, we only want to calculate headroom once.
            if (calculationStarted.compareAndSet(false, true)) {
                logger.info("Headroom calculation started");
                // This is all we need for post-processing, don't need to wait for plan to succeed.
                // Retrieve all projected entities in the plan topology.
                final Iterable<RetrieveTopologyResponse> topologyResponse = () ->
                    repositoryService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                            .setTopologyId(plan.getProjectedTopologyId())
                            .build());

                // Headroom entities filtered based on entity type.
                // Key is the oid of the TopologyEntityDTO. Value is the TopologyEntityDTO.
                final Map<Long, TopologyEntityDTO> oidToHeadroomEntity =
                    StreamSupport.stream(topologyResponse.spliterator(), false)
                        .flatMap(res -> res.getEntitiesList().stream())
                        .filter(entity -> HEADROOM_ENTITY_TYPE.contains(entity.getEntityType()))
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

                // Group entities into corresponding clusters.
                final Map<Long, ImmutableEntityCountData> entityCounts = new HashMap<>(clusters.size());
                final Map<Long, Map<Integer, List<TopologyEntityDTO>>> clusterHeadroomEntities =
                    new HashMap<>(clusters.size());
                clusters.forEach(cluster ->
                    clusterHeadroomEntities.put(cluster.getId(), new HashMap<>()));
                groupHeadroomEntities(oidToHeadroomEntity, entityCounts, clusterHeadroomEntities);

                // Get vmDailyGrowth of each cluster.
                final Map<Long, Float> clusterIdToVMDailyGrowth = getVMDailyGrowth(entityCounts);

                // Calculate headroom for each cluster.
                clusters.forEach(cluster -> {
                    final long clusterId = cluster.getId();
                    calculateHeadroomPerCluster(cluster, clusterHeadroomEntities.get(clusterId),
                        entityCounts.get(clusterId), clusterIdToVMDailyGrowth.get(clusterId));
                });
            }
        }

        // Wait until the plan completes - that is, until all pieces are finished processing -
        // to delete it, so that everything gets deleted properly.
        // In the future, we should be able to issue a delete to an in-progress plan and
        // have no orphaned data.
        String displayName = clusters.size() == 1 ?
            clusters.get(0).getCluster().getDisplayName() : "All";
        if (plan.getStatus() == PlanStatus.SUCCEEDED || plan.getStatus() == PlanStatus.FAILED
                || plan.getStatus() == PlanStatus.STOPPED) {
            if (plan.getStatus() == PlanStatus.FAILED) {
                logger.error("Cluster headroom plan for cluster {} failed! Error: {}",
                    displayName, plan.getStatusMessage());
            } else if (plan.getStatus() == PlanStatus.STOPPED) {
                logger.info("Cluster headroom plan for cluster {} was stopped!", displayName);
            } else {
                logger.info("Cluster headroom plan for cluster {} completed!", displayName);
            }

            try {
                planDao.deletePlan(plan.getPlanId());
            } catch (NoSuchObjectException e) {
                // This shouldn't happen because the plan must have existed in order to
                // have succeeded.
            } finally {
                if (onCompleteHandler != null) {
                    onCompleteHandler.accept(this);
                }
            }
        } else {
            // Do nothing.
            logger.info("Cluster headroom plan for cluster {} has new status: {}",
                displayName, plan.getStatus());
        }
    }

    /**
     * Group headroom entities into corresponding clusters and
     * get the number of VMs, hosts and Storages in each cluster.
     *
     * @param oidToHeadroomEntity projected entities in the plan topology
     * @param entityCounts key is the id of the cluster;
     *                     value is the number of VMs, PMs and Storages in this cluster
     * @param clusterHeadroomEntities key is the id of the cluster;
     *                                value is a map, whose key is the entity type,
     *                                value is all TopologyEntityDTOs of this entity type in this cluster
     */
    private void groupHeadroomEntities(
        @Nonnull final Map<Long, TopologyEntityDTO> oidToHeadroomEntity,
        @Nonnull final Map<Long, ImmutableEntityCountData> entityCounts,
        @Nonnull final Map<Long, Map<Integer, List<TopologyEntityDTO>>> clusterHeadroomEntities) {

        final GetMultiSupplyChainsRequest.Builder requestBuilder = GetMultiSupplyChainsRequest.newBuilder();
        clusters.forEach(cluster -> requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
            .setSeedOid(cluster.getId())
            .addAllStartingEntityOid(cluster.getCluster().getMembers().getStaticMemberOidsList())
            .addAllEntityTypesToInclude(HEADROOM_ENTITY_TYPES)));

        // Make a supply chain rpc call to fetch supply chain information, which is used to
        // determine which cluster a projected entity belongs to and
        // the number of VMs, PMs and Storages in this cluster.
        supplyChainRpcService.getMultiSupplyChains(requestBuilder.build())
            .forEachRemaining(supplyChainResponse -> {
                final long clusterId = supplyChainResponse.getSeedOid();
                if (supplyChainResponse.hasError()) {
                    logger.error("Failed to retrieve supply chain of cluster {} due to error: {}",
                        clusterId, supplyChainResponse.getError());
                }

                final SupplyChain supplyChain = supplyChainResponse.getSupplyChain();
                final int missingEntitiesCnt = supplyChain.getMissingStartingEntitiesCount();
                if (missingEntitiesCnt > 0) {
                    logger.warn("Supply chains of {} cluster members of cluster {} not found." +
                            " Missing members: {}", missingEntitiesCnt, clusterId,
                        supplyChain.getMissingStartingEntitiesList());
                }

                final Map<String, Long> entitiesByType = new HashMap<>();
                final Map<Integer, List<TopologyEntityDTO>> headroomEntities =
                    clusterHeadroomEntities.get(clusterId);
                HEADROOM_ENTITY_TYPE.forEach(type -> headroomEntities.put(type, new ArrayList<>()));
                supplyChain.getSupplyChainNodesList().stream()
                    .filter(node -> HEADROOM_ENTITY_TYPES.contains(node.getEntityType()))
                    .forEach(node -> {
                        node.getMembersByStateMap().values().stream()
                            .flatMap(memberList -> memberList.getMemberOidsList().stream())
                            .filter(oidToHeadroomEntity::containsKey)
                            .forEach(oid ->
                                headroomEntities.get(UIEntityType.fromString(node.getEntityType()).typeNumber())
                                    .add(oidToHeadroomEntity.get(oid)));

                        entitiesByType.put(node.getEntityType(),
                            entitiesByType.getOrDefault(node.getEntityType(), 0L)
                                + RepositoryDTOUtil.getMemberCount(node));
                    });

                entityCounts.put(clusterId, ImmutableEntityCountData.builder()
                    .numberOfVMs(entitiesByType.getOrDefault(StringConstants.VIRTUAL_MACHINE, 0L))
                    .numberOfHosts(entitiesByType.getOrDefault(StringConstants.PHYSICAL_MACHINE, 0L))
                    .numberOfStorages(entitiesByType.getOrDefault(StringConstants.STORAGE, 0L))
                    .build());
            });
    }

    /**
     * Get the averaged number of daily added VMs of each cluster since past lookBackDays (if applicable).
     * Fetch VM count from past lookBackDays to calculate growth compared to current VMs.
     *
     * @param currentEntityCounts key is the id of the cluster;
 *                                value is a map with current entity counts
 *                                of HEADROOM_ENTITY_TYPEs
     * @return vmDailyGrowth of each cluster
     */
    @VisibleForTesting
    Map<Long, Float> getVMDailyGrowth(
        @Nonnull  final Map<Long, ImmutableEntityCountData> currentEntityCounts) {
        final long currentTime = System.currentTimeMillis();
        final long lookbackDaysInMillis = currentTime - PEAK_LOOK_BACK_DAYS * DAY_MILLI_SECS;
        final Map<Long, Float> dailyVMGrowthPerCluster = new HashMap<>(currentEntityCounts.size());
        currentEntityCounts.forEach((clusterId, entityCounts) -> {
            ClusterStatsRequest vmCountRequest = ClusterStatsRequest.newBuilder()
                .setClusterId(clusterId)
                .setStats(StatsFilter.newBuilder()
                    .setStartDate(lookbackDaysInMillis)
                    .setEndDate(currentTime)
                    .addCommodityRequests(CommodityRequest.newBuilder()
                        .setCommodityName(StringConstants.VM_NUM_VMS)))
                .build();

            final Iterator<StatSnapshot> statSnapshotIterator = statsHistoryService.getClusterStats(vmCountRequest);
            Optional<StatSnapshot> earliestStatSnapshot = Optional.empty();
            while (statSnapshotIterator.hasNext()) {
                StatSnapshot currentSnapshot = statSnapshotIterator.next();
                if (earliestStatSnapshot.isPresent()) {
                    if(earliestStatSnapshot.get().getSnapshotDate() > currentSnapshot.getSnapshotDate()) {
                        earliestStatSnapshot = Optional.of(currentSnapshot);
                    }
                } else {
                    earliestStatSnapshot = Optional.of(currentSnapshot);
                }
            }

            if (!earliestStatSnapshot.isPresent()) {
                logger.info("No relevant VM stat snapshots available for cluster {}. Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                return;
            }

            Optional<StatRecord> vmCountRecord = earliestStatSnapshot.get().getStatRecordsList().stream()
                .filter(statRecord -> statRecord.getName().equals(StringConstants.VM_NUM_VMS))
                .findFirst();

            if (!vmCountRecord.isPresent()) {
                logger.info("No relevant VM count records found in history for cluster : {}." +
                    " Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                return;
            }

            long vmCountTime = earliestStatSnapshot.get().getSnapshotDate();
            Date vmCountDate = new Date(vmCountTime);
            float oldVMs = vmCountRecord.get().getValues().getAvg();
            long currentVMs = entityCounts.getNumberOfVMs();
            if (oldVMs > currentVMs) {
                logger.info("Negative VM growth for cluster {} : current VM count is {}" +
                    " and old VM count is {} recorded on {}. Setting 0 growth.", clusterId, currentVMs, oldVMs, vmCountDate);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                return;
            }

            long daysDifference = (currentTime - vmCountTime)/DAY_MILLI_SECS;
            if (daysDifference == 0) {
                logger.info("No older VM count data available for cluster {}. Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                return;
            }

            float dailyGrowth =  (currentVMs - oldVMs)/daysDifference;
            logger.debug("Daily VM growth for cluster {} is {}. New VM count {} and old VM count is {} recorded on {}",
                    clusterId, dailyGrowth, currentVMs, oldVMs, vmCountDate);
            dailyVMGrowthPerCluster.put(clusterId, dailyGrowth);
        });

        return dailyVMGrowthPerCluster;
    }

    /**
     * Calculate headroom for each cluster.
     *
     * @param cluster calculate headroom for this cluster
     * @param headroomEntities the VMs, hosts and Storages in this cluster
     * @param entityCounts the number of VMs, hosts and Storages in this cluster
     * @param vmDailyGrowth the averaged number of daily added VMs since past lookBackDays
     */
    private void calculateHeadroomPerCluster(
        @Nonnull final Group cluster,
        @Nonnull final Map<Integer, List<TopologyEntityDTO>> headroomEntities,
        @Nonnull final ImmutableEntityCountData entityCounts,
        final float vmDailyGrowth) {

        Optional<Template> template = templatesDao
            .getTemplate(cluster.getCluster().getClusterHeadroomTemplateId());

        if (!template.isPresent()) {
            logger.error("Template not found for : " + cluster.getCluster().getDisplayName() +
                " with template id : " + cluster.getCluster().getClusterHeadroomTemplateId());
            return;
        }

        // Map of commodities bought by template per relevant headroom commodities
        // set of CPU, MEM and Storage.
        Map<Set<Integer>, Map<Integer, Double>> commoditiesBoughtByTemplate =
            getCommoditiesBoughtByTemplate(template.get());

        CommodityHeadroom cpuHeadroom = calculateHeadroom(cluster,
            headroomEntities.get(EntityType.PHYSICAL_MACHINE_VALUE),
            commoditiesBoughtByTemplate.get(CPU_HEADROOM_COMMODITIES), vmDailyGrowth);
        CommodityHeadroom memHeadroom = calculateHeadroom(cluster,
            headroomEntities.get(EntityType.PHYSICAL_MACHINE_VALUE),
            commoditiesBoughtByTemplate.get(MEM_HEADROOM_COMMODITIES), vmDailyGrowth);
        CommodityHeadroom storageHeadroom = calculateHeadroom(cluster,
            headroomEntities.get(EntityType.STORAGE_VALUE),
            commoditiesBoughtByTemplate.get(STORAGE_HEADROOM_COMMODITIES), vmDailyGrowth);

        createStatsRecords(cluster.getId(), entityCounts.getNumberOfVMs(),
            entityCounts.getNumberOfHosts(), entityCounts.getNumberOfStorages(),
            cpuHeadroom, memHeadroom, storageHeadroom, getMonthlyVMGrowth(vmDailyGrowth));
    }

    /**
     * Parse given template to return commodities values grouped by specific headroom commodity types.
     * For example : For CPU_HEADROOM_COMMODITIES(CPU, CPU_PROV) Returned map has
     * key (Set<CPU_HEADROOM_COMMODITIES>) -> value (Map : key<Commodity_Type> -> Value<UsedValue>) calculated
     * by parsing template fields specific to system load.
     *
     * @param headroomTemplate to parse.
     * @return Map<Set<Integer>, Map<Integer, Double>> which has for example :
     * key (Set<CPU_HEADROOM_COMMODITIES>) -> value (Map : key<Commodity_Type> -> Value<UsedValue>)
     */
    private Map<Set<Integer>, Map<Integer, Double>> getCommoditiesBoughtByTemplate(
        Template headroomTemplate) {
        Map<Set<Integer>, Map<Integer, Double>> commBoughtMap = new HashMap<>();
        commBoughtMap.put(CPU_HEADROOM_COMMODITIES, new HashMap<>());
        commBoughtMap.put(MEM_HEADROOM_COMMODITIES, new HashMap<>());
        commBoughtMap.put(STORAGE_HEADROOM_COMMODITIES, new HashMap<>());
        Map<String, String> templateFields = getFieldNameValueMap(headroomTemplate);

        if (CollectionUtils.isEmpty(templateFields)) {
            return commBoughtMap;
        }

        // Set CPU_HEADROOM_COMMODITIES
        commBoughtMap.get(CPU_HEADROOM_COMMODITIES)
            .put(CommodityType.CPU_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.CPU_SPEED)) *
                    Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.NUM_OF_CPU)) *
                    Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.CPU_CONSUMED_FACTOR)));
        commBoughtMap.get(CPU_HEADROOM_COMMODITIES)
            .put(CommodityType.CPU_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.NUM_OF_CPU)) *
                    Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.CPU_SPEED)));

        // Set MEM_HEADROOM_COMMODITIES
        commBoughtMap.get(MEM_HEADROOM_COMMODITIES)
            .put(CommodityType.MEM_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.MEMORY_SIZE)) *
                    Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.MEMORY_CONSUMED_FACTOR)));
        commBoughtMap.get(MEM_HEADROOM_COMMODITIES)
            .put(CommodityType.MEM_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.MEMORY_SIZE)));

        // Set STORAGE_HEADROOM_COMMODITIES
        commBoughtMap.get(STORAGE_HEADROOM_COMMODITIES)
            .put(CommodityType.STORAGE_AMOUNT_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.DISK_SIZE)) *
                    Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.DISK_CONSUMED_FACTOR)));
        commBoughtMap.get(STORAGE_HEADROOM_COMMODITIES)
            .put(CommodityType.STORAGE_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.DISK_SIZE)));
        if (logger.isTraceEnabled()) {
            logger.trace("Template name: {}", headroomTemplate.getTemplateInfo().getName());
            logger.trace("Template fields: {}", templateFields.toString());
            logger.trace("Commodities bought by template: {}", commBoughtMap.toString());
        }
        return commBoughtMap;
    }

    /**
     * Calculate :
     * 1) Available Headroom : number of VMs that can be accommodated in cluster
     *                         considering its utilization for given commodity.
     * 2) Empty Headroom : number of VMs that can be accommodated in cluster
     *                     for given commodity when cluster is empty.
     * 3) DaysToExhaustion : calculated based on calculated headroom and given vmGrowth.
     *
     * @param cluster calculate headroom for this cluster
     * @param entities relevant for headroom calculation
     * @param headroomCommodities for which headroom is calculated
     * @param vmDailyGrowth the averaged number of daily added VMs since past lookBackDays
     * @return computed headroom data
     */
    private CommodityHeadroom calculateHeadroom(
        @Nonnull final Group cluster,
        @Nonnull final Collection<TopologyEntityDTO> entities,
        @Nonnull final Map<Integer, Double> headroomCommodities,
        final float vmDailyGrowth) {

        if (CollectionUtils.isEmpty(entities) || CollectionUtils.isEmpty(headroomCommodities)) {
            return CommodityHeadroom.getDefaultInstance();
        }

        Map<Integer, Double> commHeadroomAvailable = new HashMap<Integer, Double>();
        Map<Integer, Double> commHeadroomCapacity = new HashMap<Integer, Double>();
        // Number of VMs that can be accommodated in cluster considering its curr ent utilization.
        long totalHeadroomAvailable = 0;
        // Number of VMs that can be accommodated in cluster when cluster is empty.
        long totalHeadroomCapacity = 0;
        // Iterate over each entity and find number of VMs that fit providers.
        for (TopologyEntityDTO entity : entities) {
            // If entity is not powered on, don't use it for headroom calculation.
            if (!entity.hasEntityState() ||
                !(entity.getEntityState() == EntityState.POWERED_ON)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skip entity {} ({}) because it's not powered on.",
                        entity.getDisplayName(), entity.getOid());
                }
                continue;
            }
            for (CommoditySoldDTO comm : entity.getCommoditySoldListList()) {
                int commType = comm.getCommodityType().getType();
                double headroomAvailable = 0d;
                double headroomCapacity  = 0d;
                double templateCommodityUsed = headroomCommodities.getOrDefault(commType, 0D);
                if (templateCommodityUsed == 0) {
                    continue;
                }
                // Set effective capacity
                double capacity = (comm.getEffectiveCapacityPercentage() / 100) * comm.getCapacity();
                double availableAmount =  capacity - comm.getUsed();
                headroomAvailable = availableAmount > 0 ?
                                Math.floor(availableAmount / templateCommodityUsed) : 0;
                headroomCapacity = Math.floor(capacity / templateCommodityUsed);
                commHeadroomAvailable.put(commType, headroomAvailable);
                commHeadroomCapacity.put(commType, headroomCapacity);
            }

            if (CollectionUtils.isEmpty(commHeadroomAvailable) ||
                            CollectionUtils.isEmpty(commHeadroomCapacity)) {
                logger.error("Template has used value 0 for some commodities in cluster : " +
                    cluster.getCluster().getDisplayName() +" and id "+ cluster.getId());
                return CommodityHeadroom.getDefaultInstance();
            }

            final double headroomAvailableForCurrentEntity = commHeadroomAvailable.values().stream().min(Double::compare).get();
            final double headroomCapacityForCurrentEntity = commHeadroomCapacity.values().stream().min(Double::compare).get();

            // prevent overflow here, Integer.MAX_VALUE means that there is no limit, e.g.
            // VMs do not consume storage, so storage headroom is unlimited
            if (totalHeadroomAvailable != Long.MAX_VALUE) {
                totalHeadroomAvailable += headroomAvailableForCurrentEntity;
            }

            if (totalHeadroomCapacity != Long.MAX_VALUE) {
                totalHeadroomCapacity += headroomCapacityForCurrentEntity;
            }
        }
        return CommodityHeadroom.newBuilder()
                    .setHeadroom(totalHeadroomAvailable)
                    .setCapacity(totalHeadroomCapacity)
                    .setDaysToExhaustion(getDaysToExhaustion(vmDailyGrowth, totalHeadroomAvailable))
                    .build();
    }

    /**
     * Calculate days to exhaustion by using vmGrowth rate
     * and current headroom availability.
     *
     * @param vmDailyGrowth growth of VMs in one day
     * @param headroomAvailable current headroom availability.
     * @return days to exhaustion.
     */
    private long getDaysToExhaustion(float vmDailyGrowth, long headroomAvailable) {
        //if headroom == 0 - cluster is already exhausted
        if (headroomAvailable == 0) {
            return 0;
        }
        //if headroom is infinite OR VM Growth is 0  - exhaustion time is infinite
        if (headroomAvailable == Long.MAX_VALUE || vmDailyGrowth == DEFAULT_VM_GROWTH
                || vmDailyGrowth == 0) {
            return MORE_THAN_A_YEAR;
        }
        return (long) Math.floor((float)headroomAvailable / vmDailyGrowth);
    }

    /**
     * Get all template fields from template resources and generate a mapping from template field name
     * to template field value.
     *
     * @param template tp parse.
     * @return A Map which key is template field name and value is template field value.
     */
    private Map<String, String> getFieldNameValueMap(
            @Nonnull final Template template) {
        return template.getTemplateInfo().getResourcesList().stream()
            .flatMap(resources -> resources.getFieldsList().stream())
            .collect(Collectors.toMap(TemplateField::getName, TemplateField::getValue));
    }

    /**
     * Projects VM growth to a monthly value.
     *
     * @param vmDailyGrowth the number of daily added VMs
     * @return the number of monthly added VMs
     */
    private long getMonthlyVMGrowth(final float vmDailyGrowth) {
        return (long) Math.ceil(DAYS_PER_MONTH * vmDailyGrowth);
    }

    /**
     * Save the headroom value.
     *
     * @param clusterId cluster id
     * @param numVms number of vms
     * @param numHosts number of hosts
     * @param numStorages number of storages
     * @param cpuHeadroomInfo headroom values for CPU
     * @param memHeadroomInfo headroom values for Memory
     * @param storageHeadroomInfo headroom values for Storage
     * @param monthlyVMGrowth the number of monthly added VMs
     */
    private void createStatsRecords(final long clusterId, final long numVms,
                            final long numHosts, final long numStorages,
                            CommodityHeadroom cpuHeadroomInfo,
                            CommodityHeadroom memHeadroomInfo,
                            CommodityHeadroom storageHeadroomInfo,
                            final long monthlyVMGrowth) {
        long minHeadroom = Stream.of(cpuHeadroomInfo.getHeadroom(), memHeadroomInfo.getHeadroom(), storageHeadroomInfo.getHeadroom())
            .min(Comparator.comparing(Long::valueOf))
            // Ideally this should never happen but if it does we are logging it before writing to db.
            .orElse(-1L);
        // Save the headroom in the history component.
        try {
            statsHistoryService.saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                .setClusterId(clusterId)
                .setNumVMs(numVms)
                .setNumHosts(numHosts)
                .setNumStorages(numStorages)
                .setHeadroom(minHeadroom)
                .setCpuHeadroomInfo(cpuHeadroomInfo)
                .setMemHeadroomInfo(memHeadroomInfo)
                .setStorageHeadroomInfo(storageHeadroomInfo)
                .setMonthlyVMGrowth(monthlyVMGrowth)
                .build());
        } catch (StatusRuntimeException e) {
            logger.error("Failed to save cluster headroom: {}", e.getMessage());
        }
    }

    @Override
    public void registerOnCompleteHandler(final Consumer<ProjectPlanPostProcessor> handler) {
        this.onCompleteHandler = handler;
    }

    @Value.Immutable
    interface EntityCountData {
        long getNumberOfVMs();
        long getNumberOfHosts();
        long getNumberOfStorages();
    }
}
