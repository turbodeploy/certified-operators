package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.turbonomic.cpucapacity.CPUCapacityEstimator;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequestForHeadroomPlan;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A post-processor to store cluster headroom for a particular cluster.
 */
@ThreadSafe
public class ClusterHeadroomPlanPostProcessor implements ProjectPlanPostProcessor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * List of commodities used for CPU headroom calculation.
     */
    public static final Set<Integer> CPU_HEADROOM_COMMODITIES = ImmutableSet.of(
        CommodityType.CPU_VALUE, CommodityType.CPU_PROVISIONED_VALUE);

    /**
     * List of commodities used for Memory headroom calculation.
     */
    public static final Set<Integer> MEM_HEADROOM_COMMODITIES = ImmutableSet.of(
        CommodityType.MEM_VALUE, CommodityType.MEM_PROVISIONED_VALUE);

    /**
     * List of commodities used for Storage headroom calculation.
     */
    public static final Set<Integer> STORAGE_HEADROOM_COMMODITIES = ImmutableSet.of(
        CommodityType.STORAGE_AMOUNT_VALUE, CommodityType.STORAGE_PROVISIONED_VALUE);

    /**
     * List of commodities used for headroom calculation.
     */
    public static final Set<Integer> HEADROOM_COMMODITIES;

    static {
        HEADROOM_COMMODITIES = ImmutableSet.<Integer>builder()
            .addAll(CPU_HEADROOM_COMMODITIES)
            .addAll(MEM_HEADROOM_COMMODITIES)
            .addAll(STORAGE_HEADROOM_COMMODITIES).build();
    }

    private final long planId;

    /**
     * The clusters for which we're trying to calculate headroom.
     */
    private final List<Grouping> clusters;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    private final SupplyChainServiceBlockingStub supplyChainRpcService;

    private final GroupServiceBlockingStub groupRpcService;

    private final SettingServiceBlockingStub settingService;

    private final PlanDao planDao;

    private final TemplatesDao templatesDao;

    private final CPUCapacityEstimator cpuCapacityEstimator;

    private Consumer<ProjectPlanPostProcessor> onCompleteHandler;

    /**
     * A constant holding a big number of days when exhaustion days is infinite.
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

    private static final int DAYS_PER_MONTH = 30;

    /**
     * String representation of headroom entities.
     */
    private static final Set<String> HEADROOM_ENTITY_TYPES = ImmutableSet.of(StringConstants.VIRTUAL_MACHINE,
        StringConstants.PHYSICAL_MACHINE, StringConstants.STORAGE);

    /**
     * Set default VM growth as 0. Use it in cases when growth can't be
     * calculated (due to insufficient history) or when it is negative.
     */
    private static final float DEFAULT_VM_GROWTH = 0.0f;

    /**
     * Constructor for the post-processor. The post-processor is for a specific plan.
     *
     * @param planId The ID of the plan.
     * @param clusterIds The ID of the clusters the plan is running on.
     * @param repositoryChannel Access to the repository's gRPC services.
     * @param historyChannel Access to history's gRPC services.
     * @param planDao Access to plan instances.
     * @param groupChannel Access to group's gRPC services.
     * @param templatesDao Access to templates.
     * @param cpuCapacityEstimator estimates the scaling factor of a cpu model.
     */
    public ClusterHeadroomPlanPostProcessor(final long planId,
                                            @Nonnull final Set<Long> clusterIds,
                                            @Nonnull final Channel repositoryChannel,
                                            @Nonnull final Channel historyChannel,
                                            @Nonnull final PlanDao planDao,
                                            @Nonnull final Channel groupChannel,
                                            @Nonnull final TemplatesDao templatesDao,
                                            @Nonnull final CPUCapacityEstimator cpuCapacityEstimator) {
        this.planId = planId;
        this.statsHistoryService =
            StatsHistoryServiceGrpc.newBlockingStub(Objects.requireNonNull(historyChannel));
        this.supplyChainRpcService =
            SupplyChainServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
        this.groupRpcService =
            GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.cpuCapacityEstimator = Objects.requireNonNull(cpuCapacityEstimator);
        this.clusters = new ArrayList<>(clusterIds.size());
        groupRpcService.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addAllId(clusterIds))
            .build())
            .forEachRemaining(this.clusters::add);
    }

    @Override
    public long getPlanId() {
        return planId;
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) {
        // Wait until the plan completes - that is, until all pieces are finished processing -
        // to delete it, so that everything gets deleted properly.
        // In the future, we should be able to issue a delete to an in-progress plan and
        // have no orphaned data.
        String displayName = clusters.size() == 1
            ? clusters.get(0).getDefinition().getDisplayName() : "All";
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

    @Override
    public void onPlanDeleted(@Nonnull final PlanInstance plan) {
        if (!PlanDTOUtil.isTerminalStatus(plan.getStatus())) {
            // Got deleted while in progress, probably via user intervention via the
            // command line/a manual gRPC call directly to the Plan Orchestrator.
            logger.warn("Cluster headroom plan {} deleted while in progress.", plan);
            if (onCompleteHandler != null) {
                onCompleteHandler.accept(this);
            }
        }
    }

    private void doHeadroomCalculation(@Nonnull final Map<Long, HeadroomEntity> entitiesById) {
        // Group headroom entities by cluster and entity type.
        final Map<Long, Map<Integer, List<HeadroomEntity>>> entityOidsByClusterAndType =
            groupHeadroomEntities(entitiesById);

        // Get vmDailyGrowth of each cluster.
        final Map<Long, Float> clusterIdToVMDailyGrowth =
            getVMDailyGrowth(entityOidsByClusterAndType.keySet());

        // Calculate headroom for each cluster.
        for (Grouping cluster : clusters) {
            if (!entityOidsByClusterAndType.containsKey(cluster.getId())) {
                continue;
            }

            try {
                final Map<Integer, List<HeadroomEntity>> entitiesByType =
                    entityOidsByClusterAndType.get(cluster.getId());
                // Fill in nonexistent headroom entity type to avoid NPE when calling get method
                // because it's possible that there's no VM in a cluster.
                HEADROOM_ENTITY_TYPE.stream().filter(type -> !entitiesByType.containsKey(type)).forEach(
                    type -> entitiesByType.put(type, Collections.emptyList()));

                // Calculate headroom for this cluster.
                calculateHeadroomPerCluster(cluster, entitiesByType,
                    clusterIdToVMDailyGrowth.get(cluster.getId()));
            } catch (RuntimeException e) {
                logger.error("Error in calculating headroom for cluster "
                    + cluster.getDefinition().getDisplayName() + "(" + cluster.getId() + ")", e);
            }
        }
    }

    /**
     * Group headroom entities by cluster and entity type.
     *
     * @param oidToEntities a mapping from entity oid to {@link HeadroomEntity}
     * @return a mapping from cluster id to entity type to {@link HeadroomEntity}
     */
    private Map<Long, Map<Integer, List<HeadroomEntity>>> groupHeadroomEntities(
        @Nonnull final Map<Long, HeadroomEntity> oidToEntities) {
        final Map<Long, Map<Integer, List<HeadroomEntity>>> entitiesByClusterAndType =
            new HashMap<>(clusters.size());

        final GetMultiSupplyChainsRequest.Builder requestBuilder = GetMultiSupplyChainsRequest.newBuilder();
        clusters.stream()
            // Skip empty cluster
            .filter(cluster -> {
                if (GroupProtoUtil.getAllStaticMembers(cluster.getDefinition()).isEmpty()) {
                    entitiesByClusterAndType.put(cluster.getId(), new HashMap<>());
                    return false;
                } else {
                    return true;
                }
            })
            .forEach(cluster -> requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
                .setSeedOid(cluster.getId())
                .setScope(SupplyChainScope.newBuilder()
                    .addAllStartingEntityOid(GroupProtoUtil.getAllStaticMembers(cluster.getDefinition()))
                    .addAllEntityTypesToInclude(HEADROOM_ENTITY_TYPES))));

        // Make a supply chain rpc call to fetch supply chain information, which is used to
        // determine which cluster a projected entity belongs to.
        //
        // Note - we do the supply chain call using the LIVE topology, because cluster membership
        // doesn't change in a headroom plan, and using the live topology is much faster.
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
                    logger.warn("Supply chains of {} cluster members of cluster {} not found."
                            + " Missing members: {}", missingEntitiesCnt, clusterId,
                        supplyChain.getMissingStartingEntitiesList());
                }

                final Map<Integer, List<HeadroomEntity>> entitiesByType = new HashMap<>(3);
                HEADROOM_ENTITY_TYPE.forEach(type -> entitiesByType.put(type, new ArrayList<>()));
                supplyChain.getSupplyChainNodesList().stream()
                    .filter(node -> HEADROOM_ENTITY_TYPES.contains(node.getEntityType()))
                    .forEach(node -> node.getMembersByStateMap().values().stream()
                        .flatMap(memberList -> memberList.getMemberOidsList().stream())
                        .filter(oidToEntities::containsKey)
                        .forEach(oid ->
                            entitiesByType.get(ApiEntityType.fromString(node.getEntityType()).typeNumber())
                                .add(oidToEntities.get(oid))));

                entitiesByClusterAndType.put(clusterId, entitiesByType);
            });

        return entitiesByClusterAndType;
    }

    /**
     * Get the averaged number of daily added VMs of each cluster since past lookBackDays (if applicable).
     * Fetch VM count from past lookBackDays to calculate growth compared to current VMs.
     *
     * @param clusterIds a set of clusterIds
     * @return vmDailyGrowth of each cluster
     */
    @VisibleForTesting
    Map<Long, Float> getVMDailyGrowth(@Nonnull final Set<Long> clusterIds) {
        final GetGlobalSettingResponse response = settingService.getGlobalSetting(
            GetSingleGlobalSettingRequest.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.MaxVMGrowthObservationPeriod.getSettingName())
                .build());
        // Number of months to look back to calculate VM growth.
        final int lookBackMonths;
        if (response.hasSetting()) {
            lookBackMonths = (int)response.getSetting().getNumericSettingValue().getValue();
        } else {
            lookBackMonths = (int)GlobalSettingSpecs.MaxVMGrowthObservationPeriod.createSettingSpec()
                .getNumericSettingValueType().getDefault();
        }
        logger.info("Use past {} months' data for VM growth calculation.", lookBackMonths);

        final Calendar calendar = Calendar.getInstance();
        final long currentTime = calendar.getTimeInMillis();
        calendar.add(Calendar.MONTH, -lookBackMonths);

        final Map<Long, Float> dailyVMGrowthPerCluster = new HashMap<>(clusterIds.size());
        for (final long clusterId : clusterIds) {
            final ClusterStatsRequestForHeadroomPlan vmCountRequest =
                ClusterStatsRequestForHeadroomPlan.newBuilder()
                    .setClusterId(clusterId)
                    .setStats(StatsFilter.newBuilder()
                        .setStartDate(calendar.getTimeInMillis())
                        .setEndDate(currentTime)
                        .addCommodityRequests(CommodityRequest.newBuilder()
                            .setCommodityName(StringConstants.NUM_VMS)))
                    .build();
            final Iterator<StatSnapshot> snapshotIterator = statsHistoryService
                .getClusterStatsForHeadroomPlan(vmCountRequest);

            Optional<StatSnapshot> earliestSnapshot = Optional.empty();
            Optional<StatSnapshot> latestSnapshot = Optional.empty();
            while (snapshotIterator.hasNext()) {
                final StatSnapshot currentSnapshot = snapshotIterator.next();
                if (currentSnapshot.getSnapshotDate()
                    < earliestSnapshot.map(StatSnapshot::getSnapshotDate).orElse(Long.MAX_VALUE)) {
                    earliestSnapshot = Optional.of(currentSnapshot);
                }
                if (currentSnapshot.getSnapshotDate()
                    > latestSnapshot.map(StatSnapshot::getSnapshotDate).orElse(Long.MIN_VALUE)) {
                    latestSnapshot = Optional.of(currentSnapshot);
                }
            }

            if (!earliestSnapshot.isPresent() || !latestSnapshot.isPresent()) {
                logger.info("No relevant VM stat snapshots available for cluster {}. Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                continue;
            }

            final Optional<Float> earliestVMCount = earliestSnapshot.get().getStatRecordsList().stream()
                .filter(statRecord -> statRecord.getName().equals(StringConstants.NUM_VMS))
                .map(statRecord -> statRecord.getValues().getAvg())
                .findFirst();
            final Optional<Float> latestVMCount = latestSnapshot.get().getStatRecordsList().stream()
                .filter(statRecord -> statRecord.getName().equals(StringConstants.NUM_VMS))
                .map(statRecord -> statRecord.getValues().getAvg())
                .findFirst();

            if (!earliestVMCount.isPresent() || !latestVMCount.isPresent()) {
                logger.info("No relevant VM count records found in history for cluster : {}."
                    + " Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                continue;
            }

            final long earliestDate = earliestSnapshot.get().getSnapshotDate();
            final long latestDate = latestSnapshot.get().getSnapshotDate();
            logger.debug("Use data from {} to {} to calculate VMGrowth.", earliestDate, latestDate);

            if (earliestVMCount.get() > latestVMCount.get()) {
                logger.info("Negative VM growth for cluster {} : latest VM count is {}"
                        + " and earliest VM count is {}. Setting 0 growth.",
                    clusterId, earliestVMCount.get(), latestVMCount.get());
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                continue;
            }

            final long daysDifference = (latestDate - earliestDate) / DAY_MILLI_SECS;
            if (daysDifference == 0) {
                logger.info("No older VM count data available for cluster {}. Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                continue;
            }

            float dailyGrowth =  (latestVMCount.get() - earliestVMCount.get()) / daysDifference;
            logger.debug("Daily VM growth for cluster {} is {}. New VM count {} and old VM count is {}",
                clusterId, dailyGrowth, latestVMCount.get(), earliestVMCount.get());
            dailyVMGrowthPerCluster.put(clusterId, dailyGrowth);
        }

        return dailyVMGrowthPerCluster;
    }

    /**
     * Calculate headroom for each cluster.
     *
     * @param cluster calculate headroom for this cluster
     * @param headroomEntities the VMs, hosts and Storages in this cluster
     * @param vmDailyGrowth the averaged number of daily added VMs since past lookBackDays
     */
    private void calculateHeadroomPerCluster(
            @Nonnull final Grouping cluster,
            @Nonnull final Map<Integer, List<HeadroomEntity>> headroomEntities,
            final float vmDailyGrowth) {

        Optional<Template> template =
            templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId());
        if (template.isPresent()) {
            logger.info("Calculating headroom for cluster {} ({}) using template {} ({}).",
                cluster.getDefinition().getDisplayName(), cluster.getId(),
                template.get().getTemplateInfo().getName(), template.get().getId());
        } else {
            logger.error("Template not found for cluster {} ({}).",
                cluster.getDefinition().getDisplayName(), cluster.getId());
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

        createStatsRecords(cluster.getId(),
            cpuHeadroom, memHeadroom, storageHeadroom, getMonthlyVMGrowth(vmDailyGrowth));
    }

    /**
     * Parse given template to return commodities values grouped by specific headroom commodity types.
     * For example : For CPU_HEADROOM_COMMODITIES(CPU, CPU_PROV) Returned map has
     * key (Set of CPU_HEADROOM_COMMODITIES) -> value (Map : Commodity_Type -> UsedValue) calculated
     * by parsing template fields specific to system load.
     *
     * @param headroomTemplate to parse.
     * @return Set of commodities -> commodity type -> used value.
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
        final double cpuScalingFactor;
        if (headroomTemplate.hasTemplateInfo()
                && headroomTemplate.getTemplateInfo().hasCpuModel()
                && StringUtils.isNotBlank(headroomTemplate.getTemplateInfo().getCpuModel())) {
            String cpuModel = headroomTemplate.getTemplateInfo().getCpuModel();
            cpuScalingFactor = cpuCapacityEstimator.estimateMHzCoreMultiplier(cpuModel);
            logger.debug("headroom template with oid {} has cpu model {} with scaling factor {}",
                headroomTemplate.getId(), cpuModel, cpuScalingFactor);
        } else {
            cpuScalingFactor = 1.0;
            logger.warn("headroom template with oid {} did not have a cpu model. "
                + "falling back to 1.0 scaling factor."
                + " hasTemplateInfo={}, hasCpuModel={}, isNotBlank={}",
                headroomTemplate.getId(),
                headroomTemplate.hasTemplateInfo(),
                headroomTemplate.hasTemplateInfo()
                    && headroomTemplate.getTemplateInfo().hasCpuModel(),
                headroomTemplate.hasTemplateInfo()
                    && headroomTemplate.getTemplateInfo().hasCpuModel()
                    && StringUtils.isNotBlank(headroomTemplate.getTemplateInfo().getCpuModel()));
        }
        commBoughtMap.get(CPU_HEADROOM_COMMODITIES)
            .put(CommodityType.CPU_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED))
                    * Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU))
                    * Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR))
                    * cpuScalingFactor);
        commBoughtMap.get(CPU_HEADROOM_COMMODITIES)
            .put(CommodityType.CPU_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU))
                    * Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED))
                    * cpuScalingFactor);

        // Set MEM_HEADROOM_COMMODITIES
        commBoughtMap.get(MEM_HEADROOM_COMMODITIES)
            .put(CommodityType.MEM_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE))
                    * Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR)));
        commBoughtMap.get(MEM_HEADROOM_COMMODITIES)
            .put(CommodityType.MEM_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE)));

        // Set STORAGE_HEADROOM_COMMODITIES
        commBoughtMap.get(STORAGE_HEADROOM_COMMODITIES)
            .put(CommodityType.STORAGE_AMOUNT_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_STORAGE_DISK_SIZE))
                    * Double.valueOf(templateFields.get(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR)));
        commBoughtMap.get(STORAGE_HEADROOM_COMMODITIES)
            .put(CommodityType.STORAGE_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_STORAGE_DISK_SIZE)));
        if (logger.isTraceEnabled()) {
            logger.trace("Template name: {}, id: {}",
                headroomTemplate.getTemplateInfo().getName(), headroomTemplate.getId());
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
        @Nonnull final Grouping cluster,
        @Nonnull final Collection<HeadroomEntity> entities,
        @Nonnull final Map<Integer, Double> headroomCommodities,
        final float vmDailyGrowth) {

        if (CollectionUtils.isEmpty(entities) || CollectionUtils.isEmpty(headroomCommodities)) {
            return CommodityHeadroom.getDefaultInstance();
        }

        // Number of VMs that can be accommodated in cluster considering its curr ent utilization.
        long totalHeadroomAvailable = 0;
        // Number of VMs that can be accommodated in cluster when cluster is empty.
        long totalHeadroomCapacity = 0;
        // Iterate over each entity and find number of VMs that fit providers.
        for (HeadroomEntity entity : entities) {
            // If entity is not powered on, don't use it for headroom calculation.
            if (!(entity.getEntityState() == EntityState.POWERED_ON)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skip entity {} ({}) because it's not powered on.",
                        entity.getDisplayName(), entity.getOid());
                }
                continue;
            }
            double headroomAvailableForCurrentEntity = Double.MAX_VALUE;
            double headroomCapacityForCurrentEntity = Double.MAX_VALUE;
            int minAvailableCommodity = -1;
            int minCapacityCommodity = -1;
            for (CommoditySoldDTO comm : entity.getCommoditySoldList()) {
                int commType = comm.getCommodityType().getType();
                double templateCommodityUsed = headroomCommodities.getOrDefault(commType, 0D);
                if (templateCommodityUsed == 0) {
                    continue;
                }
                if (!comm.hasCapacity() || comm.getCapacity() <= 0) {
                    continue;
                }
                // Set effective capacity
                double capacity = comm.getScalingFactor()
                    * ((comm.getEffectiveCapacityPercentage() / 100) * comm.getCapacity());
                double used = comm.getScalingFactor() * comm.getUsed();
                double availableAmount =  capacity - used;

                if (logger.isTraceEnabled()) {
                    logger.trace("Commodity sold {} of {} has used/capacity ({}/{}) with scaling factor {}",
                        CommodityDTO.CommodityType.forNumber(commType).name(),
                        entity.getDisplayName(), used, capacity, comm.getScalingFactor());
                }

                double headroomAvailable = availableAmount > 0
                    ? Math.floor(availableAmount / templateCommodityUsed) : 0;
                if (headroomAvailable < headroomAvailableForCurrentEntity) {
                    headroomAvailableForCurrentEntity = headroomAvailable;
                    minAvailableCommodity = commType;
                }

                double headroomCapacity  = 0d;
                headroomCapacity = Math.floor(capacity / templateCommodityUsed);
                if (headroomCapacity < headroomCapacityForCurrentEntity) {
                    headroomCapacityForCurrentEntity = headroomCapacity;
                    minCapacityCommodity = commType;
                }
            }

            if (minAvailableCommodity == -1 || minCapacityCommodity == -1) {
                logger.error("Template has used value 0 for some commodities in cluster : "
                    + cluster.getDefinition().getDisplayName() + " and id " + cluster.getId());
                return CommodityHeadroom.getDefaultInstance();
            }

            if (logger.isTraceEnabled()) {
                final String minAvailableCommodityName = CommodityDTO.CommodityType.forNumber(minAvailableCommodity).name();
                logger.trace("Available headroom for entity {} regarding the commodity type {} is {}",
                    entity.getDisplayName(), minAvailableCommodityName, headroomAvailableForCurrentEntity);

                final String minCapacityCommodityName = CommodityDTO.CommodityType.forNumber(minCapacityCommodity).name();
                logger.trace("Headroom capacity for entity {} regarding the commodity type {} is {}",
                    entity.getDisplayName(), minCapacityCommodityName, headroomCapacityForCurrentEntity);
            }

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
        return (long)Math.floor(headroomAvailable / vmDailyGrowth);
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
        return (long)Math.ceil(DAYS_PER_MONTH * vmDailyGrowth);
    }

    /**
     * Save the headroom value.
     *
     * @param clusterId cluster id
     * @param cpuHeadroomInfo headroom values for CPU
     * @param memHeadroomInfo headroom values for Memory
     * @param storageHeadroomInfo headroom values for Storage
     * @param monthlyVMGrowth the number of monthly added VMs
     */
    private void createStatsRecords(final long clusterId,
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

    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo sourceTopologyInfo) {
        // Applies to any projected topology in a cluster headroom plan.
        return sourceTopologyInfo.getPlanInfo().getPlanProjectType() == PlanProjectType.CLUSTER_HEADROOM;
    }

    @Override
    public void handleProjectedTopology(final long projectedTopologyId,
                                        @Nonnull final TopologyInfo sourceTopologyInfo,
                                        @Nonnull final RemoteIterator<ProjectedTopologyEntity> iterator)
        throws InterruptedException, TimeoutException, CommunicationException {
        final Map<Long, HeadroomEntity> entities = new HashMap<>();
        while (iterator.hasNext()) {
            iterator.nextChunk().stream()
                .filter(projectedTopologyEntity -> HEADROOM_ENTITY_TYPE.contains(projectedTopologyEntity.getEntity().getEntityType()))
                .map(projectedTopologyEntity -> new HeadroomEntity(projectedTopologyEntity.getEntity()))
                .forEach(entity -> entities.put(entity.getOid(), entity));
        }
        doHeadroomCalculation(entities);
    }

    /**
     * A skinny entity that contains the necessary information to do headroom calculations.
     * Used so that we don't need to keep entire {@link TopologyEntityDTO}s in memory.
     */
    private static class HeadroomEntity {

        private final long oid;
        private final String displayName;
        private final int entityType;
        private final EntityState entityState;
        private final List<CommoditySoldDTO> commsSold;

        private HeadroomEntity(TopologyEntityDTO topoEntity) {
            this.oid = topoEntity.getOid();
            this.displayName = topoEntity.getDisplayName();
            this.entityType = topoEntity.getEntityType();
            this.entityState = topoEntity.getEntityState();
            this.commsSold = topoEntity.getCommoditySoldListList().stream()
                .filter(comm -> HEADROOM_COMMODITIES.contains(
                    comm.getCommodityType().getType()))
                .collect(Collectors.toList());
        }

        long getOid() {
            return oid;
        }

        String getDisplayName() {
            return displayName;
        }

        int getEntityType() {
            return entityType;
        }

        EntityState getEntityState() {
            return entityState;
        }

        List<CommoditySoldDTO> getCommoditySoldList() {
            return commsSold;
        }
    }
}
