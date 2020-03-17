package com.vmturbo.plan.orchestrator.project.headroom;

import static com.vmturbo.common.protobuf.PlanDTOUtil.CPU_HEADROOM_COMMODITIES;
import static com.vmturbo.common.protobuf.PlanDTOUtil.MEM_HEADROOM_COMMODITIES;
import static com.vmturbo.common.protobuf.PlanDTOUtil.STORAGE_HEADROOM_COMMODITIES;

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
import org.springframework.util.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.HeadroomPlanPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
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

    private final long planId;

    /**
     * The clusters for which we're trying to calculate headroom.
     */
    private final List<Grouping> clusters;

    private final RepositoryServiceBlockingStub repositoryService;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    private final SupplyChainServiceBlockingStub supplyChainRpcService;

    private final GroupServiceBlockingStub groupRpcService;

    private final SettingServiceBlockingStub settingService;

    private PlanDao planDao;

    private TemplatesDao templatesDao;

    private Consumer<ProjectPlanPostProcessor> onCompleteHandler;

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
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templatesDao);
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
        if (plan.hasProjectedTopologyId()) {
            // We may have multiple updates to the plan status after the initial one that set
            // the projected topology. However, we only want to calculate headroom once.
            if (calculationStarted.compareAndSet(false, true)) {
                logger.info("Headroom calculation started");

                // Get all projected entities of the plan.
                final Map<Long, HeadroomPlanPartialEntity> oidToEntities;
                try {
                    oidToEntities = getPlanPartialEntities(plan.getProjectedTopologyId());
                } catch (StatusRuntimeException e) {
                    logger.error("Failed to fetch entities from repository due to: " + e);
                    return;
                }

                // Group headroom entities by cluster and entity type.
                final Map<Long, Map<Integer, List<HeadroomPlanPartialEntity>>> entityOidsByClusterAndType =
                    groupHeadroomEntities(oidToEntities);

                // Get vmDailyGrowth of each cluster.
                final Map<Long, Float> clusterIdToVMDailyGrowth =
                    getVMDailyGrowth(entityOidsByClusterAndType.keySet());

                // Calculate headroom for each cluster.
                for (Grouping cluster : clusters) {
                    if (!entityOidsByClusterAndType.containsKey(cluster.getId())) {
                        continue;
                    }

                    try {
                        final Map<Integer, List<HeadroomPlanPartialEntity>> entitiesByType =
                            entityOidsByClusterAndType.get(cluster.getId());
                        // Fill in nonexistent headroom entity type to avoid NPE when calling get method
                        // because it's possible that there's no VM in a cluster.
                        HEADROOM_ENTITY_TYPE.stream().filter(type -> !entitiesByType.containsKey(type)).forEach(
                            type -> entitiesByType.put(type, Collections.emptyList()));

                        // Calculate headroom for this cluster.
                        calculateHeadroomPerCluster(cluster, entitiesByType,
                            clusterIdToVMDailyGrowth.get(cluster.getId()));
                    } catch (RuntimeException e) {
                        logger.error("Error in calculating headroom for cluster {} (): {}",
                            cluster.getDefinition().getDisplayName(), cluster.getId(), e);
                    }
                }
            }
        }

        // Wait until the plan completes - that is, until all pieces are finished processing -
        // to delete it, so that everything gets deleted properly.
        // In the future, we should be able to issue a delete to an in-progress plan and
        // have no orphaned data.
        String displayName = clusters.size() == 1 ?
            clusters.get(0).getDefinition().getDisplayName() : "All";
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
     * Return {@link HeadroomPlanPartialEntity}s given a topologyId.
     *
     * @param topologyId topology id
     * @return a mapping from entity oid to {@link HeadroomPlanPartialEntity}
     */
    private Map<Long, HeadroomPlanPartialEntity> getPlanPartialEntities(final long topologyId) {
        final Iterable<RetrieveTopologyResponse> topologyResponse = () ->
                repositoryService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                    .setTopologyId(topologyId)
                    .setEntityFilter(TopologyEntityFilter.newBuilder()
                        .addAllEntityTypes(HEADROOM_ENTITY_TYPE))
                    .setReturnType(Type.HEADROOM_PLAN)
                    .build());
        return StreamSupport.stream(topologyResponse.spliterator(), false)
            .flatMap(res -> res.getEntitiesList().stream())
            .map(PartialEntity::getHeadroomPlanPartialEntity)
            .collect(Collectors.toMap(HeadroomPlanPartialEntity::getOid, Function.identity()));
    }

    /**
     * Group headroom entities by cluster and entity type.
     *
     * @param oidToEntities a mapping from entity oid to {@link HeadroomPlanPartialEntity}
     * @return a mapping from cluster id to entity type to {@link HeadroomPlanPartialEntity}
     */
    private Map<Long, Map<Integer, List<HeadroomPlanPartialEntity>>> groupHeadroomEntities(
            @Nonnull final Map<Long, HeadroomPlanPartialEntity> oidToEntities) {
        final Map<Long, Map<Integer, List<HeadroomPlanPartialEntity>>> entitiesByClusterAndType =
            new HashMap<>(clusters.size());

        final GetMultiSupplyChainsRequest.Builder requestBuilder = GetMultiSupplyChainsRequest.newBuilder();
        clusters.forEach(cluster -> requestBuilder.addSeeds(SupplyChainSeed.newBuilder()
            .setSeedOid(cluster.getId())
            .setScope(SupplyChainScope.newBuilder()
                .addAllStartingEntityOid(GroupProtoUtil.getAllStaticMembers(cluster.getDefinition()))
                .addAllEntityTypesToInclude(HEADROOM_ENTITY_TYPES))));

        // Make a supply chain rpc call to fetch supply chain information, which is used to
        // determine which cluster a projected entity belongs to.
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

                final Map<Integer, List<HeadroomPlanPartialEntity>> entitiesByType = new HashMap<>(3);
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
            final ClusterStatsRequest vmCountRequest = ClusterStatsRequest.newBuilder()
                .setClusterId(clusterId)
                .setStats(StatsFilter.newBuilder()
                    .setStartDate(calendar.getTimeInMillis())
                    .setEndDate(currentTime)
                    .addCommodityRequests(CommodityRequest.newBuilder()
                        .setCommodityName(StringConstants.VM_NUM_VMS)))
                .build();
            final Iterator<StatSnapshot> snapshotIterator = statsHistoryService
                .getClusterStatsForHeadroomPlan(vmCountRequest);

            Optional<StatSnapshot> earliestSnapshot = Optional.empty();
            Optional<StatSnapshot> latestSnapshot = Optional.empty();
            while (snapshotIterator.hasNext()) {
                final StatSnapshot currentSnapshot = snapshotIterator.next();
                if (currentSnapshot.getSnapshotDate() <
                        earliestSnapshot.map(StatSnapshot::getSnapshotDate).orElse(Long.MAX_VALUE)) {
                    earliestSnapshot = Optional.of(currentSnapshot);
                }
                if (currentSnapshot.getSnapshotDate() >
                        latestSnapshot.map(StatSnapshot::getSnapshotDate).orElse(Long.MIN_VALUE)) {
                    latestSnapshot = Optional.of(currentSnapshot);
                }
            }

            if (!earliestSnapshot.isPresent() || !latestSnapshot.isPresent()) {
                logger.info("No relevant VM stat snapshots available for cluster {}. Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                continue;
            }

            final Optional<Float> earliestVMCount = earliestSnapshot.get().getStatRecordsList().stream()
                .filter(statRecord -> statRecord.getName().equals(StringConstants.VM_NUM_VMS))
                .map(statRecord -> statRecord.getValues().getAvg())
                .findFirst();
            final Optional<Float> latestVMCount = latestSnapshot.get().getStatRecordsList().stream()
                .filter(statRecord -> statRecord.getName().equals(StringConstants.VM_NUM_VMS))
                .map(statRecord -> statRecord.getValues().getAvg())
                .findFirst();

            if (!earliestVMCount.isPresent() || !latestVMCount.isPresent()) {
                logger.info("No relevant VM count records found in history for cluster : {}." +
                    " Setting 0 growth.", clusterId);
                dailyVMGrowthPerCluster.put(clusterId, DEFAULT_VM_GROWTH);
                continue;
            }

            final long earliestDate = earliestSnapshot.get().getSnapshotDate();
            final long latestDate = latestSnapshot.get().getSnapshotDate();
            logger.debug("Use data from {} to {} to calculate VMGrowth.", earliestDate, latestDate);

            if (earliestVMCount.get() > latestVMCount.get()) {
                logger.info("Negative VM growth for cluster {} : latest VM count is {}" +
                    " and earliest VM count is {}. Setting 0 growth.",
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
        @Nonnull final Map<Integer, List<HeadroomPlanPartialEntity>> headroomEntities,
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
            headroomEntities.get(EntityType.VIRTUAL_MACHINE_VALUE).size(),
            headroomEntities.get(EntityType.PHYSICAL_MACHINE_VALUE).size(),
            headroomEntities.get(EntityType.STORAGE_VALUE).size(),
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
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED)) *
                    Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU)) *
                    Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR)));
        commBoughtMap.get(CPU_HEADROOM_COMMODITIES)
            .put(CommodityType.CPU_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU)) *
                    Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED)));

        // Set MEM_HEADROOM_COMMODITIES
        commBoughtMap.get(MEM_HEADROOM_COMMODITIES)
            .put(CommodityType.MEM_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE)) *
                    Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR)));
        commBoughtMap.get(MEM_HEADROOM_COMMODITIES)
            .put(CommodityType.MEM_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_COMPUTE_MEM_SIZE)));

        // Set STORAGE_HEADROOM_COMMODITIES
        commBoughtMap.get(STORAGE_HEADROOM_COMMODITIES)
            .put(CommodityType.STORAGE_AMOUNT_VALUE,
                Double.valueOf(templateFields.get(TemplateProtoUtil.VM_STORAGE_DISK_SIZE)) *
                    Double.valueOf(templateFields.get(TemplateProtoUtil.VM_STORAGE_DISK_CONSUMED_FACTOR)));
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
        @Nonnull final Collection<HeadroomPlanPartialEntity> entities,
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
        for (HeadroomPlanPartialEntity entity : entities) {
            // If entity is not powered on, don't use it for headroom calculation.
            if (!entity.hasEntityState() ||
                !(entity.getEntityState() == EntityState.POWERED_ON)) {
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
                double headroomAvailable = 0d;
                double headroomCapacity  = 0d;
                double templateCommodityUsed = headroomCommodities.getOrDefault(commType, 0D);
                if (templateCommodityUsed == 0) {
                    continue;
                }
                // Set effective capacity
                double capacity = (comm.getEffectiveCapacityPercentage() / 100) * comm.getCapacity();
                double availableAmount =  capacity - comm.getUsed();

                if (logger.isTraceEnabled()) {
                    logger.trace("Commodity sold {} of {} has used/capacity ({}/{}) ",
                        CommodityDTO.CommodityType.forNumber(commType).name(),
                        entity.getDisplayName(), comm.getUsed(), capacity);
                }

                headroomAvailable = availableAmount > 0 ?
                                Math.floor(availableAmount / templateCommodityUsed) : 0;
                if (headroomAvailable < headroomAvailableForCurrentEntity) {
                    headroomAvailableForCurrentEntity = headroomAvailable;
                    minAvailableCommodity = commType;
                }

                headroomCapacity = Math.floor(capacity / templateCommodityUsed);
                if (headroomCapacity < headroomCapacityForCurrentEntity) {
                    headroomCapacityForCurrentEntity = headroomCapacity;
                    minCapacityCommodity = commType;
                }
            }

            if (minAvailableCommodity == -1 || minCapacityCommodity == -1) {
                logger.error("Template has used value 0 for some commodities in cluster : " +
                    cluster.getDefinition().getDisplayName() + " and id " + cluster.getId());
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
}
