package com.vmturbo.plan.orchestrator.project.headroom;

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

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.CommodityHeadroom;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
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
     * The cluster for which we're trying to calculate headroom.
     */
    private final Group cluster;

    /**
     * The number of clones added to the cluster in the plan.
     */
    private final long addedClones;

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
    private static final int PEAK_LOOKBACK_DAYS = 7;

    // Milliseconds in a day
    final long dayMilliSecs = TimeUnit.DAYS.toMillis(1);

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
                    ImmutableSet.of(CommodityType.STORAGE_VALUE, CommodityType.STORAGE_PROVISIONED_VALUE);

    /**
     * Whether the actual calculation of headroom (kicked off by the projected topology being
     * available) has started.
     */
    private final AtomicBoolean calculationStarted = new AtomicBoolean(false);

    public ClusterHeadroomPlanPostProcessor(final long planId,
                                            @Nonnull final long clusterId,
                                            @Nonnull final Channel repositoryChannel,
                                            @Nonnull final Channel historyChannel,
                                            final long addedClones,
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

        this.addedClones = addedClones;
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.cluster = Objects.requireNonNull(groupRpcService
                        .getGroup(GroupID.newBuilder().setId(clusterId).build()).getGroup());
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
                // This is all we need for post-processing, don't need to wait for plan
                // to succeed.
                final Iterable<RetrieveTopologyResponse> topologyResponse = () ->
                    repositoryService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                            .setTopologyId(plan.getProjectedTopologyId())
                            .build());

                // Headroom entities filtered based on entity type.
                Map<Integer, List<TopologyEntityDTO>> headroomEntities =
                                StreamSupport.stream(topologyResponse.spliterator(), false)
                                    .flatMap(res -> res.getEntitiesList().stream())
                                    .filter(entity -> HEADROOM_ENTITY_TYPE.contains(entity.getEntityType()))
                                    .collect(Collectors.groupingBy(e -> e.getEntityType()));

                // In a headroom plan only the clones are unplaced, and nothing else changes. Therefore
                // the number of unplaced VMs = the number of unplaced clones.
                final long unplacedClones = headroomEntities.get(EntityType.VIRTUAL_MACHINE_VALUE).stream()
                                .filter(vm -> !TopologyDTOUtil.isPlaced(vm))
                                .count();
                final long headroom = addedClones - unplacedClones;
                final long numVMs = getNumberOfVMs();
                Optional<Template> template = templatesDao
                                .getTemplate(cluster.getCluster().getClusterHeadroomTemplateId());

                if (!template.isPresent()) {
                    logger.error("Template not found for : " + cluster.getCluster().getName() +
                                    " with template id : " + cluster.getCluster().getClusterHeadroomTemplateId());
                    return;
                }

                // Map of  commodities bought by template per relevant headroom commodities
                // set of CPU, MEM and Storage.
                Map<Set<Integer>, Map<Integer, Double>> commoditiesBoughtByTemplate =
                                getCommoditiesBoughtByTemplate(template.get());


                long vmGrowth = getVMGrowth(headroomEntities.get(EntityType.VIRTUAL_MACHINE_VALUE).stream()
                                .map(vm -> vm.getOid())
                                .collect(Collectors.toSet()));
                CommodityHeadroom cpuHeadroom = calculateHeadroom(
                                headroomEntities.get(EntityType.PHYSICAL_MACHINE_VALUE),
                                commoditiesBoughtByTemplate.get(CPU_HEADROOM_COMMODITIES), vmGrowth);
                CommodityHeadroom memHeadroom = calculateHeadroom(
                                headroomEntities.get(EntityType.PHYSICAL_MACHINE_VALUE),
                                commoditiesBoughtByTemplate.get(MEM_HEADROOM_COMMODITIES), vmGrowth);
                CommodityHeadroom storageHeadroom = calculateHeadroom(
                                headroomEntities.get(EntityType.STORAGE_VALUE),
                                commoditiesBoughtByTemplate.get(STORAGE_HEADROOM_COMMODITIES), vmGrowth);
                createStatsRecords(headroom, numVMs, cpuHeadroom, memHeadroom, storageHeadroom);
            }
        }

        // Wait until the plan completes - that is, until all pieces are finished processing -
        // to delete it, so that everything gets deleted properly.
        // In the future, we should be able to issue a delete to an in-progress plan and
        // have no orphaned data.
        if (plan.getStatus() == PlanStatus.SUCCEEDED || plan.getStatus() == PlanStatus.FAILED) {
            if (plan.getStatus() == PlanStatus.FAILED) {
                logger.error("Cluster headroom plan for cluster ID {} failed! Error: {}",
                        cluster.getCluster().getName(), plan.getStatusMessage());
            } else {
                logger.info("Cluster headroom plan for cluster ID {} completed!",
                        cluster.getCluster().getName());
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
            logger.info("Cluster headroom plan for cluster ID {} has new status: {}",
                    cluster.getCluster().getName(), plan.getStatus());
        }
    }

    /**
     * VM growth is calculated based on assumption that current VMs in cluster since PEAK_LOOKBACK_DAYS.
     * and then fetch VMs from week before to count VMs that were not present earlier but are present now.
     * @param currentVMsInCluster VMs in cluster currently.
     * @return newly added VMs since past PEAK_LOOKBACK_DAYS.
     */
    private long getVMGrowth(Set<Long> currentVMsInCluster) {
        EntityStatsScope.Builder scope = EntityStatsScope.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);

        // calculate date peakLookBackDays behind
        Long currentTime = System.currentTimeMillis();
        Long oneWeekduration = Long.valueOf(dayMilliSecs * PEAK_LOOKBACK_DAYS);
        Long timeOneWeekAgo = currentTime - oneWeekduration;
        Long timeTwoWeeksAgo = timeOneWeekAgo - oneWeekduration;

        // This is a way around to fetch VMs from history since currently we don't populate
        // Cluster_Members table (legacy uses this table to get required VMs).
        GetEntityStatsRequest entityStatsRequest = GetEntityStatsRequest.newBuilder()
                        .setScope(scope)
                        .setFilter(StatsFilter.newBuilder()
                            .setStartDate(timeTwoWeeksAgo).setEndDate(timeOneWeekAgo))
                        .build();

        GetEntityStatsResponse response = statsHistoryService.getEntityStats(entityStatsRequest);
        final Set<Long> vmOidsFromHistory = response.getEntityStatsList().stream()
                        .map(entity -> entity.getOid())
                        .collect(Collectors.toSet());
        return currentVMsInCluster.stream()
        .filter(currentVM -> !vmOidsFromHistory.contains(currentVM))
        .count();
    }

    /**
     * Parse given template to return commodities values grouped by specific headroom commodity types.
     * For example : For CPU_HEADROOM_COMMODITIES(CPU, CPU_PROV) Returned map has
     * key (Set<CPU_HEADROOM_COMMODITIES>) -> value (Map : key<Commodity_Type> -> Value<UsedValue>) calculated
     * by parsing template fields specific to system load.
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
            .put(CommodityType.STORAGE_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.DISK_SIZE)) *
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.DISK_CONSUMED_FACTOR)));
        commBoughtMap.get(STORAGE_HEADROOM_COMMODITIES)
            .put(CommodityType.STORAGE_PROVISIONED_VALUE,
                Double.valueOf(templateFields.get(SystemLoadCalculatedProfile.DISK_SIZE)));
        return commBoughtMap;
    }

    /**
     * Save the headroom value.
     * @param headroom headroom available
     * @param numVms number of vms
     * @param cpuHeadroomInfo Headroom values for CPU.
     * @param memHeadroomInfo Headroom values for Memory.
     * @param storageHeadroomInfo Headroom values for Storage.
     */
    @VisibleForTesting
    void createStatsRecords(final long headroom, final long numVms,
                    CommodityHeadroom cpuHeadroomInfo,
                    CommodityHeadroom memHeadroomInfo,
                    CommodityHeadroom storageHeadroomInfo) {
        if (headroom == addedClones) {
            logger.info("Cluster headroom for cluster {} is over {}",
                    cluster.getCluster().getName(), headroom);
        } else {
            logger.info("Cluster headroom for cluster {} is {}",
                    cluster.getCluster().getName(), headroom);
        }

        // Save the headroom in the history component.
        try {
            statsHistoryService.saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                    .setClusterId(cluster.getId())
                    .setNumVMs(numVms)
                    .setHeadroom(headroom)
                    .setCpuHeadroomInfo(cpuHeadroomInfo)
                    .setMemHeadroomInfo(memHeadroomInfo)
                    .setStorageHeadroomInfo(storageHeadroomInfo)
                    .build());
        } catch (StatusRuntimeException e) {
            logger.error("Failed to save cluster headroom: {}", e.getMessage());
        }
    }

    @Override
    public void registerOnCompleteHandler(final Consumer<ProjectPlanPostProcessor> handler) {
        this.onCompleteHandler = handler;
    }

    /**
     * Get the number of VMs running in the cluster.
     *
     * @return number of VMs
     */
    private long getNumberOfVMs() {
        // Use the group service to get a list of IDs of all members (physical machines)
        GetMembersResponse response = groupRpcService.getMembers(GetMembersRequest.newBuilder()
                .setId(cluster.getId())
                .build());
        List<Long> memberIds = response.getMembers().getIdsList();

        // Use the supply chain service to get all VM nodes that belong to the physical machines
        Iterator<SupplyChainNode> supplyChainNodeIterator = supplyChainRpcService.getSupplyChain(
                SupplyChainRequest.newBuilder()
                        .addAllStartingEntityOid(memberIds)
                        .addEntityTypesToInclude("VirtualMachine")
                        .build());

        Iterable<SupplyChainNode> iterable = () -> supplyChainNodeIterator;
        Stream<SupplyChainNode> nodeStream = StreamSupport.stream(iterable.spliterator(), false);
        return nodeStream.map(RepositoryDTOUtil::getMemberCount).reduce(0, Integer::sum);
    }

    /**
     * Calculate :
     * 1) Available Headroom : number of VMs that can be accommodated in cluster considering its utilization for given commodity.
     * 2) Empty Headroom : number of VMs that can be accommodated in cluster for given commodity when cluster is empty.
     * 3) DaysToExhaustion : calculated based on calculated headroom and given vmGrowth.
     * @param entities relevant for headroom calculation.
     * @param headroomCommodities for which headroom is calculated.
     * @param vmGrowth growth of VMs in past PEAK_LOOKBACK_DAYS
     * @return CommodityHeadroom returns computed headroom data.
     */
    private CommodityHeadroom calculateHeadroom(List<TopologyEntityDTO> entities,
                    Map<Integer, Double> headroomCommodities, long vmGrowth) {

        if (CollectionUtils.isEmpty(entities) || CollectionUtils.isEmpty(headroomCommodities)) {
            return CommodityHeadroom.getDefaultInstance();
        }

        // Initialize headroom with maximum values because we want to maintain minimum
       //  of headroom availability and capacity across commodities.
        HeadroomData headroomData = new HeadroomData(Double.MAX_VALUE, Double.MAX_VALUE);

        entities.stream()
            .filter(e -> e.getEntityState() == TopologyDTO.EntityState.POWERED_ON)
            .flatMap(entity -> entity.getCommoditySoldListList().stream())
            .filter(comm -> headroomCommodities.keySet().contains(comm.getCommodityType().getType()))
            .collect(Collectors.groupingBy(comm -> comm.getCommodityType().getType()))
            .forEach((commType, commodityList) -> {
                double headroomAvailable = 0d;
                double headroomCapacity  = 0d;
                for (CommoditySoldDTO comm : commodityList) {
                    double templateCommodityUsed = headroomCommodities.get(commType);
                    if (templateCommodityUsed == 0) {
                        continue;
                    }
                    double availableAmount = comm.getCapacity() - comm.getUsed();
                    headroomAvailable += availableAmount > 0 ?
                                    Math.floor(availableAmount / templateCommodityUsed) : 0;
                    headroomCapacity += Math.floor(comm.getCapacity() / templateCommodityUsed);
                }
                // Maintain a minimum across headroom commodities.
                headroomData.setHeadroomAvailable(Math.min(headroomData.getHeadroomAvailable(), headroomAvailable));
                headroomData.setHeadroomCapacity(Math.min(headroomData.getHeadroomCapacity(), headroomCapacity));
            });

        return CommodityHeadroom.newBuilder()
                    .setHeadroom((long)headroomData.getHeadroomAvailable())
                    .setCapacity((long)headroomData.getHeadroomCapacity())
                    .setDaysToExhaustion(getDaysToExhaustion(vmGrowth, headroomData.getHeadroomAvailable()))
                    .build();
    }

    /**
     * Calculate days to exhaustion by using vmGrowth rate in past PEAK_LOOKBACK_DAYS
     * and current headroom availability.
     * @param vmGrowth growth of VMs in past PEAK_LOOKBACK_DAYS
     * @param headroomAvailable current headroom availability.
     * @return days to exhaustion.
     */
    private long getDaysToExhaustion(long vmGrowth, double headroomAvailable) {
        //if headroom == 0 - cluster is already exhausted
        if (headroomAvailable == 0) {
            return 0;
        }
        //if headroom is infinite OR VM Growth is 0  - exhaustion time is infinite
        if (headroomAvailable == Double.MAX_VALUE || vmGrowth == 0) {
            return Long.MAX_VALUE;
        }
        return (long)Math.floor(((float)headroomAvailable / vmGrowth) * PEAK_LOOKBACK_DAYS);
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
     * Contains availability and capacity for headroom.
     *
     */
    private class HeadroomData {
        private double headroomAvailable;
        private double headroomCapacity;

        /** Number of VMs that can be accommodated in cluster considering its utilization for given commodity.
         * @return headroomAvailable
         */
        public double getHeadroomAvailable() {
            return headroomAvailable;
        }

        /** Sets number of VMs that can be accommodated in cluster considering its utilization for given commodity.
         * @param headroomAvailable the headroomAvailable to set
         */
        public void setHeadroomAvailable(double headroomAvailable) {
            this.headroomAvailable = headroomAvailable;
        }

        /**
         * Number of VMs that can be accommodated in cluster for given commodity when cluster is empty.
         * @return the headroomCapacity
         */
        public double getHeadroomCapacity() {
            return headroomCapacity;
        }

        /**
         * Number of VMs that can be accommodated in cluster for given commodity when cluster is empty.
         * @param headroomCapacity the headroomCapacity to set
         */
        public void setHeadroomCapacity(double headroomCapacity) {
            this.headroomCapacity = headroomCapacity;
        }

        HeadroomData(double headroomAvailable, double headroomCapacity) {
            this.headroomAvailable = headroomAvailable;
            this.headroomCapacity = headroomCapacity;
        }
    }
}
