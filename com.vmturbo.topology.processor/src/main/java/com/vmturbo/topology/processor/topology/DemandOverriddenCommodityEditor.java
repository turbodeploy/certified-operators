package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;

/**
 *  The class to override the workload usage and propagated down to the suppliers when user change
 *  workload utilization setting in plans.
 */
public class DemandOverriddenCommodityEditor {

    private static final Logger logger = LogManager.getLogger();

    private final GroupServiceBlockingStub groupServiceClient;

    // currently, we only allow VM util change in plan settings
    // TODO: update this set once we decide to consider other kinds of workloads,
    // such as container, for utilization change. We may need to expand the USAGE_OVERRIDDEN_COMMODITY_SET
    // and COMMODITY_BOUGHT_TO_SOLD_MAP as well.
    private static final Set<ApiEntityType> WORKLOAD_ENTITY_TYPES = ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE);
    // the set of commodity bought types associates with entities whose usage can be overriden by user in plan
    // TODO: currently we only focus on the impact of vm utilization change on host. We may consider storage use case when needed.
    private static final Set<Integer> USAGE_OVERRIDDEN_COMMODITY_SET = ImmutableSet
        .of(CommodityType.VCPU_VALUE, CommodityType.VMEM_VALUE, CommodityType.MEM_VALUE, CommodityType.CPU_VALUE);
    // a mapping between the workload's commodity bought type to the corresponding sold type
    private static final ImmutableMap<Integer, Integer> COMMODITY_BOUGHT_TO_SOLD_MAP = ImmutableMap.of(
            CommodityType.MEM_VALUE, CommodityType.VMEM_VALUE,
            CommodityType.CPU_VALUE, CommodityType.VCPU_VALUE);

    public DemandOverriddenCommodityEditor(@Nonnull final GroupServiceBlockingStub groupServiceClient) {
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
    }

    /**
     * Apply the utilization configurations on workload's commodity sold and bought.
     *
     * @param graph the topology graph
     * @param groupResolver group resolver
     * @param changes the list of utilization configurations
     */
    public void applyDemandUsageChange(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                       @Nonnull GroupResolver groupResolver,
                                       @Nonnull final List<ScenarioChange> changes) throws GroupResolutionException {
        List<UtilizationLevel> utilizationLevels = changes.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .map(ScenarioChange::getPlanChanges)
                .filter(PlanChanges::hasUtilizationLevel)
                .map(PlanChanges::getUtilizationLevel)
                .collect(Collectors.toList());
        if (utilizationLevels.isEmpty()) {
            return;
        }
        // we first find the utilization configurations and apply on the workload's commodity sold
        // then propagate the adjustment value to its commodity bought, and its provider's
        // commodity sold
        Map<UtilizationLevel, Set<TopologyEntity>> workloadsToBeOverriddenMap =
                findUsageOverriddenWorkload(groupResolver, utilizationLevels, graph);
        // workloadsUsageAdjustmentMap keeps track of a given entity and its commodity
        // bought type to the adjusted quantity difference mapping
        Map<TopologyEntity, Map<Integer, List<Double>>> workloadsUsageAdjustmentMap =
                overrideWorkloadsUsage(workloadsToBeOverriddenMap);
        // use the workloadsUsageAdjustmentMap to update the provider commodity sold
        overrideProviderUsage(workloadsUsageAdjustmentMap);

    }

    /**
     * Override the provider's commodity sold usage by finding the adjusted quantity in the commodity to adjustment map.
     *
     * @param workloadsUsageAdjustmentMap a map stores the VM and its commodity bought type to adjusted quantity mapping
     */
    private void overrideProviderUsage(@Nonnull final Map<TopologyEntity, Map<Integer, List<Double>>> workloadsUsageAdjustmentMap) {
        for (Map.Entry<TopologyEntity, Map<Integer, List<Double>>> entry : workloadsUsageAdjustmentMap.entrySet()) {
            TopologyEntity vm = entry.getKey();
            Map<Integer, List<Double>> adjustmentByCommodityType = entry.getValue();
            for (TopologyEntity provider : vm.getProviders()) {
                for (CommoditySoldDTO.Builder commSold : provider.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()) {
                    // when we find a value mapped to the provider commodity sold type in
                    // vmUsageAdjustmentMap, it means that commodity sold has to be adjusted
                    // using the same quantity
                    int commSoldType = commSold.getCommodityType().getType();
                    if (adjustmentByCommodityType.get(commSoldType) == null) {
                        continue;
                    }
                    Double usedDiff = adjustmentByCommodityType.get(commSoldType).get(0);
                    if (usedDiff != null) {
                        // change the commodity sold hisUtilization if it exist, otherwise
                        // change the commodity sold used
                        if (commSold.hasHistoricalUsed()) {
                            double oldUsed = commSold.getHistoricalUsed().getHistUtilization();
                            commSold.getHistoricalUsedBuilder().setHistUtilization(oldUsed + usedDiff);
                        } else {
                            double oldUsed = commSold.getUsed();
                            commSold.setUsed(oldUsed + usedDiff);
                        }
                    }
                    Double peakDiff = adjustmentByCommodityType.get(commSoldType).get(1);
                    if (peakDiff != null) {
                        // change the commodity sold peak hisUtilization if it exist, otherwise
                        // change the commodity sold peak
                        if (commSold.hasHistoricalPeak()) {
                            double oldPeak = commSold.getHistoricalPeak().getHistUtilization();
                            commSold.getHistoricalPeakBuilder().setHistUtilization(oldPeak + peakDiff);
                        } else {
                            double oldPeak = commSold.getPeak();
                            commSold.setPeak(oldPeak + peakDiff);
                        }
                    }
                }
            }
        }
    }

    /**
     * Utility method to apply changes on CommoditySoldDTO.
     *
     * @param commSold the dto to be changed
     * @param percentage percentage level to be changed
     * @param isPeak whether apply change on used or peak
     *
     * @return the amount difference
     */
    private double applyChangeOnCommoditySold(@Nonnull CommoditySoldDTO.Builder commSold,
                                              int percentage, boolean isPeak) {
        double difference;
        if (commSold.hasHistoricalUsed() && !isPeak) {
            HistoricalValues.Builder builder = commSold.getHistoricalUsedBuilder();
            difference = applyChangeOnHistoricalBuilder(commSold, builder, percentage);
        } else if (commSold.hasHistoricalPeak() && isPeak) {
            HistoricalValues.Builder builder = commSold.getHistoricalPeakBuilder();
            difference = applyChangeOnHistoricalBuilder(commSold, builder, percentage);
        } else if (isPeak) {
            double newPeak = Math.max(0, Math.min(commSold.getCapacity(),
                commSold.getPeak() * (percentage / 100d + 1)));
            difference = newPeak - commSold.getPeak();
            commSold.setPeak(Math.max(0, newPeak));
        } else {
            double newUsed = Math.max(0, Math.min(commSold.getCapacity(),
                commSold.getUsed() * (percentage / 100d + 1)));
            difference = newUsed - commSold.getUsed();
            commSold.setUsed(Math.max(0, newUsed));
        }
        return difference;
    }

    /**
     * Utility method to apply change on historical values.
     *
     * @param commSold the dto to be changed
     * @param builder the historical value builder
     * @param percentage percentage level to be changed
     *
     * @return the amount difference
     */
    private double applyChangeOnHistoricalBuilder(@Nonnull CommoditySoldDTO.Builder commSold,
                                                @Nonnull HistoricalValues.Builder builder,
                                                int percentage) {
        double newUsed = Math.max(0, Math.min(commSold.getCapacity(),
            builder.getHistUtilization() * (percentage / 100d + 1)));
        double difference = newUsed - builder.getHistUtilization();
        builder.setHistUtilization(Math.max(0, newUsed));
        return difference;
    }

    /**
     * Utility method to apply changes on CommodityBoughtDTO.
     *
     * @param commBought the dto to be changed
     * @param diff the difference to be applied
     * @param isPeak whether apply change on used or peak
     */
    private void applyChangeOnCommodityBought(@Nonnull CommodityBoughtDTO.Builder commBought,
                                              double diff, boolean isPeak) {
        if (commBought.hasHistoricalUsed() && !isPeak) {
            HistoricalValues.Builder builder = commBought.getHistoricalUsedBuilder();
            double oldUsed = builder.getHistUtilization();
            builder.setHistUtilization(Math.max(0, oldUsed + diff));
        } else if (commBought.hasHistoricalPeak() && isPeak) {
            HistoricalValues.Builder builder = commBought.getHistoricalPeakBuilder();
            double oldUsed =  builder.getHistUtilization();
            builder.setHistUtilization(Math.max(0, oldUsed + diff));
        } else if (isPeak) {
            double oldPeak = commBought.getPeak();
            commBought.setPeak(Math.max(0, oldPeak + diff));
        } else {
            double oldUsed = commBought.getUsed();
            commBought.setUsed(Math.max(0, oldUsed + diff));
        }
    }

    /**
     * Override VM commodity sold and commodity bought usage based on utilization configurations.
     * In particular, the utilization percentage is applied on the sold commodity. The quantity
     * difference of a sold commodity will be propagated to the corresponding bought commodity.
     *
     * @param workloadsToBeOverriddenMap the map of VM sets grouped by utilization configurations
     * @return a map of VM and its commodity bought type to adjusted amount mapping
     */
    private Map<TopologyEntity, Map<Integer, List<Double>>> overrideWorkloadsUsage(
            @Nonnull final Map<UtilizationLevel, Set<TopologyEntity>> workloadsToBeOverriddenMap) {
        // a map to keep track of workloads commodity bought type and its adjusted quantity difference
        Map<TopologyEntity, Map<Integer, List<Double>>> workloadsBoughtUsageAdjustmentMap = new HashMap<>();
        for (Map.Entry<UtilizationLevel, Set<TopologyEntity>> entry : workloadsToBeOverriddenMap.entrySet()) {
            int percentage = entry.getKey().getPercentage();
            Set<TopologyEntity> wls = entry.getValue();
            wls.stream().forEach(v ->  {
                Map<Integer, List<Double>> usageChangebycommSoldType = new HashMap<>();
                for (CommoditySoldDTO.Builder commSold : v.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()) {
                    double usedDifference = 0;
                    double peakDifference = 0;
                    int soldType = commSold.getCommodityType().getType();
                    if (USAGE_OVERRIDDEN_COMMODITY_SET.contains(soldType)) {
                        // change the commodity sold hisUtilization if it exist, otherwise
                        // change the commodity sold used
                        usedDifference = applyChangeOnCommoditySold(commSold, percentage, false);
                        peakDifference = applyChangeOnCommoditySold(commSold, percentage, true);
                        // keep track of the adjusted quantity of a commodity sold
                        usageChangebycommSoldType.put(soldType,
                                new ArrayList<>(Arrays.asList(usedDifference, peakDifference)));
                    }
                }
                Map<Integer, List<Double>> usageChangebycommBoughtType = new HashMap<>();
                workloadsBoughtUsageAdjustmentMap.put(v, usageChangebycommBoughtType);
                for (CommoditiesBoughtFromProvider.Builder boughtFromProvider
                        : v.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList()) {
                    for (CommodityBoughtDTO.Builder commBought : boughtFromProvider.getCommodityBoughtBuilderList()) {
                        int boughtType = commBought.getCommodityType().getType();
                        if (USAGE_OVERRIDDEN_COMMODITY_SET.contains(boughtType)) {
                            // for a given commodity bought, find the commodity sold that depends on it
                            Integer soldMappingType = COMMODITY_BOUGHT_TO_SOLD_MAP.get(boughtType);
                            if (soldMappingType != null) {
                                // the commodity sold adjusted quantity has to be transfer to the
                                // commodity bought.
                                // Note: user requirement is to adjust the commodity bought by the
                                // same quantity, not by the same percentage
                                if (usageChangebycommSoldType.get(soldMappingType) == null) {
                                    continue;
                                }
                                double usedDiff = usageChangebycommSoldType.get(soldMappingType).get(0);
                                double peakDiff = usageChangebycommSoldType.get(soldMappingType).get(1);
                                // change the commodity bought hisUtilization if it exist, otherwise
                                // change the commodity bought used
                                applyChangeOnCommodityBought(commBought, usedDiff, false);
                                applyChangeOnCommodityBought(commBought, peakDiff, true);
                                // keep track of VM commodity bought and its adjusted quantity
                                usageChangebycommBoughtType.put(boughtType,
                                        new ArrayList<>(Arrays.asList(usedDiff, peakDiff)));
                            }
                        }
                    }
                }
            });
        }
        return workloadsBoughtUsageAdjustmentMap;
    }

    /**
     * A utility method to find the set of VMs associated with a given utilization change.
     *
     * @param groupResolver  the group resolver
     * @param utilizationLevels the scenarioChange object containing utilization configuration
     * @param graph  the full topology graph
     * @return a map of VM sets grouped by utilization configuration
     * @throws GroupResolutionException If the group for the utilization level override is not found or can't be resolved.
     */
    private Map<UtilizationLevel, Set<TopologyEntity>> findUsageOverriddenWorkload(
            @Nonnull GroupResolver groupResolver,
            @Nonnull final List<UtilizationLevel> utilizationLevels,
            @Nonnull final TopologyGraph<TopologyEntity> graph) throws GroupResolutionException {
        // a map to group together the workloads by a certain type of utilization setting
        Map<UtilizationLevel, Set<TopologyEntity>> workloadsToBeOverriddenMap = new HashMap<>();
        final Map<Long, Grouping> groups = new HashMap<>();
        Set<Long> groupOids = utilizationLevels.stream()
                .filter(u -> u.hasGroupOid())
                .map(UtilizationLevel::getGroupOid)
                .collect(Collectors.toSet());
        // query for group objects
        if (!groupOids.isEmpty()) {
            groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                        .addAllId(groupOids))
                    .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                    .build())
                    .forEachRemaining(group -> {
                        if (group.hasId()) {
                            groups.put(group.getId(), group);
                        } else {
                            logger.warn("Group has no id. Skipping. {}", group);
                        }
                    });
        }
        for (UtilizationLevel util : utilizationLevels) {
            Set<TopologyEntity> workloads = new HashSet<>();
            if (!util.hasGroupOid()) {
                // this is a full scope utilization setting for VM
                WORKLOAD_ENTITY_TYPES.forEach(e -> {
                    workloads.addAll(graph.entitiesOfType(e.typeNumber()).collect(Collectors.toSet()));
                });
                workloadsToBeOverriddenMap.put(util, workloads);
            } else {
                Grouping group = groups.get(util.getGroupOid());
                final ResolvedGroup resolvedGroup = groupResolver.resolve(group, graph);
                resolvedGroup.getEntitiesOfTypes(WORKLOAD_ENTITY_TYPES).stream()
                    .map(graph::getEntity)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(workloads::add);
                workloadsToBeOverriddenMap.put(util, workloads);
            }
        }
        return workloadsToBeOverriddenMap;
    }
}

