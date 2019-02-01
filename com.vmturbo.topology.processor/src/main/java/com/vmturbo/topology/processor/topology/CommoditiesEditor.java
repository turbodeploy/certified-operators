package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Editor to update values for commodities.
 */
public class CommoditiesEditor {

    private static final int MAX_SUPPLY_CHAIN_LENGTH = 15;
    private static final ImmutableSet<CommodityType> ACCESS_COMMODITIES =
                    ImmutableSet.of(CommodityType.STORAGE_ACCESS, CommodityType.DISK_ARRAY_ACCESS,
                                    CommodityType.TEMPLATE_ACCESS, CommodityType.LICENSE_ACCESS,
                                    CommodityType.ACCESS, CommodityType.TENANCY_ACCESS,
                                    CommodityType.VMPM_ACCESS, CommodityType.VAPP_ACCESS);

    private final Logger logger = LogManager.getLogger();
    private final StatsHistoryServiceBlockingStub historyClient;

    public CommoditiesEditor(@Nonnull StatsHistoryServiceBlockingStub historyClient) {
        this.historyClient = historyClient;
    }

    /**
     * Applies changes to commodities values for used,peak etc. for VMs in
     * specific cases like baseline plan, cluster headroom plan.
     * @param graph to extract entities from.
     * @param changes to iterate over and find relevant changes (e.g baseline change).
     * @param topologyInfo to find VMs in current scope or to find plan type.
     * @param scope to get information about plan scope.
     */
    public void applyCommodityEdits(@Nonnull final TopologyGraph graph,
                    @Nonnull final List<ScenarioChange> changes,
                    TopologyDTO.TopologyInfo topologyInfo, @Nonnull final PlanScope scope) {
        editCommoditiesForBaselineChanges(graph, changes, topologyInfo);
        editCommoditiesForClusterHeadroom(graph, scope, topologyInfo);
    }

    /**
     * Change commodity values for VMs and its providers if a scenario change
     * related to historical baseline exists.
     * @param graph to extract entities from.
     * @param changes to iterate over and find relevant to baseline change.
     * @param topologyInfo to find VMs in current scope.
     */
    private void editCommoditiesForBaselineChanges(@Nonnull final TopologyGraph graph,
                    @Nonnull final List<ScenarioChange> changes,
                    TopologyDTO.TopologyInfo topologyInfo) {
        changes.stream()
            .filter(change -> change.getPlanChanges().hasHistoricalBaseline())
            .findFirst()
            .ifPresent(change -> {
                final Set<TopologyEntity> vmSet = topologyInfo.getScopeSeedOidsList().stream()
                    .distinct()
                    .flatMap(oid -> findVMsInScope(graph, oid))
                    .collect(Collectors.toSet());
                long baselineDate = change.getPlanChanges()
                    .getHistoricalBaseline().getBaselineDate();
                fetchAndApplyHistoricalData(vmSet, graph, baselineDate);
            });
    }

    /**
     * Get historical data for given baselineDate from database and
     * apply to relevant entities in graph.
     * @param vmSet if empty -> fetch all VMs' data otherwise only for given set.
     * @param graph which entities belong to.
     * @param baselineDate for which data is fetched for.
     */
    private void fetchAndApplyHistoricalData(Set<TopologyEntity> vmSet,
                    final TopologyGraph graph, long baselineDate) {
        EntityStatsScope.Builder scope = EntityStatsScope.newBuilder();

        // Empty set implies global scope hence set scope to all VMs.
        if (CollectionUtils.isEmpty(vmSet)) {
            scope.setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        } else {
            // Set scope to given entities.
            scope.setEntityList(EntityList.newBuilder().addAllEntities(
                            vmSet.stream().map(e -> e.getOid()).collect(Collectors.toList())));
        }

        GetEntityStatsRequest entityStatsRequest = GetEntityStatsRequest.newBuilder()
                        .setScope(scope)
                        .setFilter(StatsFilter.newBuilder()
                            .setStartDate(baselineDate).setEndDate(baselineDate))
                        .build();

        GetEntityStatsResponse response = historyClient.getEntityStats(entityStatsRequest);

        // Group entity stats by VM's oid.
        final Map<Long, List<EntityStats>> resp = response.getEntityStatsList().stream()
                .collect(Collectors.groupingBy(EntityStats::getOid));

        resp.keySet().forEach(oid -> {
            graph.getEntity(oid).ifPresent(vm -> {
                // Create map for this VM and its providers.
                Map<Integer, Queue<Long>> providerIdsByCommodityType =
                                getProviderIdsByCommodityType(vm);
                resp.get(oid).forEach(entityStat -> {
                    entityStat.getStatSnapshotsList().forEach(statSnapshot -> {
                        statSnapshot.getStatRecordsList().forEach(statRecord -> {
                            // Update commodity value for this entity
                            final CommodityType commType = ClassicEnumMapper.COMMODITY_TYPE_MAPPINGS
                                .get(statRecord.getName());
                            updateCommodityValuesForVmAndProvider(vm, commType, graph, providerIdsByCommodityType,
                                statRecord.getPeak().getAvg(), statRecord.getUsed().getAvg());
                        });
                    });
                });
            });
        });
    }

    /**
     * Create map with key as commdityBought type and value as queue of providerIds
     * for commodity bought type. Queue is used to handle cases when VM buys same
     * commodity type from multiple providers.
     * @param vm to create map for.
     * @return map with key as commdityBought type and value as queue of providerIds.
     */
    private Map<Integer, Queue<Long>> getProviderIdsByCommodityType(TopologyEntity vm) {
        final Map<Integer, Queue<Long>> providerIdsByCommodityType = new HashMap<>();
        for (TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.Builder commBoughtFromProvider :
            vm.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList()) {
            for (TopologyDTO.CommodityBoughtDTO.Builder comm
                            : commBoughtFromProvider.getCommodityBoughtBuilderList()) {
                long providerOid = commBoughtFromProvider.getProviderId();
                providerIdsByCommodityType
                    .computeIfAbsent(comm.getCommodityType().getType(), k -> new LinkedList<>())
                    .add(providerOid);
            }
        }
        return providerIdsByCommodityType;
    }

    /**
     * Update current used/peak commodity values for VMs and its provider.
     * Example : Expected value used for provider : used/peak - currValueForVM + valueFromStatRecord.
     * Example : Expected value used for VM : as fetched from database (i.e StatRecord values).
     * @param vm and its providers for which commodity values are updated.
     * @param commType commodity type to be updated.
     * @param graph which entities belong to.
     * @param providerIdsByCommodityType provides relationship between commodity type and its providers for commodities bought by VM.
     * @param peak value as fetched from database for this VM's commodity.
     * @param used value as fetched from database for this VM's commodity.
     */
    private void updateCommodityValuesForVmAndProvider(TopologyEntity vm, CommodityType commType,
                    final TopologyGraph graph,
                    final Map<Integer, Queue<Long>> providerIdsByCommodityType,
                    final double peak,
                    final double used) {
        // We skip access commodities
        if (commType == null || ACCESS_COMMODITIES.contains(commType)) {
            return;
        }

        // W.r.t legacy for baseline and headroom we want to update VMs, Hosts and Storages.
        // May be we might need to update VM's consumers (containers) in future but that will be an improvement.
        // handle commodity sold by VM
        Optional<CommoditySoldDTO.Builder> commoditySold = vm.getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().stream()
                .filter(comm -> comm.getCommodityType().getType() == commType.getNumber())
                .findFirst();
        if (commoditySold.isPresent()) {
            commoditySold.get().setUsed(used);

            if (peak < 0) {
                logger.error("Peak quantity = {} for commodity type {} of topology entity {}",
                        peak, commoditySold.get().getCommodityType(), vm.getDisplayName());
            }

            commoditySold.get().setPeak(peak);
        // Otherwise, handle commodity bought by VM
        } else if (providerIdsByCommodityType.containsKey(commType.getNumber())) {
            // Get first provider and add it to the end because we want
            // to apply commodities' update across VM providers if we have multiple commodities of same type.
            // For example : If we have a VM consuming from multiple storages currently as well as
            // on baseline date we want to update providers in round robin fashion.
            // Let's say, on baseline date VM was consuming from ST1 and ST2 and is currently consuming from
            // ST3 and ST4. Queue will contain ST3 and ST4 and we will update ST3 and ST4
            // because we will get two stat entries from db. For first stat entry we remove first element from
            // queue(ST3) and add it to end and in next stat entry we will update ST4.
            Queue<Long> providers = providerIdsByCommodityType.get(commType.getNumber());
            long providerOid = providers.remove();
            providers.add(providerOid);

            graph.getEntity(providerOid).ifPresent(provider -> {
             // Find commodity bought relevant to current provider.
                Optional<CommodityBoughtDTO.Builder> commBought = vm.getTopologyEntityDtoBuilder()
                                .getCommoditiesBoughtFromProvidersBuilderList().stream()
                                .filter(commsFromProvider -> commsFromProvider.getProviderId() == providerOid)
                                .flatMap(c -> c.getCommodityBoughtBuilderList().stream())
                                .filter(g -> g.getCommodityType().getType() == commType.getNumber())
                                .findFirst();

                 commBought.ifPresent(commodityBought -> {
                    // Set values of provider
                    Optional<CommoditySoldDTO.Builder> commSoldByProvider =
                                    provider.getTopologyEntityDtoBuilder()
                                    .getCommoditySoldListBuilderList()
                                    .stream()
                                    .filter(c -> c.getCommodityType().getType() == commType.getNumber() )
                                    .findFirst();

                    if (commSoldByProvider.isPresent()) {
                        CommoditySoldDTO.Builder commSold = commSoldByProvider.get();
                        // Subtract current value and add fetched value from database.
                        float newPeak = (float) (Math.max(commSold.getPeak() - commodityBought.getPeak(), 0) + peak);
                        if (newPeak < 0) {
                            logger.error("Peak quantity = {} for commodity type {} of topology entity {}"
                                    , newPeak, commSold.getCommodityType(), provider.getDisplayName());
                        }
                        commSold.setPeak(newPeak);
                        commSold.setUsed(Math.max(commSold.getUsed() - commodityBought.getUsed(), 0)
                                        + used);
                    }

                    // Set value for consumer.
                    // Note : If we have multiple providers on baseline date and one provider currently
                    // we will set value for commodity bought from last record. It is not a problem because
                    // price in market is calculated by providers based on commodity sold. Mismatch in commodity
                    // bought and sold values will be reflected as overhead.
                   commodityBought.setPeak(peak);
                   commodityBought.setUsed(used);
                });
            });
        }
    }

    /**
     * Use given scope oid to traverse up or down the supply chain and find related VMs.
     * @param graph to traverse on for providers/consumers.
     * @param oid in current scope.
     * @return stream of VMs in scope found via supply chain traversal.
     */
    private Stream<TopologyEntity> findVMsInScope(final TopologyGraph graph, long oid) {
        Optional<TopologyEntity> optionalEntity = graph.getEntity(oid);
        if (!optionalEntity.isPresent()) {
            return Stream.empty();
        }

        final TopologyEntity seedEntity = optionalEntity.get();

        // If already a VM, add and return
        if (seedEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            return Collections.singleton(seedEntity).stream();
        }

        Set<TopologyEntity> vmSet = new HashSet<>();
        // Traverse up the supply chain.
        vmSet.addAll(traverseToVms(seedEntity, TopologyEntity::getConsumers));
        // Traverse down the supply chain.
        vmSet.addAll(traverseToVms(seedEntity, TopologyEntity::getProviders));

        return vmSet.stream();
    }

    private Set<TopologyEntity> traverseToVms(@Nonnull final TopologyEntity seedEntity,
                    @Nonnull final Function<TopologyEntity, List<TopologyEntity>> traversalFn) {
        // Keep iteration count to prevent getting stuck in infinite loop if we have a bad
        // consumer/provider relationship. For example : an entity pointing to itself as consumer/provider.
        int iterationCount = 0;
        HashSet<TopologyEntity> vmSet = new HashSet<>();
        List<TopologyEntity> nextLevelEntities = traversalFn.apply(seedEntity);
        while (iterationCount <= MAX_SUPPLY_CHAIN_LENGTH && !CollectionUtils.isEmpty(nextLevelEntities)) {
            final List<TopologyEntity> newEntities = new ArrayList<TopologyEntity>();
            nextLevelEntities.forEach(currEntity -> {
                if (currEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    vmSet.add(currEntity);
                } else {
                    newEntities.addAll(traversalFn.apply(currEntity));
                }
            });
            nextLevelEntities = newEntities;
            iterationCount++;
        }
        return vmSet;
    }

    /**
     * Apply system load data to VMs if it is a cluster headroom plan with valid cluster oid.
     * @param graph which entities belong to.
     * @param scope which contains cluster oid.
     * @param topologyInfo to identify if it is a Cluster headroom plan.
     */
    private void editCommoditiesForClusterHeadroom(@Nonnull final TopologyGraph graph,
                    @Nonnull final PlanScope scope,
                    @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {

        if (!TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo)) {
            return;
        }

        if (scope.getScopeEntriesCount() != 1) {
            logger.error("Cluster headroom plan  has invalid scope entry count : "
                            + scope.getScopeEntriesCount());
            return;
        }

        long clusterOid = scope.getScopeEntriesList().get(0).getScopeObjectOid();

        SystemLoadInfoRequest request = SystemLoadInfoRequest.newBuilder()
                            .setClusterId(clusterOid).build();

        SystemLoadInfoResponse response = historyClient.getSystemLoadInfo(request);

        // Order response by VM's oid.
        // Note : Ideally response should contain one record for one comm bought type per VM.
        // So if there is repetition  for "VM1" with comm bought "MEM" it should be flagged
        // here but there are commodities like Storage Amount which have multiple records and
        // are valid. So it becomes difficult to differentiate and hence, currently we rely
        // on response to contain accurate values.
        Map<Long, List<SystemLoadRecord>> resp = response.getRecordList().stream()
            .filter(SystemLoadRecord::hasUuid)
            .collect(Collectors.groupingBy(SystemLoadRecord::getUuid));

        resp.keySet().forEach(oid -> {
            graph.getEntity(oid).ifPresent(vm -> {
                // Create map for this VM and its providers.
                Map<Integer, Queue<Long>> providerIdsByCommodityType =
                                getProviderIdsByCommodityType(vm);
                resp.get(oid).forEach(record -> {
                    double peak = record.getMaxValue();
                    double used = record.getAvgValue();
                    // Update commodity value for this entity
                    CommodityType commType = CommodityType.valueOf(record.getPropertyType());
                    updateCommodityValuesForVmAndProvider(vm, commType, graph,
                                    providerIdsByCommodityType, peak, used);
                });
            });
        });
    }
}
