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
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.OSMigration;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.mediation.hybrid.cloud.common.OsType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

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

    // Providers from which LICENSE_ACCESS can be bought. Physical Machine only would
    // buy it in the case of on-prem VMs which are migrating to the cloud.
    private static final Set<Integer> LICENSE_PROVIDER_TYPES = ImmutableSet.of(
        EntityType.COMPUTE_TIER_VALUE, EntityType.PHYSICAL_MACHINE_VALUE);

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
     * @param cloneEntityOids in migration plans, the oids of cloned entities to be migrated,
     */
    public void applyCommodityEdits(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                    @Nonnull final List<ScenarioChange> changes,
                                    @Nonnull TopologyInfo topologyInfo,
                                    @Nonnull final PlanScope scope,
                                    @Nullable final Set<Long> cloneEntityOids) {
        editCommoditiesForBaselineChanges(graph, changes, topologyInfo);
        editCommoditiesForClusterHeadroom(graph, scope, topologyInfo);
        if (cloneEntityOids != null) {
            editCommoditiesForMigrateToCloud(graph, changes, cloneEntityOids);
        }
    }

    /**
     * Change commodity values for VMs and its providers if a scenario change
     * related to historical baseline exists.
     * @param graph to extract entities from.
     * @param changes to iterate over and find relevant to baseline change.
     * @param topologyInfo to find VMs in current scope.
     */
    private void editCommoditiesForBaselineChanges(@Nonnull final TopologyGraph<TopologyEntity> graph,
                    @Nonnull final List<ScenarioChange> changes,
                    TopologyDTO.TopologyInfo topologyInfo) {
        changes.stream()
            .filter(change -> change.getPlanChanges().hasHistoricalBaseline())
            .findFirst()
            .ifPresent(change -> {
                final Set<TopologyEntity> vmSet = getPlanVms(graph, topologyInfo);
                long baselineDate = change.getPlanChanges()
                    .getHistoricalBaseline().getBaselineDate();
                fetchAndApplyHistoricalData(vmSet, graph, baselineDate);
            });
    }

    /**
     * Change commodity values for VMs and its providers if a scenario change
     * related to migration to cloud exists.
     *  @param graph to extract entities from.
     * @param changes to iterate over and find relevant to baseline change.
     * @param cloneEntityOids the oids of the entities to be migrated, cloned from the source
     *                        entities.
     */
    private void editCommoditiesForMigrateToCloud(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                                  @Nonnull final List<ScenarioChange> changes,
                                                  @Nonnull final Set<Long> cloneEntityOids) {
        for (ScenarioChange change : changes) {
            if (change.hasTopologyMigration()) {
                final Map<OSType, String> licenseCommodityKeyByOS =
                    computeLicenseCommodityKeysByOS(change.getTopologyMigration());

                for (TopologyEntity vm : getCloneVMs(graph, cloneEntityOids)) {
                    String licenseCommodityKey = licenseCommodityKeyByOS.getOrDefault(
                        vm.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsType(),
                        OsType.UNKNOWN.name()
                    );

                    updateAccessCommodityForVmAndProviders(vm, LICENSE_PROVIDER_TYPES,
                        CommodityType.LICENSE_ACCESS, licenseCommodityKey);

                    // TODO: this would also be the place, for migrations to Azure, where
                    // if the migrating VM is not buying IO_THROUGHPUT from storage,
                    // to add it in proportion to STORAGE_ACCESS so that we get reasonable
                    // cost estimates for Ultra Disk.
                }
            }
        }
    }

    /**
     * Compute a map of each source OS value to the LICENSE_ACCESS commodity key to buy,
     * considering any configuration provided in the scenario regarding remapped OSes
     * and Bring Yoyr Own License options.
     *
     * @param migrationScenario May contain a lit of os migration options to apply which
     *                          alter the default mapping.
     * @return A map from each possible OS type to the LICENSE_ACCESS commodty key to buy.
     */
    @Nonnull
    private Map<OSType, String> computeLicenseCommodityKeysByOS(@Nonnull TopologyMigration migrationScenario) {
        Map<OsType, OSMigration> licenseTranslations = migrationScenario.getOsMigrationsList()
            .stream().collect(Collectors.toMap(
                migration -> OsType.fromDtoOS(migration.getFromOs()),
                Function.identity()));

        ImmutableMap.Builder<OSType, String> licensingMap = ImmutableMap.builder();
        for (OSType os : OSType.values()) {
            // 1. Convert subtypes like LINUX_FOO/WINDOWS_FOO to the general category OS
            OsType categoryOs = OsType.fromDtoOS(os).getCategoryOs();

            // 2. Apply changes from the scenario, or default to migrate to the same OS without
            // Bring Your Own License otherwise.
            OsType destinationOS = categoryOs;
            boolean byol = false;
            OSMigration osMigration = licenseTranslations.get(categoryOs);
            if (osMigration != null) {
                destinationOS = OsType.fromDtoOS(osMigration.getToOs());
                byol = osMigration.getByol();
            }

            // 3. Look up the appropriate license commodity key given the target OS and whether
            // the customer is going to BYOL.
            if (byol) {
                destinationOS = destinationOS.getByolOs();
            }

            licensingMap.put(os, destinationOS.getName());
        }

        return licensingMap.build();
    }

    /**
     * Get historical data for given baselineDate from database and
     * apply to relevant entities in graph.
     * @param vmSet if empty -> fetch all VMs' data otherwise only for given set.
     * @param graph which entities belong to.
     * @param baselineDate for which data is fetched for.
     */
    private void fetchAndApplyHistoricalData(Set<TopologyEntity> vmSet,
                                             final TopologyGraph<TopologyEntity> graph, long baselineDate) {
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

    private void updateAccessCommodityForVmAndProviders(@Nonnull TopologyEntity vm,
                                                        @Nonnull Set<Integer> providerTypeIds,
                                                        @Nonnull CommodityType commodityType,
                                                        @Nonnull String newKey) {
        Builder vmBuilder = vm.getTopologyEntityDtoBuilder();

        List<CommoditiesBoughtFromProvider> originalCommoditiesBoughtFromProviderList =
            vmBuilder.getCommoditiesBoughtFromProvidersList();

        vm.getTopologyEntityDtoBuilder().clearCommoditiesBoughtFromProviders();

        for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider
            : originalCommoditiesBoughtFromProviderList) {
            if (providerTypeIds.contains(commoditiesBoughtFromProvider.getProviderEntityType())) {
                commoditiesBoughtFromProvider =
                    updateAccessCommodityKey(commoditiesBoughtFromProvider, commodityType, newKey);
            }

            vmBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
        }
    }

    private CommoditiesBoughtFromProvider updateAccessCommodityKey(
        @Nonnull CommoditiesBoughtFromProvider commoditiesBoughtFromProvider,
        @Nonnull CommodityType commodityType,
        @Nonnull String newKey) {

        CommoditiesBoughtFromProvider.Builder newCommoditiesBoughtFromProviderBuilder
            = commoditiesBoughtFromProvider.toBuilder().clearCommodityBought();

        boolean foundCommodity = false;

        for (CommodityBoughtDTO commodityBought
            : commoditiesBoughtFromProvider.getCommodityBoughtList()) {

            // If this is the commodity type we're looking to change, rebuild it with the new key
            if (commodityBought.getCommodityType().getType() == commodityType.getNumber()) {
                commodityBought = commodityBought.toBuilder().setCommodityType(
                    commodityBought.getCommodityType().toBuilder().setKey(newKey)
                ).build();

                foundCommodity = true;
            }

            newCommoditiesBoughtFromProviderBuilder.addCommodityBought(commodityBought);
        }

        if (!foundCommodity) {
            // Add commodity since it wasn't present
            newCommoditiesBoughtFromProviderBuilder.addCommodityBought(
                CommodityBoughtDTO.newBuilder().setCommodityType(
                    TopologyDTO.CommodityType.newBuilder()
                        .setType(commodityType.getNumber())
                        .setKey(newKey)
                )
            );
        }

        return newCommoditiesBoughtFromProviderBuilder.build();
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
                    final TopologyGraph<TopologyEntity> graph,
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
            CommoditySoldDTO.Builder commBuilder = commoditySold.get();
            if (peak < 0) {
                logger.error("Peak quantity = {} for commodity type {} of topology entity {}",
                        peak, commoditySold.get().getCommodityType(), vm.getDisplayName());
            }

            commBuilder.setUsed(used);
            commBuilder.setPeak(peak);
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
                        double newUsed = Math.max(commSold.getUsed() - commodityBought.getUsed(), 0) + used;
                        commSold.setUsed(newUsed);
                        commSold.setPeak(newPeak);
                    }

                    // Set value for consumer.
                    // Note : If we have multiple providers on baseline date and one provider currently
                    // we will set value for commodity bought from last record. It is not a problem because
                    // price in market is calculated by providers based on commodity sold. Mismatch in commodity
                    // bought and sold values will be reflected as overhead.
                    commodityBought.setUsed(used);
                    commodityBought.setPeak(peak);
                });
            });
        }
    }

    @Nonnull
    private Set<TopologyEntity> getPlanVms(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                           @Nonnull TopologyDTO.TopologyInfo topologyInfo) {
        return topologyInfo.getScopeSeedOidsList().stream()
            .distinct()
            .flatMap(oid -> findVMsInScope(graph, oid))
            .collect(Collectors.toSet());
    }

    @Nonnull
    private Set<TopologyEntity> getCloneVMs(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                            @Nonnull final Set<Long> cloneEntityOids) {
        return cloneEntityOids.stream()
            .map(oid -> graph.getEntity(oid))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .collect(Collectors.toSet());
    }

    /**
     * Use given scope oid to traverse up or down the supply chain and find related VMs.
     * @param graph to traverse on for providers/consumers.
     * @param oid in current scope.
     * @return stream of VMs in scope found via supply chain traversal.
     */
    private Stream<TopologyEntity> findVMsInScope(final TopologyGraph<TopologyEntity> graph, long oid) {
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
    private void editCommoditiesForClusterHeadroom(@Nonnull final TopologyGraph<TopologyEntity> graph,
                    @Nonnull final PlanScope scope,
                    @Nonnull final TopologyDTO.TopologyInfo topologyInfo) {

        if (!TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo)) {
            return;
        }

        SystemLoadInfoRequest.Builder requestBuilder = SystemLoadInfoRequest.newBuilder();
        scope.getScopeEntriesList().stream().map(PlanScopeEntry::getScopeObjectOid)
            .forEach(requestBuilder::addClusterId);

        historyClient.getSystemLoadInfo(requestBuilder.build())
            .forEachRemaining(response -> {
                final long clusterId = response.getClusterId();
                if (response.hasError()) {
                    logger.error("Limited system load data will be applied to the" +
                            " cluster {}. Failed to retrieve system load info due to error: {}",
                        clusterId, response.getError());
                }

                // Order response by VM's oid.
                // Note : Ideally response should contain one record for one comm bought type per VM.
                // So if there is repetition  for "VM1" with comm bought "MEM" it should be flagged
                // here but there are commodities like Storage Amount which have multiple records and
                // are valid. So it becomes difficult to differentiate and hence, currently we rely
                // on response to contain accurate values.
                final Map<Long, List<SystemLoadRecord>> oidToSystemLoadInfo = response.getRecordList().stream()
                    .filter(SystemLoadRecord::hasUuid)
                    .collect(Collectors.groupingBy(SystemLoadRecord::getUuid));

                oidToSystemLoadInfo.keySet().forEach(oid -> {
                    graph.getEntity(oid).ifPresent(vm -> {
                        // Create map for this VM and its providers.
                        Map<Integer, Queue<Long>> providerIdsByCommodityType =
                            getProviderIdsByCommodityType(vm);
                        oidToSystemLoadInfo.get(oid).forEach(record -> {
                            double peak = record.getMaxValue();
                            double used = record.getAvgValue();
                            // Update commodity value for this entity
                            CommodityType commType = CommodityType.valueOf(record.getPropertyType());
                            updateCommodityValuesForVmAndProvider(vm, commType, graph,
                                providerIdsByCommodityType, peak, used);
                        });
                    });
                });
        });
    }

}
