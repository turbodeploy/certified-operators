package com.vmturbo.extractor.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.ints.Int2DoubleArrayMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.ClusterStatsFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.DataFetcher;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory.RICoverageData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * The class which is responsible for fetching data from other components and provides interfaces
 * for use during the ingestion.
 */
public class DataProvider {

    private final GroupServiceBlockingStub groupService;
    // commodity values cached for use later in finish stage
    private final Long2ObjectMap<Int2DoubleMap> entityToCommodityUsed = new Long2ObjectArrayMap<>();
    private final Long2ObjectMap<Int2DoubleMap> entityToCommodityCapacity = new Long2ObjectArrayMap<>();


    // data cached until next cycle
    private volatile GroupData groupData;
    private volatile SupplyChain supplyChain;
    private volatile TopologyGraph<SupplyChainEntity> graph;
    private volatile List<EntityStats> clusterStats;
    private volatile TopDownCostData topDownCostData;
    private volatile BottomUpCostData bottomUpCostData;
    private volatile BottomUpCostData projectedBottomUpCostData;
    private volatile RICoverageData currentRiCoverageData;
    private volatile RICoverageData projectedRiCoverageData;

    private final ClusterStatsFetcherFactory clusterStatsFetcherFactory;
    private final TopDownCostFetcherFactory topDownCostFetcherFactory;
    private final BottomUpCostFetcherFactory bottomUpCostFetcherFactory;
    private final RICoverageFetcherFactory riCoverageFetcherFactory;
    private final ThinTargetCache targetCache;

    DataProvider(GroupServiceBlockingStub groupService,
            ClusterStatsFetcherFactory clusterStatsFetcherFactory,
            TopDownCostFetcherFactory topDownCostFetcherFactory,
            BottomUpCostFetcherFactory bottomUpCostFetcherFactory,
            ThinTargetCache targetCache,
            RICoverageFetcherFactory riCoverageFetcherFactory) {
        this.groupService = groupService;
        this.clusterStatsFetcherFactory = clusterStatsFetcherFactory;
        this.topDownCostFetcherFactory = topDownCostFetcherFactory;
        this.bottomUpCostFetcherFactory = bottomUpCostFetcherFactory;
        this.targetCache = targetCache;
        this.riCoverageFetcherFactory = riCoverageFetcherFactory;
    }

    private final Long2ObjectMap<Boolean> virtualVolumeToEphemeral = new Long2ObjectOpenHashMap<>();
    private final Long2ObjectMap<Boolean> virtualVolumeToEncrypted = new Long2ObjectOpenHashMap<>();

    /**
     * Scraps data from topologyEntityDTO.
     * @param topologyEntityDTO entity
     */
    public void scrapeData(@Nonnull TopologyEntityDTO topologyEntityDTO) {
        scrapeCommodities(topologyEntityDTO);
        scrapeVirtualVolumes(topologyEntityDTO);
    }

    /**
     * Scrape VirtualVolume information.
     * @param topologyEntityDTO entity
     */
    public void scrapeVirtualVolumes(@Nonnull TopologyEntityDTO topologyEntityDTO) {
        if (topologyEntityDTO.getEntityType() != EntityType.VIRTUAL_VOLUME_VALUE) {
            return;
        }
        VirtualVolumeInfo virtualVolumeInfo = topologyEntityDTO.getTypeSpecificInfo().getVirtualVolume();
        if (virtualVolumeInfo.hasIsEphemeral()) {
            this.virtualVolumeToEphemeral.put(topologyEntityDTO.getOid(),
                    (Boolean)virtualVolumeInfo.getIsEphemeral());
        }

        if (virtualVolumeInfo.hasEncryption()) {
            this.virtualVolumeToEncrypted.put(
                    topologyEntityDTO.getOid(),
                    (Boolean)virtualVolumeInfo.getEncryption());
        }
    }

    /**
     * Scrape the commodities we are interested in for use by groups and related entities later.
     * Only those defined in metadata for group aggregated commodity and commodity on related
     * entities are cached.
     *
     * @param topologyEntityDTO entity
     */
    public void scrapeCommodities(@Nonnull TopologyEntityDTO topologyEntityDTO) {
        final Set<Integer> commodityTypes = SearchMetadataUtils.getCommodityTypesToScrape(
                topologyEntityDTO.getEntityType());
        if (commodityTypes.isEmpty()) {
            // nothing to scrape
            return;
        }

        Int2DoubleMap used = new Int2DoubleArrayMap();
        Int2DoubleMap capacity = new Int2DoubleArrayMap();
        // an entity may sell multiple commodities of same type, just sum them
        topologyEntityDTO.getCommoditySoldListList().forEach(commoditySoldDTO -> {
            int commodityType = commoditySoldDTO.getCommodityType().getType();
            if (commodityTypes.contains(commodityType)) {
                used.put(commodityType, used.get(commodityType) + commoditySoldDTO.getUsed());
                capacity.put(commodityType, capacity.get(commodityType) + commoditySoldDTO.getCapacity());
                //TODO: We can't follow the above pattern because we can't sum utilization, we have to average it
                // We'll have to change this whole loop to allow us to address one commodity type at a time
            }
        });
        entityToCommodityUsed.put(topologyEntityDTO.getOid(), used);
        entityToCommodityCapacity.put(topologyEntityDTO.getOid(), capacity);
    }



    /**
     * Fetch data from other components.
     * @param timer a {@link MultiStageTimer} to collect overall timing information
     * @param graph The topology graph contains all entities and relations between entities.
     * @param requireFullSupplyChain whether or not to require full supply chain for all entities
     * @param topologyCreationTime the creation time of the topology currently being ingested
     */
    public void fetchData(@Nonnull MultiStageTimer timer,
                          @Nonnull TopologyGraph<SupplyChainEntity> graph,
                          boolean requireFullSupplyChain, final long topologyCreationTime) {
        this.graph = graph;
        // prepare all needed fetchers
        final List<DataFetcher<?>> dataFetchers = new ArrayList<>();
        dataFetchers.add(new GroupFetcher(groupService, timer, this::setGroupData));
        dataFetchers.add(new SupplyChainFetcher(graph, timer, this::setSupplyChain, requireFullSupplyChain));
        if (clusterStatsFetcherFactory != null) {
            dataFetchers.add(clusterStatsFetcherFactory.getClusterStatsFetcher(this::setClusterStats,
                    topologyCreationTime));
        }
        // run all fetchers in parallel
        dataFetchers.parallelStream().forEach(DataFetcher::fetchAndConsume);
    }

    /**
     * Fetch bottom-up cost data for the given snapshot, it is updated in 'bottomUpCostData'.
     *
     * @param snapshotTime snapshot time of topology whose cost data is needed
     * @param timer        timer to use
     */
    public void fetchBottomUpCostData(long snapshotTime, @Nonnull MultiStageTimer timer) {
        bottomUpCostFetcherFactory.newCurrentCostFetcher(timer, snapshotTime, this::setBottomUpCostData)
                .fetchAndConsume();
    }

    /**
     * Fetch bottom-up cost data for the given snapshot, it is updated in 'bottomUpCostData'.
     *
     * @param timer        timer to use
     */
    public void fetchProjectedBottomUpCostData(@Nonnull MultiStageTimer timer) {
        bottomUpCostFetcherFactory.newProjectedCostFetcher(timer, this::setProjectedBottomUpCostData)
                .fetchAndConsume();
    }

    /**
     * Makes a call to the cost component to fetch the latest billing account expenses.
     * This explicit call is needed to be called for embedded reporting billing data, otherwise
     * data is not available when we get notified from cost that new billing data is available.
     *
     * @param timer Timer to use.
     */
    public void fetchTopDownCostData(@Nonnull MultiStageTimer timer) {
        topDownCostFetcherFactory.newFetcher(timer, this::setTopDownCostData).fetchAndConsume();
    }

    /**
     * Fetch current RI coverage data for all entities.
     *
     * @param timer timer to use
     */
    public void fetchCurrentRICoverageData(@Nonnull MultiStageTimer timer) {
        riCoverageFetcherFactory.newCurrentRiCoverageFetcher(timer, this::setCurrentRiCoverageData)
                .fetchAndConsume();
    }

    /**
     * Fetch projected RI coverage data for the given topology context.
     *
     * @param timer timer to use
     * @param topologyContextId id of the topology context, like real time or plan topology
     */
    public void fetchProjectedRICoverageData(@Nonnull MultiStageTimer timer, long topologyContextId) {
        riCoverageFetcherFactory.newProjectedRiCoverageFetcher(timer,
                this::setProjectedRiCoverageData, topologyContextId).fetchAndConsume();
    }

    /**
     * Clean unneeded data while keeping useful data for actions ingestion.
     */
    public void clean() {
        entityToCommodityUsed.clear();
        entityToCommodityCapacity.clear();
        virtualVolumeToEphemeral.clear();
        virtualVolumeToEncrypted.clear();
    }

    private void setGroupData(GroupData groupData) {
        this.groupData = groupData;
    }

    private void setSupplyChain(SupplyChain supplyChain) {
        this.supplyChain = supplyChain;
    }

    private void setClusterStats(List<EntityStats> clusterStats) {
        this.clusterStats = clusterStats;
    }

    private void setTopDownCostData(TopDownCostData topDownCostData) {
        this.topDownCostData = topDownCostData;
    }

    private void setBottomUpCostData(BottomUpCostData bottomUpCostData) {
        this.bottomUpCostData = bottomUpCostData;
    }

    private void setProjectedBottomUpCostData(BottomUpCostData projectedBottomUpCostData) {
        this.projectedBottomUpCostData = projectedBottomUpCostData;
    }

    public void setCurrentRiCoverageData(RICoverageData currentRiCoverageData) {
        this.currentRiCoverageData = currentRiCoverageData;
    }

    public void setProjectedRiCoverageData(RICoverageData projectedRiCoverageData) {
        this.projectedRiCoverageData = projectedRiCoverageData;
    }

    /**
     * Get all groups.
     *
     * @return all groups.
     */
    public Stream<Grouping> getAllGroups() {
        return groupData.getLeafEntityToGroups().values().stream()
                .flatMap(Collection::stream)
                .distinct();
    }

    /**
     * Get all targets.
     *
     * @return all targets in a stream.
     */
    public Stream<ThinTargetInfo> getAllTargets() {
        return targetCache.getAllTargets().stream();
    }

    /**
     * Get the latest group data.
     *
     * @return {@link GroupData} containing data for all groups
     */
    public GroupData getGroupData() {
        return groupData;
    }

    /**
     * Get the cluster stats.
     *
     * @return List of {@link EntityStats} containing stats for clusters
     */
    public List<EntityStats> getClusterStats() {
        return clusterStats;
    }

    /**
     * Get the latest group to leaf entities map.
     *
     * @return map from group id to its leaf entities
     */
    @Nullable
    public Long2ObjectMap<List<Long>> getGroupToLeafEntities() {
        return groupData != null && groupData.getGroupToLeafEntityIds() != null
                ? groupData.getGroupToLeafEntityIds() : null;
    }

    /**
     * Get all the groups which contains the given entity.
     *
     * @param entityOid oid of entity
     * @return list of groups
     */
    public List<Grouping> getGroupsForEntity(long entityOid) {
        return groupData == null ? Collections.emptyList() : groupData.getGroupsForEntity(entityOid);
    }

    /**
     * Get direct member count for the given group. For example: if a group contains 2 clusters,
     * it will return 2.
     *
     * @param groupId group id
     * @return direct member count
     */
    public int getGroupDirectMembersCount(long groupId) {
        final List<Long> members = groupData.getGroupToDirectMemberIds().get(groupId);
        return members != null ? members.size() : 0;
    }

    /**
     * Get direct member count for the given group and given entity type. For example: if a group
     * contains 2 hosts and 1 vm, it will return 2 if requested entityType is host.
     *
     * @param groupId group id
     * @param entityType entity type
     * @return direct member count
     */
    public int getGroupDirectMembersCount(long groupId, EntityType entityType) {
        final List<Long> members = groupData.getGroupToDirectMemberIds().get(groupId);
        if (members == null) {
            return 0;
        }
        return (int)members.stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(entity -> entity.getEntityType() == entityType.getNumber())
                .count();
    }

    /**
     * Get indirect member count for the given group, which is the leaf entities count.
     * For example: if a group contains 2 clusters, each cluster contain 2 hosts, it will return 4.
     *
     * @param groupId group id
     * @return indirect member count
     */
    public int getGroupIndirectMembersCount(long groupId) {
        final List<Long> members = groupData.getGroupToLeafEntityIds().get(groupId);
        return members != null ? members.size() : 0;
    }

    /**
     * Get indirect member count for the given group and entity type.
     * For example: if a group contains 1 cluster and 1 vm group, the cluster contain 2 hosts,
     * and vm group contains 3 vms, it will return 3 if requested entityType is vm.
     *
     * @param groupId group id
     * @param entityType type of the entity
     * @return direct member count
     */
    public int getGroupIndirectMembersCount(long groupId, EntityType entityType) {
        return (int)getGroupLeafEntitiesOfType(groupId, entityType).count();
    }

    /**
     * Get the leaf entities of provided entity type on the given group.
     *
     * @param groupId group id
     * @param entityType type of the entity
     * @return stream of entities
     */
    public Stream<Long> getGroupLeafEntitiesOfType(long groupId, EntityType entityType) {
        final List<Long> leafEntities = groupData.getGroupToLeafEntityIds().get(groupId);
        if (leafEntities == null) {
            return Stream.empty();
        }
        return leafEntities.stream()
                .filter(entityId -> graph.getEntity(entityId)
                        .map(entity -> entity.getEntityType() == entityType.getNumber())
                        .orElse(false));
    }

    /**
     * Get the related entities count of specified entity types for the given group. For example:
     * if a group contains 1 host, and the host hosts 2 vms, then related vms count for the group
     * is 2.
     *
     * @param groupId group id
     * @param relatedEntityTypes related types of the entity
     * @return related entities count for the group
     */
    public int getGroupRelatedEntitiesCount(long groupId, Set<EntityType> relatedEntityTypes) {
        return (int)
                getGroupRelatedEntities(groupId, relatedEntityTypes)
                .count();
    }

    /**
     * Get the related entities names of group considering EntityType.
     *
     * @param groupId group id
     * @param relatedEntityTypes related types of the entity
     * @return related entities count for the group
     */
    public List<String> getGroupRelatedEntitiesNames(long groupId, Set<EntityType> relatedEntityTypes) {
        return getGroupRelatedEntities(groupId, relatedEntityTypes)
                .map(this::getDisplayName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Get the related entities oids for groupOid considering EntityType.
     *
     * @param groupId group id
     * @param relatedEntityTypes related types of the entity
     * @return related entities count for the group
     */
    public Stream<Long> getGroupRelatedEntities(long groupId, Set<EntityType> relatedEntityTypes) {
        List<Long> leafEntities = groupData.getGroupToLeafEntityIds().get(groupId);
        if (leafEntities == null) {
            return Stream.empty();
        }
        return leafEntities.stream()
                .flatMap(entityOid -> relatedEntityTypes.stream().flatMap(relatedEntityType ->
                        getRelatedEntitiesOfType(entityOid, relatedEntityType).stream()))
                .distinct();
    }

    /**
     * Get all the related entities for the given entity in the SupplyChain.
     *
     * @param entityOid oid of entity
     * @return set of related entities oids
     */
    public LongSet getRelatedEntities(long entityOid) {
        return supplyChain.getRelatedEntities(entityOid).values().stream()
                .flatMap(Collection::stream)
                .mapToLong(Long::longValue)
                .collect(LongOpenHashSet::new, LongSet::add, LongSet::addAll);
    }

    /**
     * Get the ids of related entities of specified type for the given entity in the SupplyChain.
     *
     * @param entityOid oid of entity
     * @param relatedEntityType type of related entity
     * @return set of related entities oids
     */
    public Set<Long> getRelatedEntitiesOfType(long entityOid, EntityType relatedEntityType) {
        return getRelatedEntitiesOfType(entityOid, relatedEntityType.getNumber());
    }

    /**
     * Get the ids of related entities of specified type for the given entity in the SupplyChain.
     *
     * @param entityOid oid of entity
     * @param relatedEntityType int value of related entity type
     * @return set of related entities oids
     */
    public Set<Long> getRelatedEntitiesOfType(long entityOid, int relatedEntityType) {
        return supplyChain.getRelatedEntitiesOfType(entityOid, relatedEntityType);
    }

    /**
     * Get the displayNames of related entity of specified type for the given entity in the SupplyChain.
     *
     * @param entityOid oid of entity
     * @param relatedEntityType type of related entity
     * @return set of related entities displayNames
     */
    public List<String> getRelatedEntityNames(long entityOid, EntityType relatedEntityType) {
        return getRelatedEntitiesOfType(entityOid, relatedEntityType).stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(SupplyChainEntity::getDisplayName)
                .collect(Collectors.toList());
    }

    /**
     * Get the severity for the given entity/group.
     *
     * @param oid id of the entity or group
     * @return {@link Severity} enum generated by jooq
     */
    public Severity getSeverity(long oid) {
        // todo: fake value for now, fetch severity from AO, like we do in API SeverityPopulator
        return Severity.MAJOR;
    }

    /**
     * Get the optional used value for the commodity type on the given entity.
     *
     * @param entityId id of entity
     * @param commodityType type of commodity
     * @return optional used value
     */
    public OptionalDouble getCommodityUsed(long entityId, int commodityType) {
        if (entityToCommodityUsed.containsKey(entityId)) {
            Int2DoubleMap commodityMap = entityToCommodityUsed.get(entityId);
            if (commodityMap.containsKey(commodityType)) {
                return OptionalDouble.of(commodityMap.get(commodityType));
            }
        }
        return OptionalDouble.empty();
    }

    /**
     * Get the optional capacity value for the commodity type on the given entity.
     *
     * @param entityId id of entity
     * @param commodityType type of commodity
     * @return optional capacity value
     */
    public OptionalDouble getCommodityCapacity(long entityId, int commodityType) {
        if (entityToCommodityCapacity.containsKey(entityId)) {
            Int2DoubleMap commodityMap = entityToCommodityCapacity.get(entityId);
            if (commodityMap.containsKey(commodityType)) {
                return OptionalDouble.of(commodityMap.get(commodityType));
            }
        }
        return OptionalDouble.empty();
    }

    /**
     * Get the optional utilization value for the commodity type on the given entity. If used or
     * capacity is not available, then it returns empty.
     *
     * @param entityId id of entity
     * @param commodityType type of commodity
     * @return optional utilization value
     */
    public OptionalDouble getCommodityUtilization(long entityId, int commodityType) {
        OptionalDouble capacity = getCommodityCapacity(entityId, commodityType);
        if (!capacity.isPresent() || capacity.getAsDouble() == 0) {
            return OptionalDouble.empty();
        }

        OptionalDouble used = getCommodityUsed(entityId, commodityType);
        if (!used.isPresent()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(used.getAsDouble() / capacity.getAsDouble());
    }

    /**
     * Get display name for the given entity.
     *
     * @param entityOid id of the entity
     * @return optional display name
     */
    public Optional<String> getDisplayName(long entityOid) {
        return graph.getEntity(entityOid).map(SupplyChainEntity::getDisplayName);
    }

    /**
     * Get entity type for the given entity.
     *
     * @param entityOid id of the entity
     * @return optional integer value of entity type
     */
    public Optional<Integer> getEntityType(long entityOid) {
        return graph.getEntity(entityOid).map(SupplyChainEntity::getEntityType);
    }

    /**
     * Get a stream of target OIDs that discovered a given entity.
     *
     * @param entityOid an entity OID
     * @return stream of target OIDs
     */
    @Nonnull
    public Stream<Long> getDiscoveryTargets(long entityOid) {
        TopologyGraphEntity<SupplyChainEntity> entity = graph.getEntity(entityOid).orElse(null);
        if (entity != null) {
            return entity.getDiscoveringTargetIds();
        }
        return Stream.empty();
    }

    /**
     * Gets if VirtualVolume isEphemeral.
     * @param virtualVolumeOid virtualVolume oid
     * @return boolean is known, otherwise null
     */
    @Nullable
    public Boolean virtualVolumeIsEphemeral(long virtualVolumeOid) {
        return virtualVolumeToEphemeral.get(virtualVolumeOid);
    }

    /**
     * Gets if VirtualVolume isEncrypted.
     * @param virtualVolumeOid virtualVolume oid
     * @return boolean is known, otherwise null
     */
    @Nullable
    public Boolean virtualVolumeIsEncrypted(long virtualVolumeOid) {
        return virtualVolumeToEncrypted.get(virtualVolumeOid);
    }

    /**
     * Return the latest topology graph.
     *
     * @return {@link TopologyGraph}
     */
    public TopologyGraph<SupplyChainEntity> getTopologyGraph() {
        return graph;
    }

    @Nullable
    public TopDownCostData getTopDownCostData() {
        return topDownCostData;
    }

    @Nullable
    public BottomUpCostData getBottomUpCostData() {
        return bottomUpCostData;
    }

    public BottomUpCostData getProjectedBottomUpCostData() {
        return projectedBottomUpCostData;
    }

    public RICoverageData getCurrentRiCoverageData() {
        return currentRiCoverageData;
    }

    public RICoverageData getProjectedRiCoverageData() {
        return projectedRiCoverageData;
    }

    /**
     * Get the latest calculated supply chain.
     *
     * @return related entities in supply chain for entity for different entity types
     */
    public SupplyChain getSupplyChain() {
        return supplyChain;
    }
}
