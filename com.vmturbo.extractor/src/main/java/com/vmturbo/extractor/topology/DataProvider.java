package com.vmturbo.extractor.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import it.unimi.dsi.fastutil.ints.Int2DoubleArrayMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.fetcher.ActionFetcher;
import com.vmturbo.extractor.topology.fetcher.DataFetcher;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * The class which is responsible for fetching data from other components and provides interfaces
 * for use during the ingestion.
 */
public class DataProvider {

    private static final Logger logger = LogManager.getLogger();

    // commodity values cached for use later in finish stage
    private final Long2ObjectMap<Int2DoubleMap> entityToCommodityUsed = new Long2ObjectArrayMap<>();
    private final Long2ObjectMap<Int2DoubleMap> entityToCommodityCapacity = new Long2ObjectArrayMap<>();

    /**
     * Cache the historical utilization value (when present) for each entity, for use in calculating
     * average group utilization (currently only applies to Clusters).
     *
     * <p>The historical utilization is a percentage; there are multiple ways it can be calculated.
     * If the percentile-based calculation is available, that will be used to populate this value.
     * Otherwise, if the older weighted-average calculation is available then that will be used.
     * Entities with no historical utilization will be omitted.</p>
     */
    private final Long2ObjectMap<Int2DoubleMap> entityToCommodityHistoricalUtilization = new Long2ObjectArrayMap<>();

    private GroupData groupData;

    private Map<Long, Map<Integer, Set<Long>>> entityToRelatedEntities;

    private Long2IntMap entityOrGroupToActionCount;

    private TopologyGraph<SupplyChainEntity> graph;

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
        //entityToCommodityHistoricalUtilization.put(topologyEntityDTO.getOid(), historicalUtilization);
    }

    /**
     * Fetch data from other components.
     *
     * @param timer a {@link MultiStageTimer} to collect overall timing information
     * @param graph The topology graph contains all entities and relations between entities.
     * @param groupService group rpc service
     * @param actionService action rpc service
     * @param topologyContextId topology context id
     */
    public void fetchData(@Nonnull MultiStageTimer timer,
                          @Nonnull TopologyGraph<SupplyChainEntity> graph,
                          @Nonnull GroupServiceBlockingStub groupService,
                          @Nonnull ActionsServiceBlockingStub actionService,
                          long topologyContextId) {
        this.graph = graph;
        // run basic fetchers first since other fetchers may depend on them
        // like: ActionFetcher needs the groups data to fetch action count for groups
        new GroupFetcher(groupService, timer, (data) -> this.groupData = data).fetchAndConsume();

        // prepare all other needed fetchers
        final List<DataFetcher<?>> dataFetchers = ImmutableList.of(
                new SupplyChainFetcher(graph, timer, (data) -> this.entityToRelatedEntities = data),
                new ActionFetcher(actionService, groupData, timer,
                        (data) -> this.entityOrGroupToActionCount = data, topologyContextId)
                // todo: add more fetchers for severity, cost, etc
        );
        // run other fetchers sequentially
        // todo: evaluate whether running them in parallel gives us a performance benefit
        dataFetchers.forEach(DataFetcher::fetchAndConsume);
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
     * Get all the groups which contains the given entity.
     *
     * @param entityOid oid of entity
     * @return list of groups
     */
    public List<Grouping> getGroupsForEntity(long entityOid) {
        return groupData.getLeafEntityToGroups().getOrDefault(entityOid, Collections.emptyList());
    }

    /**
     * Get direct member count for the given group. For example: if a group contains 2 clusters,
     * it will return 2.
     *
     * @param groupId group id
     * @return direct member count
     */
    public int getGroupDirectMembersCount(long groupId) {
        return groupData.getGroupToDirectMemberIds().getOrDefault(groupId, Collections.emptyList()).size();
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
        return (int)groupData.getGroupToDirectMemberIds().getOrDefault(groupId, Collections.emptyList())
                .stream()
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
        return groupData.getGroupToLeafEntityIds().getOrDefault(groupId, Collections.emptyList()).size();
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
        return groupData.getGroupToLeafEntityIds().getOrDefault(groupId, Collections.emptyList())
                .stream()
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
        return (int)groupData.getGroupToLeafEntityIds()
                .getOrDefault(groupId, Collections.emptyList())
                .stream()
                .flatMap(entityOid -> relatedEntityTypes.stream().flatMap(relatedEntityType ->
                        getRelatedEntitiesOfType(entityOid, relatedEntityType).stream()))
                .distinct()
                .count();
    }

    /**
     * Get all the related entities for the given entity in the SupplyChain.
     *
     * @param entityOid oid of entity
     * @return set of related entities oids
     */
    public Set<Long> getRelatedEntities(long entityOid) {
        return entityToRelatedEntities.getOrDefault(entityOid, Collections.emptyMap()).values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Get the ids of related entities of specified type for the given entity in the SupplyChain.
     *
     * @param entityOid oid of entity
     * @param relatedEntityType type of related entity
     * @return set of related entities oids
     */
    public Set<Long> getRelatedEntitiesOfType(long entityOid, EntityType relatedEntityType) {
        return entityToRelatedEntities.getOrDefault(entityOid, Collections.emptyMap())
                .getOrDefault(relatedEntityType.getNumber(), Collections.emptySet());
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
     * Get the action count for the given entity/group.
     *
     * @param oid id of entity or group
     * @return action count for the given entity/group
     */
    public int getActionCount(long oid) {
        return entityOrGroupToActionCount.getOrDefault(oid, 0);
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
}
