package com.vmturbo.extractor.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2IntMap;

import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.EntitySeverity;
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

    private GroupData groupData;

    private Map<Long, Map<Integer, Set<Long>>> entityToRelatedEntities;

    private Long2IntMap entityOrGroupToActionCount;

    private final TopologyGraph<SupplyChainEntity> graph;

    DataProvider(@Nonnull MultiStageTimer timer,
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
        return groupData.getEntityToGroups().values().stream()
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
        return groupData.getEntityToGroups().getOrDefault(entityOid, Collections.emptyList());
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
     * @return {@link EntitySeverity} enum generated by jooq
     */
    public EntitySeverity getSeverity(long oid) {
        // todo: fake value for now, fetch severity from AO, like we do in API SeverityPopulator
        return EntitySeverity.MAJOR;
    }
}
