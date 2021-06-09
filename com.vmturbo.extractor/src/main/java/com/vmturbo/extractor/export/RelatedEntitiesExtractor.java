package com.vmturbo.extractor.export;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Extract related entities (and groups) in the supply chain for an entity.
 */
public class RelatedEntitiesExtractor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Include all types of related entities.
     */
    public static final Predicate<Integer> INCLUDE_ALL_RELATED_ENTITY_TYPES = relatedEntityType -> true;

    private final TopologyGraph<SupplyChainEntity> graph;
    private final SupplyChain supplyChain;
    private final GroupData groupData;
    private final RelatedEntityMapper relatedEntityMapper = new RelatedEntityMapper();

    /**
     * Constructor for {@link RelatedEntitiesExtractor}.
*
     * @param graph topology graph containing all entities
     * @param supplyChain supply chain data for all entities
     * @param groupData containing all groups related data
     */
    public RelatedEntitiesExtractor(@Nonnull final TopologyGraph<SupplyChainEntity> graph,
            @Nonnull final SupplyChain supplyChain, @Nullable final GroupData groupData) {
        this.graph = graph;
        this.supplyChain = supplyChain;
        this.groupData = groupData;
    }

    /**
     * Extract related entities and groups for given entity.
     *
     * @param entityOid oid of the entity to get related entities for
     * @param relatedEntityTypeFilter filter for evaluating which type of related entity to include
     * @return mapping from related entity/group type to list of related entities/groups
     */
    @Nullable
    public Map<String, List<RelatedEntity>> extractRelatedEntities(long entityOid,
            Predicate<Integer> relatedEntityTypeFilter) {
        final Map<String, List<RelatedEntity>> relatedEntities = new HashMap<>();
        final Map<Integer, Set<Long>> relatedEntitiesByType = getRelatedEntitiesByType(entityOid,
                relatedEntityTypeFilter);
        // related entities
        relatedEntitiesByType.forEach((relatedEntityType, relatedEntityIds) -> {
            final List<RelatedEntity> relatedEntityList = relatedEntityIds.stream()
                    // do not include the entity itself
                    .filter(relatedEntityId -> relatedEntityId != entityOid)
                    .map(graph::getEntity)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(relatedEntityMapper::createRelatedEntity)
                    .collect(Collectors.toList());

            if (!relatedEntityList.isEmpty()) {
                final String entityTypeJsonKey = ExportUtils.getEntityTypeJsonKey(relatedEntityType);
                if (entityTypeJsonKey != null) {
                    relatedEntities.put(entityTypeJsonKey, relatedEntityList);
                } else {
                    logger.debug("Invalid entity type {} for entity {}", relatedEntityType, entityOid);
                }
            }
        });

        // related groups
        getRelatedGroups(relatedEntitiesByType.values().stream().flatMap(Collection::stream))
                .collect(Collectors.groupingBy(grouping -> grouping.getDefinition().getType()))
                .forEach((groupType, groupings) -> {
                    List<RelatedEntity> relatedGroups = groupings.stream()
                            .map(relatedEntityMapper::createRelatedEntity)
                            .collect(Collectors.toList());
                    if (!relatedGroups.isEmpty()) {
                        relatedEntities.put(ExportUtils.getGroupTypeJsonKey(groupType), relatedGroups);
                    }
                });

        return relatedEntities.isEmpty() ? null : relatedEntities;
    }

    /**
     * Extract all related entities and groups for given entity.
     *
     * @param entityOid oid of the entity to get related entities for
     * @return mapping from related entity/group type to list of related entities/groups
     */
    public Map<String, List<RelatedEntity>> extractRelatedEntities(long entityOid) {
        return extractRelatedEntities(entityOid, INCLUDE_ALL_RELATED_ENTITY_TYPES);
    }

    /**
     * Get the related entities for given entity, grouped by entity type.
     *
     * @param entityOid oid of the entity to get related entities for
     * @param relatedEntityTypeFilter filter for evaluating which type of related entity to include
     * @return related entities grouped by type
     */
    @Nonnull
    public Map<Integer, Set<Long>> getRelatedEntitiesByType(long entityOid, Predicate<Integer> relatedEntityTypeFilter) {
        return supplyChain.getRelatedEntities(entityOid).entrySet().stream()
                .filter(entry -> relatedEntityTypeFilter.test(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, entry -> {
                    final Integer relatedEntityType = entry.getKey();
                    Set<Long> relatedEntityIds = entry.getValue();
                    // size check to reduce unnecessary logic inside if block
                    if (relatedEntityType == EntityType.BUSINESS_ACCOUNT_VALUE && relatedEntityIds.size() > 1) {
                        // if the entity is owned by an account, then we should only include the
                        // owner account, not the account which owns the owner account, since it
                        // has already been represented as related billing family
                        Optional<Long> ownerAccountId = getOwnerAccountId(entityOid);
                        if (ownerAccountId.isPresent()) {
                            relatedEntityIds = Collections.singleton(ownerAccountId.get());
                        }
                    }
                    return relatedEntityIds;
                }));
    }

    /**
     * Get all related entities for given entity, grouped by entity type.
     *
     * @param entityOid oid of the entity to get related entities for
     * @return related entities grouped by type
     */
    public Map<Integer, Set<Long>> getRelatedEntitiesByType(long entityOid) {
        return getRelatedEntitiesByType(entityOid, INCLUDE_ALL_RELATED_ENTITY_TYPES);
    }

    /**
     * Get the related groups for given entities.
     *
     * @param relatedEntities stream of entity oids
     * @return stream of groups which contain the given entities as members
     */
    @Nonnull
    public Stream<Grouping> getRelatedGroups(Stream<Long> relatedEntities) {
        if (groupData == null) {
            return Stream.empty();
        }
        return relatedEntities.map(groupData::getGroupsForEntity)
                .flatMap(List::stream)
                .distinct();
    }

    /**
     * Get the id of the owner account for the given entity. If the entity is not owned by account,
     * then it returns empty.
     *
     * @param entityOid oid of entity
     * @return owner account of entity
     */
    private Optional<Long> getOwnerAccountId(long entityOid) {
        return graph.getEntity(entityOid)
                .flatMap(SupplyChainEntity::getOwner)
                .filter(owner -> owner.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .map(SupplyChainEntity::getOid);
    }
}
