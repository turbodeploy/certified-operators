package com.vmturbo.extractor.export;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Extract related entities (and groups) in the supply chain for an entity.
 */
public class RelatedEntitiesExtractor {

    private static final Logger logger = LogManager.getLogger();

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
     * @return mapping from related entity/group type to list of related entities/groups
     */
    @Nullable
    public Map<String, List<RelatedEntity>> extractRelatedEntities(long entityOid) {
        final Map<String, List<RelatedEntity>> relatedEntities = new HashMap<>();
        final Map<Integer, Set<Long>> relatedEntitiesByType = supplyChain.getRelatedEntities(entityOid);
        relatedEntitiesByType.forEach((relatedEntityType, relatedEntityIds) -> {
            final List<RelatedEntity> relatedEntityList = new ArrayList<>();
            for (long relatedEntityId : relatedEntityIds) {
                // do not include the entity itself
                if (relatedEntityId == entityOid) {
                    continue;
                }
                graph.getEntity(relatedEntityId)
                    .map(relatedEntityMapper::createRelatedEntity)
                    .ifPresent(relatedEntityList::add);
            }

            if (!relatedEntityList.isEmpty()) {
                final String entityTypeJsonKey = ExportUtils.getEntityTypeJsonKey(relatedEntityType);
                if (entityTypeJsonKey != null) {
                    relatedEntities.put(entityTypeJsonKey, relatedEntityList);
                } else {
                    logger.debug("Invalid entity type {} for entity {}", relatedEntityType, entityOid);
                }
            }
        });

        // related group
        if (groupData != null) {
            relatedEntitiesByType.values().stream()
                    .flatMap(Collection::stream)
                    .map(groupData::getGroupsForEntity)
                    .flatMap(List::stream)
                    .distinct()
                    .collect(Collectors.groupingBy(grouping -> grouping.getDefinition().getType()))
                    .forEach((groupType, groupings) -> {
                        List<RelatedEntity> relatedGroups = groupings.stream()
                                .map(relatedEntityMapper::createRelatedEntity)
                                .collect(Collectors.toList());
                        if (!relatedGroups.isEmpty()) {
                            relatedEntities.put(ExportUtils.getGroupTypeJsonKey(groupType), relatedGroups);
                        }
                    });
        }

        return relatedEntities.isEmpty() ? null : relatedEntities;
    }

}
