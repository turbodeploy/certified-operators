package com.vmturbo.extractor.export;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.extractor.export.schema.RelatedEntity;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Extract related entities (and groups) in the supply chain for an entity.
 */
public class RelatedEntitiesExtractor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Table of info about related groups we want to extract for an entity. It contains the group
     * type, member type in the group, and the json key used in final json object.
     */
    private static final Table<GroupType, Integer, String> GROUP_TYPE_TO_MEMBER_TYPE_AND_JSON_KEY =
            new ImmutableTable.Builder<GroupType, Integer, String>()
                    .put(GroupType.COMPUTE_HOST_CLUSTER, EntityType.PHYSICAL_MACHINE.getNumber(),
                            ExportUtils.getGroupTypeJsonKey(GroupType.COMPUTE_HOST_CLUSTER))
                    .put(GroupType.STORAGE_CLUSTER, EntityType.STORAGE.getNumber(),
                            ExportUtils.getGroupTypeJsonKey(GroupType.STORAGE_CLUSTER))
                    .put(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, EntityType.VIRTUAL_MACHINE.getNumber(),
                            ExportUtils.getGroupTypeJsonKey(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER))
                    .build();

    private final TopologyGraph<SupplyChainEntity> graph;
    private final SupplyChain supplyChain;
    private final GroupData groupData;
    // cache of relatedEntity by id, so it can be reused to reduce memory footprint
    private final Map<Long, RelatedEntity> relatedEntityById;

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
        // synchronized to allow for parallel extraction of related entities
        this.relatedEntityById = Collections.synchronizedMap(new Long2ObjectOpenHashMap<>());
    }

    /**
     * Extract related entities for given entity.
     *
     * @param entityOid oid of the entity to get related entities for
     * @return mapping from related entity type to list of related entities
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
                graph.getEntity(relatedEntityId).ifPresent(entity -> relatedEntityList.add(
                        getOrCreateRelatedEntity(relatedEntityId, entity.getDisplayName())));
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
            GROUP_TYPE_TO_MEMBER_TYPE_AND_JSON_KEY.cellSet().forEach(cell -> {
                Set<Long> relatedEntitiesInGroup = relatedEntitiesByType.get(cell.getColumnKey());
                // also include the entity itself, since the group may contain the entity
                // directly rather than through a related entity
                List<RelatedEntity> relatedGroups = Stream.concat(Stream.of(entityOid),
                        CollectionUtils.emptyIfNull(relatedEntitiesInGroup).stream())
                        .map(groupData::getGroupsForEntity)
                        .flatMap(List::stream)
                        .filter(g -> g.getDefinition().getType() == cell.getRowKey())
                        .distinct()
                        .map(group -> getOrCreateRelatedEntity(group.getId(),
                                group.getDefinition().getDisplayName()))
                        .collect(Collectors.toList());
                if (!relatedGroups.isEmpty()) {
                    relatedEntities.put(cell.getValue(), relatedGroups);
                }
            });
        }

        return relatedEntities.isEmpty() ? null : relatedEntities;
    }

    /**
     * Get or create a new instance of {@link RelatedEntity}.
     *
     * @param id id of the entity
     * @param name name of the entity
     * @return instance of {@link RelatedEntity}
     */
    private RelatedEntity getOrCreateRelatedEntity(long id, String name) {
        return relatedEntityById.computeIfAbsent(id, k -> {
            final RelatedEntity relatedEntity = new RelatedEntity();
            relatedEntity.setOid(id);
            relatedEntity.setName(name);
            return relatedEntity;
        });
    }
}
