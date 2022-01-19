package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor;

/**
 * Topology edits on the scoped topology are done here.
 * Here are the edits it does:
 * Phase 1. It connects newly added hosts to ALL non-local storages in the scoped topology (that are not removed/replaced)
 * Phase 2. It connects newly added storages to the existing hosts in the scoped topology (that are not removed/replaced)
 * Example:
 * <pre>
 * Existing:
 * Host 1
 * Host 2       Storage 7
 * Host 3       Storage 8
 *
 * Newly Added:
 * Host 4       Storage 9
 * Host 5
 * </pre>
 * Consider this topology above.
 * Phase 1: Connect the newly added hosts - 4 and 5 to storages 7, 8 and 9.
 * Phase 2: Connect the newly added storage - 9 to hosts 1, 2 and 3. (Storage 9 was connected hosts 4 and 5 in previous step)
 *
 */
public class PostScopingTopologyEditor {

    private final Predicate<TopologyEntity> isEntityAddedInPlan = TopologyEntity::hasPlanOrigin;
    private final Predicate<TopologyEntity> isNonLocalStorage = t -> t.getTypeSpecificInfo().hasStorage()
            && t.getTypeSpecificInfo().getStorage().hasIsLocal()
            && !t.getTypeSpecificInfo().getStorage().getIsLocal();
    private final Predicate<TopologyEntity> isRemovedOrRepaced = t -> t.getTopologyEntityDtoBuilder().hasEdit()
            && (t.getTopologyEntityDtoBuilder().getEdit().hasRemoved() || t.getTopologyEntityDtoBuilder().getEdit().hasReplaced());
    private final List<PostScopingTopologyEdit> topologyEdits;

    /**
     * Constructor. Here, we initialize the edits that need to be performed.
     */
    public PostScopingTopologyEditor() {
        // First, connect the added hosts to ALL the non-local storages which have not been removed or replaced.
        PostScopingTopologyEdit addedHostEdit = new PostScopingTopologyEdit(EntityType.PHYSICAL_MACHINE_VALUE, CommodityType.DATASTORE,
                EntityType.STORAGE_VALUE, CommodityType.DSPM_ACCESS, isEntityAddedInPlan, isEntityAddedInPlan.or(isNonLocalStorage.and(isRemovedOrRepaced.negate())));
        // Second, connect the added storages to all the existing hosts which have not been removed or replaced (added storages should already be
        // connected to new hosts in the previous step)
        PostScopingTopologyEdit addedStorageEdit = new PostScopingTopologyEdit(EntityType.STORAGE_VALUE, CommodityType.DSPM_ACCESS,
                EntityType.PHYSICAL_MACHINE_VALUE, CommodityType.DATASTORE, isEntityAddedInPlan, isEntityAddedInPlan.negate().and(isRemovedOrRepaced.negate()));
        topologyEdits = ImmutableList.of(addedHostEdit, addedStorageEdit);
    }

    /**
     * Edit the scoped topology.
     * Phase 1. It connects newly added hosts to all non-local storages in the scoped topology (that are not removed/replaced)
     * Phase 2. It connects newly added storages to all the hosts in the scoped topology (that are not removed/replaced)
     * @param topology the scoped topology
     * @param scenarioChanges the scenario changes can be checked to see if any topology additions were made
     * @return the results of the editing done here
     */
    public List<PostScopingTopologyEditResult> editTopology(@Nonnull final TopologyGraph<TopologyEntity> topology,
                                                            @Nonnull final List<ScenarioChange> scenarioChanges) {
        List<PostScopingTopologyEditResult> results = new ArrayList<>();
        for (PostScopingTopologyEdit edit : topologyEdits) {
            boolean hasRelevantTopologyAddition = scenarioChanges.stream().anyMatch(sc -> sc.hasTopologyAddition()
                    && edit.addedEntityType == sc.getTopologyAddition().getTargetEntityType());
            if (!hasRelevantTopologyAddition) {
                continue;
            }
            List<TopologyEntity> addedEntities = new ArrayList<>();
            List<TopologyEntity> connectedEntities = new ArrayList<>();
            topology.entities().forEach(entity -> {
                if (entity.getEntityType() == edit.addedEntityType && edit.addedEntityFilter.test(entity)) {
                    addedEntities.add(entity);
                } else if (entity.getEntityType() == edit.connectedEntityType && edit.connectedEntityTypeFilter.test(entity)) {
                    connectedEntities.add(entity);
                }
            });
            int numCommoditiesCreated = 0;
            for (TopologyEntity addedEntity : addedEntities) {
                for (TopologyEntity connectedEntity : connectedEntities) {
                    // 1. Check if addedEntity has access commodity with accesses pointing to connectedEntity.
                    // If not, create it.
                    if (!areEntitiesConnected(addedEntity, connectedEntity)) {
                        String key = findConnectingCommodityKey(connectedEntity, topology).orElse(null);
                        CommoditySoldDTO.Builder commSoldForAddedEntity =
                                TopologyEntityConstructor.createAccessCommodity(edit.commTypeForAddedEntityType,
                                        connectedEntity.getOid(), key);
                        addedEntity.getTopologyEntityDtoBuilder().addCommoditySoldList(commSoldForAddedEntity);
                        numCommoditiesCreated++;
                    }
                    // 2. Check if connectedEntity has access commodity with accesses pointing to addedEntity.
                    // If not, create it.
                    if (!areEntitiesConnected(connectedEntity, addedEntity)) {
                        // Its fine to pass in key as null because there will be no existing connections to a newly
                        // added entity
                        CommoditySoldDTO.Builder commSoldForConnectedEntity =
                                TopologyEntityConstructor.createAccessCommodity(edit.commTypeForConnectedEntityType,
                                        addedEntity.getOid(), null);
                        connectedEntity.getTopologyEntityDtoBuilder().addCommoditySoldList(commSoldForConnectedEntity);
                        numCommoditiesCreated++;
                    }
                }
            }
            results.add(new PostScopingTopologyEditResult(edit, numCommoditiesCreated, addedEntities.size(),
                    connectedEntities.size()));
        }
        return results;
    }

    /**
     * Checks if there is a commodity sold by entity1 which accesses entity2.
     * @param entity1 entity to check
     * @param entity2 connecting entity
     * @return true if there is a commodity sold by entity1 which accesses entity2. False otherwise.
     */
    private boolean areEntitiesConnected(TopologyEntity entity1, TopologyEntity entity2) {
        return entity1.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream().anyMatch(
                cs -> cs.hasAccesses() && entity2.getOid() == cs.getAccesses());
    }

    /**
     * Finds the key for the commodity which accesses this entity.
     * Ex: Let's say the entity passed in here is a Storage called A. We go from the Storage A to all the connected Hosts
     * using the accesses relation. From one of these connected hosts, we find the key of the commodity sold
     * which accesses the storage A.
     * If we cannot find such a commodity that accesses storage A, then we return Optional.empty().
     * @param entity the entity which is to be accessed using access commodity
     * @param topology the topology graph
     * @return key of the commodity which accesses this entity. If it cannot be found, then Optional.empty is returned.
     */
    private Optional<String> findConnectingCommodityKey(TopologyEntity entity,
                                                        TopologyGraph<TopologyEntity> topology) {
        Set<Long> connectingEntities = entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                .filter(CommoditySoldDTO::hasAccesses).map(CommoditySoldDTO::getAccesses).collect(Collectors.toSet());
        for (Long connectingEntity : connectingEntities) {
            Optional<TopologyEntity> entityOpt = topology.getEntity(connectingEntity);
            if (entityOpt.isPresent()) {
                Optional<String> key = entityOpt.get().getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                        .filter(c -> c.getAccesses() == entity.getOid())
                        .map(CommoditySoldDTO::getCommodityType)
                        .map(TopologyDTO.CommodityType::getKey)
                        .findFirst();
                if (key.isPresent()) {
                    return key;
                }
            }
        }
        return Optional.empty();
    }

    /**
     * This class defines the edit which needs to be performed for the added entities.
     */
    private static class PostScopingTopologyEdit {
        private int addedEntityType;
        private CommodityType commTypeForAddedEntityType;
        private int connectedEntityType;
        private CommodityType commTypeForConnectedEntityType;
        private Predicate<TopologyEntity> addedEntityFilter;
        private Predicate<TopologyEntity> connectedEntityTypeFilter;

        /**
         * Constructor.
         * @param addedEntityType the added entity type (ex., type for host / storage)
         * @param commTypeForAddedEntityType the comm type to be sold by the added entity type (ex., DataStore is sold
         *                                   by Hosts and DSPM comm is sold by storage)
         * @param connectedEntityType the entity type to connect the added entity to
         * @param commTypeForConnectedEntityType the comm type to be sold by the connected entity type (ex., DataStore
         *                                       is sold by Hosts and DSPM comm is sold by storage)
         * @param addedEntityFilter filter for the added entity type
         * @param connectedEntityTypeFilter filter for the connected entity type
         */
        private PostScopingTopologyEdit(int addedEntityType,
                                        CommodityType commTypeForAddedEntityType,
                                        int connectedEntityType,
                                        CommodityType commTypeForConnectedEntityType,
                                        Predicate<TopologyEntity> addedEntityFilter,
                                        Predicate<TopologyEntity> connectedEntityTypeFilter) {
            this.addedEntityType = addedEntityType;
            this.commTypeForAddedEntityType = commTypeForAddedEntityType;
            this.connectedEntityType = connectedEntityType;
            this.commTypeForConnectedEntityType = commTypeForConnectedEntityType;
            this.addedEntityFilter = addedEntityFilter;
            this.connectedEntityTypeFilter = connectedEntityTypeFilter;
        }
    }

    /**
     * The result of the post scoping topology edits.
     */
    public static class PostScopingTopologyEditResult {
        private PostScopingTopologyEdit edit;
        private int numCommoditiesCreated;
        private int addedEntityCount;
        private int connectedEntityCount;

        private PostScopingTopologyEditResult(PostScopingTopologyEdit edit, int numCommoditiesCreated,
                                              int addedEntityCount, int connectedEntityCount) {
            this.edit = edit;
            this.numCommoditiesCreated = numCommoditiesCreated;
            this.addedEntityCount = addedEntityCount;
            this.connectedEntityCount = connectedEntityCount;
        }

        public int getNumCommoditiesCreated() {
            return numCommoditiesCreated;
        }

        public int getAddedEntityCount() {
            return addedEntityCount;
        }

        public int getConnectedEntityCount() {
            return connectedEntityCount;
        }

        public int getAddedEntityType() {
            return edit.addedEntityType;
        }

        @Override
        public String toString() {
            return new StringBuilder("Connected ").append(addedEntityCount).append(" added ")
                    .append(EntityType.forNumber(edit.addedEntityType)).append("s").append(" to ")
                    .append(connectedEntityCount).append(" ").append(EntityType.forNumber(edit.connectedEntityType))
                    .append("s").append(" using ").append(numCommoditiesCreated).append(" commodities").toString();
        }
    }
}