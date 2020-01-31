package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Maps;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.repository.api.TopologyAvailabilityTracker;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.TopologyUnavailableException;
import com.vmturbo.topology.graph.OwnershipGraph;

/**
 * The {@link EntitiesAndSettingsSnapshotFactory} is a way to create a
 * {@link EntitiesAndSettingsSnapshot} that can be used to look up setting and entity-related
 * information during {@link LiveActionStore} population.
 */
@ThreadSafe
public class EntitiesAndSettingsSnapshotFactory implements RepositoryListener {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final RepositoryServiceBlockingStub repositoryService;

    private final GroupServiceBlockingStub groupService;

    // TODO this is a temporary implementation.  Roman will have the Business Account in the Snapshot
    //      so that no explicitly call need to be made.
    private final SearchServiceBlockingStub searchService;

    private final long timeToWaitForTopology;

    private final TimeUnit timeToWaitUnit;

    private final TopologyAvailabilityTracker topologyAvailabilityTracker;

    private final long realtimeTopologyContextId;

    EntitiesAndSettingsSnapshotFactory(@Nonnull final Channel groupChannel,
                                       @Nonnull final Channel repoChannel,
                                       @Nonnull final long realtimeTopologyContextId,
                                       @Nonnull final TopologyAvailabilityTracker topologyAvailabilityTracker,
                                       @Nonnull final long timeToWaitForTopology,
                                       @Nonnull final TimeUnit timeToWaitUnit) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repoChannel);
        this.searchService = SearchServiceGrpc.newBlockingStub(repoChannel);
        this.groupService = GroupServiceGrpc.newBlockingStub(groupChannel);
        this.timeToWaitForTopology = timeToWaitForTopology;
        this.timeToWaitUnit = timeToWaitUnit;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.topologyAvailabilityTracker = topologyAvailabilityTracker;
    }

    /**
     * The snapshot of entities and settings required to properly initialize the various
     * fields of an action (most notably the action mode, which factors in settings, flags on
     * an entity, and commodities).
     */
    public static class EntitiesAndSettingsSnapshot {
        private final Map<Long, Map<String, Setting>> settingsByEntityAndSpecName;
        private final Map<Long, ActionPartialEntity> oidToEntityMap;
        private final OwnershipGraph<EntityWithConnections> ownershipGraph;
        private final Map<Long, Long> entityToResourceGroupMap;
        private final long topologyContextId;

        public EntitiesAndSettingsSnapshot(@Nonnull final Map<Long, Map<String, Setting>> settings,
                @Nonnull final Map<Long, ActionPartialEntity> entityMap,
                @Nonnull final OwnershipGraph<EntityWithConnections> ownershipGraph,
                @Nonnull final Map<Long, Long> entityToResourceGroupMap,
                final long topologyContextId) {
            this.settingsByEntityAndSpecName = settings;
            this.oidToEntityMap = entityMap;
            this.ownershipGraph = ownershipGraph;
            this.entityToResourceGroupMap = entityToResourceGroupMap;
            this.topologyContextId = topologyContextId;
        }

        /**
         * Get the list of action-orchestrator related settings associated with an entity.
         *
         * @param entityId The ID of the entity.
         * @return A map of (setting spec name, setting) for the settings associated with the entity.
         *         This may be empty, but will not be null.
         */
        @Nonnull
        public Map<String, Setting> getSettingsForEntity(final long entityId) {
            return settingsByEntityAndSpecName.getOrDefault(entityId, Collections.emptyMap());
        }

        @Nonnull
        public Optional<ActionPartialEntity> getEntityFromOid(final long entityOid) {
            return Optional.ofNullable(oidToEntityMap.get(entityOid));
        }

        @Nonnull
        public  Map<Long, ActionPartialEntity> getEntityMap() {
            return oidToEntityMap;
        }

        public long getToologyContextId() {
            return topologyContextId;
        }

        /**
         * Get the owner business account of an entity.
         *
         * @param entityId the entity which is looking for the owner
         * @return A {@link EntityWithConnections} describing the account.
         */
        @Nonnull
        public Optional<EntityWithConnections> getOwnerAccountOfEntity(final long entityId) {
            // The first owner is the immediate owner.
            return ownershipGraph.getOwners(entityId).stream().findFirst();
        }

        /**
         * Get the resource group for entity.
         *
         * @param entityId entityId which is looking for the resource group
         * @return resourceGroupId
         */
        @Nonnull
        public Optional<Long> getResourceGroupForEntity(final long entityId) {
            return Optional.ofNullable(entityToResourceGroupMap.get(entityId));
        }

    }

    /**
     * Create a new snapshot containing set of action-related settings and entities.
     * This call involves making remote calls to other components, and can take a while.
     *
     * @param entities The new set of entities to get settings for. This set should contain
     *                 the IDs of all entities involved in all actions we expose to the user.
     * @param topologyContextId The topology context of the topology broadcast that
     *                          triggered the cache update.
     * @param topologyId The topology id of the topology, the broadcast of which triggered the
     *                   cache update. The topology id can be null as well in case we are trying to
     *                   get a new snapshot for RI Buy Actions. Because RI Buy Algorithm is not
     *                   triggered on topology broadcast.
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings and entities.
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot newSnapshot(@Nonnull final Set<Long> entities,
                                                   final long topologyContextId,
                                                   final long topologyId) {
        return internalNewSnapshot(entities, topologyContextId, topologyId);
    }

    /**
     * A version of {@link EntitiesAndSettingsSnapshotFactory#newSnapshot(Set, long, long)}
     * that waits for the latest topology in a particular context.
     *
     * @param entities See {@link EntitiesAndSettingsSnapshot#newSnapshot(Set, long, long)} .
     * @param topologyContextId See {@link EntitiesAndSettingsSnapshot#newSnapshot(Set, long, long)}.
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings and entities.
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot newSnapshot(@Nonnull final Set<Long> entities,
                                                   final long topologyContextId) {
        return internalNewSnapshot(entities, topologyContextId, null);
    }

    @Nonnull
    private EntitiesAndSettingsSnapshot internalNewSnapshot(@Nonnull final Set<Long> entities,
                                                            final long topologyContextId,
                                                            @Nullable final Long topologyId) {
        final Map<Long, Map<String, Setting>> newSettings = retrieveEntityToSettingListMap(entities,
            topologyContextId, topologyId);
        final Map<Long, ActionPartialEntity> entityMap = retrieveOidToEntityMap(entities,
            topologyContextId, topologyId);
        final OwnershipGraph<EntityWithConnections> ownershipGraph =
            retrieveOwnershipGraph(entities, topologyContextId, topologyId);
        final Map<Long, Long> entityToResourceGroupMap =
                retrieveResourceGroupsForEntities(entities);
        return new EntitiesAndSettingsSnapshot(newSettings, entityMap, ownershipGraph,
                entityToResourceGroupMap, topologyContextId);
    }

    @Nonnull
    private Map<Long, Long> retrieveResourceGroupsForEntities(@Nonnull Set<Long> entities) {
        final GetGroupsForEntitiesResponse response = groupService.getGroupsForEntities(
                GetGroupsForEntitiesRequest.newBuilder()
                        .addAllEntityId(entities)
                        .addGroupType(GroupType.RESOURCE)
                        .build());
        final Map<Long, Long> resultMap = new HashMap<>();
        for (Entry<Long, Groupings> groupingsEntry : response.getEntityGroupMap().entrySet()) {
            final long entityId = groupingsEntry.getKey();
            for (Long groupId : groupingsEntry.getValue().getGroupIdList()) {
                final Long oldGroupId = resultMap.put(entityId, groupId);
                if (oldGroupId != null) {
                    logger.warn("Found multiple resource groups for entity {}: {} and {}", entityId,
                            oldGroupId, groupId);
                }
            }
        }
        return Collections.unmodifiableMap(resultMap);
    }

    @Nonnull
    private OwnershipGraph<EntityWithConnections> retrieveOwnershipGraph(@Nonnull final Set<Long> entities,
                                                                         final long topologyContextId,
                                                                         @Nullable final Long topologyId) {
        final OwnershipGraph.Builder<EntityWithConnections> graphBuilder =
            OwnershipGraph.newBuilder(EntityWithConnections::getOid);

        final RetrieveTopologyEntitiesRequest.Builder entitiesReqBldr = RetrieveTopologyEntitiesRequest.newBuilder()
            .setReturnType(Type.WITH_CONNECTIONS)
            .setTopologyContextId(topologyContextId)
            .setTopologyType(TopologyType.SOURCE)
            .addEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber());
        // Set the topologyId if its non null. Else it defaults to real time.
        if (topologyId != null) {
            entitiesReqBldr.setTopologyId(topologyId);
        }


        // Get all the business accounts and add them to the ownership graph.
        RepositoryDTOUtil.topologyEntityStream(
            repositoryService.retrieveTopologyEntities(entitiesReqBldr.build()))
            .map(PartialEntity::getWithConnections)
            .forEach(ba -> ba.getConnectedEntitiesList().stream()
                .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.OWNS_CONNECTION)
                .filter(connectedEntity -> entities.contains(connectedEntity.getConnectedEntityId()))
                .forEach(relevantEntity -> graphBuilder.addOwner(ba, relevantEntity.getConnectedEntityId())));
        return graphBuilder.build();
    }

    /**
     * Creates an empty snapshot. It only has a topology context id.
     *
     * @return An empty {@link EntitiesAndSettingsSnapshot}
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot emptySnapshot() {
        return new EntitiesAndSettingsSnapshot(Collections.emptyMap(), Maps.newHashMap(),
            OwnershipGraph.empty(), Maps.newHashMap(), realtimeTopologyContextId);
    }

    /**
     * Fetch entities from repository for given entities set.
     *
     * @param entities to fetch from repository.
     * @param topologyContextId of topology.
     * @param topologyId of topology, if we are looking for a particular topology. Null if we just
     *                   want whatever is the current latest topology (e.g. for RI Buy Actions).
     * @return mapping with oid as key and {@link ActionPartialEntity} as value.
     */
    private Map<Long, ActionPartialEntity> retrieveOidToEntityMap(Set<Long> entities,
                    long topologyContextId, @Nullable final Long topologyId) {
        if (entities.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            // For plans we want to look in the projected topology, because in plans we will be
            // getting actions involving provisioned entities. In realtime we get provision actions,
            // but no actions on top of the provisioned entities, so looking in the source topology
            // is safe (and more efficient).
            final TopologyType targetTopologyType = topologyContextId == realtimeTopologyContextId ?
                TopologyType.SOURCE : TopologyType.PROJECTED;

            if (topologyId != null) {
                // If we want a specific topology, wait for that topology to become available.
                topologyAvailabilityTracker.queueTopologyRequest(topologyContextId, topologyId)
                    .waitForTopology(timeToWaitForTopology, timeToWaitUnit);
            } else {
                // If not, wait for SOME topology of the target type to be available in the context.
                topologyAvailabilityTracker.queueAnyTopologyRequest(topologyContextId, targetTopologyType)
                    .waitForTopology(timeToWaitForTopology, timeToWaitUnit);
            }

            final RetrieveTopologyEntitiesRequest getEntitiesRequestBuilder =
                RetrieveTopologyEntitiesRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .addAllEntityOids(entities)
                    .setReturnType(PartialEntity.Type.ACTION)
                    .setTopologyType(targetTopologyType)
                    .build();

            final Map<Long, ActionPartialEntity> entitiesMap = RepositoryDTOUtil.topologyEntityStream(
                repositoryService.retrieveTopologyEntities(getEntitiesRequestBuilder))
                .map(PartialEntity::getAction)
                .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
            return entitiesMap;
        } catch (TopologyUnavailableException e) {
            logger.error("Topology not available. Entity snapshot won't have entity information." +
                " Error: {}", e.getMessage());
            return Collections.emptyMap();
        } catch (StatusRuntimeException ex) {
            logger.error("Failed to fetch entities due to exception : " + ex);
            return Collections.emptyMap();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
            logger.error("Failed to wait for repository to return data due to exception : " + e);
            return Collections.emptyMap();
        }
    }

    @Nonnull
    private Map<Long, Map<String, Setting>> retrieveEntityToSettingListMap(final Set<Long> entities,
                                                                    @Nonnull final Long topologyContextId,
                                                                    @Nullable final Long topologyId) {
        try {
            final Builder builder = TopologySelection.newBuilder()
                    .setTopologyContextId(topologyContextId);
            if (topologyId != null) {
                builder.setTopologyId(topologyId);
            }
            final GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                    .setTopologySelection(builder)
                    .setSettingFilter(EntitySettingFilter.newBuilder()
                            .addAllEntities(entities))
                    .build();
            return Collections.unmodifiableMap(SettingDTOUtil.indexSettingsByEntity(
                SettingDTOUtil.flattenEntitySettings(
                    settingPolicyService.getEntitySettings(request))));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

}
