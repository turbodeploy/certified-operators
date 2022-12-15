package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Maps;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.store.EntitiesSnapshotFactory.EntitiesSnapshot;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.SettingAndPolicies;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.api.RepositoryListener;
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

    private final GroupServiceBlockingStub groupService;

    private final ScheduleServiceGrpc.ScheduleServiceBlockingStub scheduleService;

    private final AcceptedActionsDAO acceptedActionsStore;

    private final EntitiesSnapshotFactory entitiesSnapshotFactory;

    private final long realtimeTopologyContextId;

    EntitiesAndSettingsSnapshotFactory(@Nonnull final Channel groupChannel,
            final long realtimeTopologyContextId,
            @Nonnull final AcceptedActionsDAO acceptedActionsStore,
            @Nonnull final EntitiesSnapshotFactory entitiesSnapshotFactory) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.groupService = GroupServiceGrpc.newBlockingStub(groupChannel);
        this.scheduleService = ScheduleServiceGrpc.newBlockingStub(groupChannel);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.acceptedActionsStore = Objects.requireNonNull(acceptedActionsStore);
        this.entitiesSnapshotFactory = Objects.requireNonNull(entitiesSnapshotFactory);
    }

    /**
     * The snapshot of entities and settings required to properly initialize the various
     * fields of an action (most notably the action mode, which factors in settings, flags on
     * an entity, and commodities).
     */
    public static class EntitiesAndSettingsSnapshot {
        private final Map<Long, Map<String, SettingAndPolicies>>
                settingAndPoliciesByEntityAndSpecName;
        private final Map<Long, String> policyIdToDisplayName;
        private final Map<Long, ActionPartialEntity> oidToEntityMap;
        private final OwnershipGraph<EntityWithConnections> ownershipGraph;
        private final Map<Long, Long> entityToResourceGroupMap;
        private final Map<Long, Long> entityToNodePoolMap;
        private final Map<Long, ScheduleProto.Schedule> oidToScheduleMap;
        private final Map<Long, String> actionToAcceptorMap;
        private final long topologyContextId;
        private final TopologyType topologyType;
        private final long populationTimestamp;
        @Nullable
        private TopologyInfo topologyInfo;
        private final Map<Long, Long> accountIdToCspId;

        /**
         * Constructor of {@link EntitiesAndSettingsSnapshot}.
         *
         * @param settingsAndPoliciesMap mapping from entity oid to map contains information about
         * settings and associated policies
         * @param policyIdToDisplayName mapping from policy id to policy display name
         * @param entityMap mapping from entity oid to entity info
         * @param ownershipGraph the graph contains connections between entities
         * @param entityToResourceGroupMap mapping from entity oid to related resource group
         * @param entityToNodePoolMap mapping from entity oid to related node pool
         * @param oidToScheduleMap mapping from entity oid to related schedule
         * @param actionToAcceptorMap mapping from recommendation oid to accepting user
         * @param topologyContextId the topology context id
         * @param targetTopologyType the topology type
         * @param populationTimestamp the time when snapshot is created
         */
        public EntitiesAndSettingsSnapshot(
                @Nonnull final Map<Long, Map<String, SettingAndPolicies>> settingsAndPoliciesMap,
                @Nonnull final Map<Long, String> policyIdToDisplayName,
                @Nonnull final Map<Long, ActionPartialEntity> entityMap,
                @Nonnull final OwnershipGraph<EntityWithConnections> ownershipGraph,
                @Nonnull final Map<Long, Long> entityToResourceGroupMap,
                @Nonnull final Map<Long, Long> entityToNodePoolMap,
                @Nonnull final Map<Long, ScheduleProto.Schedule> oidToScheduleMap,
                @Nonnull final Map<Long, String> actionToAcceptorMap, final long topologyContextId,
                @Nonnull final TopologyType targetTopologyType, final long populationTimestamp) {
            this.settingAndPoliciesByEntityAndSpecName = settingsAndPoliciesMap;
            this.policyIdToDisplayName = policyIdToDisplayName;
            this.oidToEntityMap = entityMap;
            this.ownershipGraph = ownershipGraph;
            this.entityToResourceGroupMap = entityToResourceGroupMap;
            this.entityToNodePoolMap = entityToNodePoolMap;
            this.actionToAcceptorMap = actionToAcceptorMap;
            this.topologyContextId = topologyContextId;
            this.topologyType = targetTopologyType;
            this.oidToScheduleMap = oidToScheduleMap;
            this.populationTimestamp = populationTimestamp;
            final Map<Long, Long> accountIdToCspId = new HashMap<>();
            ownershipGraph.getAllOwners().forEach(account -> account.getConnectedEntitiesList().stream()
                .filter(connectedEntity -> connectedEntity.getConnectedEntityType()
                    == CommonDTO.EntityDTO.EntityType.SERVICE_PROVIDER_VALUE)
                .findAny()
                .ifPresent(connectedEntity -> accountIdToCspId.put(account.getOid(),
                    connectedEntity.getConnectedEntityId())));
            this.accountIdToCspId = accountIdToCspId;
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
            return settingAndPoliciesByEntityAndSpecName.getOrDefault(entityId,
                    Collections.emptyMap())
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey, v -> v.getValue().getSetting()));
        }

        /**
         * Get the list of settings policies associated with an entity.
         *
         * @param entityId the id of the entity
         * @return A map of (setting spec name, settings policies ids) related to the entity.
         */
        @Nonnull
        public Map<String, Collection<Long>> getSettingPoliciesForEntity(final long entityId) {
            return settingAndPoliciesByEntityAndSpecName.getOrDefault(entityId,
                    Collections.emptyMap())
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey, v -> v.getValue().getPoliciesIds()));
        }

        /**
         * Get the set of settings defined in default policies and associated with an entity.
         * @param entityId the id of the entity
         * @return set of settings defined in default policies and associated with an entity
         */
        @Nonnull
        public Set<String> getDefaultSettingPoliciesForEntity(final long entityId) {
            return settingAndPoliciesByEntityAndSpecName.getOrDefault(entityId,
                    Collections.emptyMap())
                    .entrySet()
                    .stream()
                    .filter(v -> !CollectionUtils.isEmpty(v.getValue().getDefaultPoliciesIds()))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
        }

        @Nonnull
        public Optional<ActionPartialEntity> getEntityFromOid(final long entityOid) {
            return Optional.ofNullable(oidToEntityMap.get(entityOid));
        }

        @Nonnull
        public  Map<Long, ActionPartialEntity> getEntityMap() {
            return oidToEntityMap;
        }

        @Nonnull
        public Map<Long, ScheduleProto.Schedule> getScheduleMap() {
            return Collections.unmodifiableMap(oidToScheduleMap);
        }

        @Nonnull
        public Optional<Long> getCspIdForAccountId(final long accountId) {
            return Optional.ofNullable(accountIdToCspId.get(accountId));
        }

        /**
         * Returns the user that has accepted the action.
         *
         * @param recommendationId the stable id for the action for which we are querying the
         * accepting user.
         * @return the user name for the user that accepted action or empty otherwise.
         */
        @Nonnull
        public Optional<String> getAcceptingUserForAction(long recommendationId) {
            return  Optional.ofNullable(actionToAcceptorMap.get(recommendationId));
        }

        public long getTopologyContextId() {
            return topologyContextId;
        }

        @Nonnull
        public TopologyType getTopologyType() {
            return topologyType;
        }

        /**
         * Sets optional topology info, for plans.
         *
         * @param topologyInfo Topology info for plans.
         */
        public void setTopologyInfo(@Nullable final TopologyInfo topologyInfo) {
            this.topologyInfo = topologyInfo;
        }

        /**
         * Gets topology info.
         *
         * @return Topology info, can be null.
         */
        @Nullable
        public TopologyInfo getTopologyInfo() {
            return topologyInfo;
        }

        /**
         * Checks if TopologyInfo is null.
         *
         * @return Whether TopologyInfo is set.
         */
        public boolean hasTopologyInfo() {
            return topologyInfo != null;
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
         * Get the node pool for entity.
         *
         * @param entityId entityId which is looking for the node pool
         * @return nodePoolId
         */
        @Nonnull
        public Optional<Long> getNodePoolForEntity(final long entityId) {
            return Optional.ofNullable(entityToNodePoolMap.get(entityId));
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

        /**
         * Returns the timestamp where the snapshot is created.
         * @return the timestamp where the snapshot is created.
         */
        public long getPopulationTimestamp() {
            return populationTimestamp;
        }

        /**
         * Returns the map of policy ids to its associated name
         * @return policyIdToDisplayName
         */
        public Map<Long, String> getPolicyIdToDisplayName() { return policyIdToDisplayName; }
    }

    /**
     * Create a new snapshot containing set of action-related settings and entities.
     * This call involves making remote calls to other components, and can take a while.
     *
     * @param entities The new set of entities to get settings for. This set should contain
     *                 the IDs of all entities involved in all actions we expose to the user.
     * @param topologyContextId The topology context of the topology broadcast that
     *                          triggered the cache update.
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings and entities.
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot newSnapshot(@Nonnull final Set<Long> entities,
                                                   final long topologyContextId) {
        final Map<Long, String> policyIdToDisplayName = new HashMap<>();

        final Map<Long, Map<String, SettingAndPolicies>> settingAndPoliciesMapByEntityAndSpecName =
                populatePolicyMaps(entities, topologyContextId, policyIdToDisplayName);

        final Map<Long, Long> entityToResourceGroupMap =
                retrieveResourceGroupsForEntities(entities);

        final Map<Long, Long> entityToNodePoolMap =
                retrieveNodePoolForEntities(entities);

        final EntitiesSnapshot entitiesSnapshot = entitiesSnapshotFactory.getEntitiesSnapshot(
                entities, topologyContextId);


        final Map<Long, ScheduleProto.Schedule> oidToScheduleMap = new HashMap<>();
        scheduleService.getSchedules(
                ScheduleProto.GetSchedulesRequest.newBuilder().build()).forEachRemaining(
                schedule -> oidToScheduleMap.put(schedule.getId(), schedule));

        // RecommendationId -> AcceptedBy
        final Map<Long, String> actionToAcceptorMap =
                acceptedActionsStore.getAcceptorsForAllActions();

        return new EntitiesAndSettingsSnapshot(settingAndPoliciesMapByEntityAndSpecName,
                policyIdToDisplayName, entitiesSnapshot.getEntityMap(), entitiesSnapshot.getOwnershipGraph(),
                entityToResourceGroupMap, entityToNodePoolMap, oidToScheduleMap, actionToAcceptorMap,
                topologyContextId, entitiesSnapshot.getTopologyType(), System.currentTimeMillis());
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
    private Map<Long, Long> retrieveNodePoolForEntities(@Nonnull Set<Long> entities) {
        final GetGroupsForEntitiesResponse response = groupService.getGroupsForEntities(
                GetGroupsForEntitiesRequest.newBuilder()
                        .addAllEntityId(entities)
                        .addGroupType(GroupType.NODE_POOL)
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


    /**
     * Creates an empty snapshot. It only has a topology context id.
     *
     * @return An empty {@link EntitiesAndSettingsSnapshot}
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot emptySnapshot() {
        return new EntitiesAndSettingsSnapshot(Collections.emptyMap(), Maps.newHashMap(), Maps.newHashMap(),
                OwnershipGraph.empty(), Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap(),
                realtimeTopologyContextId, TopologyType.SOURCE, System.currentTimeMillis());
    }

    /**
     * @param entities The new set of entities to get settings for. This set should contain
     *                 the IDs of all entities involved in all actions we expose to the user.
     * @param topologyContextId The topology context of the topology broadcast that
     *                          triggered the cache update.
     * @param policyIdToDisplayName empty map to be populated with the names of policies from id.
     * @return map of settingAndPoliciesMapByEntityAndSpecName.
     */
    @Nonnull
    private Map<Long, Map<String, SettingAndPolicies>> populatePolicyMaps(
            final Set<Long> entities, final long topologyContextId,
            final Map<Long, String> policyIdToDisplayName) {
        // We don't currently upload action-relevant settings in plans,
        // so no point trying to get them.
        if (topologyContextId != realtimeTopologyContextId) {
            return Collections.emptyMap();
        }

        try {
            final Builder builder = TopologySelection.newBuilder()
                .setTopologyContextId(topologyContextId);
            final GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                    .setTopologySelection(builder)
                    .setSettingFilter(EntitySettingFilter.newBuilder()
                            .addAllEntities(entities))
                    .setIncludeSettingPolicies(true)
                    .build();
            return Collections.unmodifiableMap(SettingDTOUtil.indexSettingsByEntity(
                SettingDTOUtil.flattenEntitySettings(
                    settingPolicyService.getEntitySettings(request)), policyIdToDisplayName));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * Get all entity settings.
     *
     * @param topologyContextId The topology context of the topology broadcast that
     *         triggered the cache update.
     * @return Iterator of all {@link GetEntitySettingsResponse}.
     */
    @Nonnull
    public Iterator<GetEntitySettingsResponse> getEntitySettings(final long topologyContextId) {
        // We don't currently upload action-relevant settings in plans,
        // so no point trying to get them.
        if (topologyContextId != realtimeTopologyContextId) {
            return Collections.emptyIterator();
        }

        try {
            final Builder builder = TopologySelection.newBuilder()
                .setTopologyContextId(topologyContextId);
            final GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                    .setTopologySelection(builder)
                    // .setSettingFilter(EntitySettingFilter.newBuilder())
                    .setIncludeSettingPolicies(true)
                    .build();
            return settingPolicyService.getEntitySettings(request);
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyIterator();
        }
    }

}
