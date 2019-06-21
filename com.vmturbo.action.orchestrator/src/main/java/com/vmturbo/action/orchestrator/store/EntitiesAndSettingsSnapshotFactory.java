package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingToPolicyName;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.components.common.setting.SettingDTOUtil;

/**
 * The {@link EntitiesAndSettingsSnapshotFactory} is a way to create a
 * {@link EntitiesAndSettingsSnapshot} that can be used to look up setting and entity-related
 * information during {@link LiveActionStore} population.
 */
@ThreadSafe
public class EntitiesAndSettingsSnapshotFactory {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final RepositoryServiceBlockingStub repositoryService;

    EntitiesAndSettingsSnapshotFactory(@Nonnull final Channel groupChannel, @Nonnull final Channel repoChannel) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repoChannel);
    }

    /**
     * The snapshot of entities and settings required to properly initialize the various
     * fields of an action (most notably the action mode, which factors in settings, flags on
     * an entity, and commodities).
     */
    public static class EntitiesAndSettingsSnapshot {
        private final Map<Long, Map<String, Setting>> settingsByEntityAndSpecName;
        private final Map<Long, ActionPartialEntity> oidToEntityMap;

        public EntitiesAndSettingsSnapshot(@Nonnull final Map<Long, Map<String, Setting>> settings,
                                           @Nonnull final Map<Long, ActionPartialEntity> entityMap) {
            this.settingsByEntityAndSpecName = settings;
            this.oidToEntityMap = entityMap;
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
     *                   cache update.
     * @return A {@link EntitiesAndSettingsSnapshot} containing the new action-related settings an entities.
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot newSnapshot(@Nonnull final Set<Long> entities,
                                                   final long topologyContextId,
                                                   final long topologyId) {
        final Map<Long, Map<String, Setting>> newSettings = retrieveEntityToSettingListMap(entities,
            topologyContextId, topologyId);
        final Map<Long, ActionPartialEntity> entityMap = retrieveOidToEntityMap(entities,
            topologyContextId, topologyId);
        return new EntitiesAndSettingsSnapshot(newSettings, entityMap);
    }

    /**
     * Fetch entities from repository for given entities set.
     * @param entities to fetch from repository.
     * @param topologyContextId of topology.
     * @param topologyId of topology.
     * @return mapping with oid as key and TopologyEntityDTO as value.
     */
    private Map<Long, ActionPartialEntity> retrieveOidToEntityMap(Set<Long> entities,
                    long topologyContextId, long topologyId) {
        try {
            RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(topologyId)
                .setTopologyType(TopologyType.SOURCE)
                .addAllEntityOids(entities)
                .setReturnType(PartialEntity.Type.ACTION)
                .build();
            return RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(getEntitiesrequest))
                .map(PartialEntity::getAction)
                .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
        } catch (StatusRuntimeException ex) {
            logger.error("Failed to fetch entities due to exception : " + ex);
            return Collections.emptyMap();
        }
    }

    @Nonnull
    private Map<Long, Map<String, Setting>> retrieveEntityToSettingListMap(final Set<Long> entities,
                                                                    final long topologyContextId,
                                                                    final long topologyId) {
        try {
            final GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                    .setTopologySelection(TopologySelection.newBuilder()
                            .setTopologyContextId(topologyContextId)
                            .setTopologyId(topologyId))
                    .setSettingFilter(EntitySettingFilter.newBuilder()
                            .addAllEntities(entities))
                    .build();
            return Collections.unmodifiableMap(SettingDTOUtil.flattenEntitySettings(
                settingPolicyService.getEntitySettings(request))
                    .filter(settingsForEntity -> settingsForEntity.getSettingsCount() > 0)
                    .collect(Collectors.toMap(SettingsForEntity::getEntityId,
                        settingsForEntity -> Collections.unmodifiableMap(
                            settingsForEntity.getSettingsList().stream()
                                .map(SettingToPolicyName::getSetting)
                                .collect(Collectors.toMap(
                                    Setting::getSettingSpecName,
                                    Function.identity(),
                                    (v1, v2) -> {
                                        // This shouldn't happen, because conflict resolution
                                        // gets done before entity settings are uploaded and made
                                        // available to clients.
                                        logger.error("Settings service returned two setting values for" +
                                            " entity {}.\nFirst: \n{}\nSecond:\n{}. Choosing first.",
                                            settingsForEntity.getEntityId(), v1, v2);
                                        return v1;
                                    }))
                        ))));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

}
