package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * The {@link EntitiesCache} stores the list of settings for each entity involved
 * in actions in the {@link LiveActionStore}. Every re-population of the store is responsible
 * for updating the cache (via the {@link EntitiesCache#update(Set, long, long)} call)
 * so that the action orchestrator always has the latest settings for the entities it cares about.
 *
 * <p>The reasons we cache the settings instead of getting them on demand are:
 *    1) We just need one call to the setting policy service.
 *    2) Settings are static within one realtime topology broadcast, so we don't need to worry
 *       about cache invalidation.
 *          Note - settings are static because we don't apply changes until the next broadcast
 *                 as an intentional design choice.
 */
@ThreadSafe
public class EntitiesCache {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final RepositoryServiceBlockingStub repositoryService;

    @GuardedBy("cacheLock")
    private final Map<Long, Map<String, Setting>> settingsByEntityAndSpecName = new HashMap<>();

    @GuardedBy("cacheLock")
    private final Map<Long, TopologyEntityDTO> oidToEntityMap = new HashMap<>();

    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    EntitiesCache(@Nonnull final Channel groupChannel, @Nonnull final Channel repoChannel) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repoChannel);
    }

    /**
     * Update the cache to obtain action-related settings for a new set of entities from the
     * setting policy service. There should be exactly one cache update for every new realtime
     * topology broadcast.
     *
     * <p>Once this method returns, all old information will no longer be in the cache.
     *
     * @param entities The new set of entities to get settings for. This set should contain
     *                 the IDs of all entities involved in all actions we expose to the user.
     * @param topologyContextId The topology context of the topology broadcast that
     *                          triggered the cache update.
     * @param topologyId The topology id of the topology, the broadcast of which triggered the
     *                   cache update.
     */
    public void update(@Nonnull final Set<Long> entities,
                       final long topologyContextId,
                       final long topologyId) {
        logger.info("Refreshing entity settings cache...");
        final Map<Long, Map<String, Setting>> newSettings = retrieveEntityToSettingListMap(entities,
                topologyContextId, topologyId);
        final Map<Long, TopologyEntityDTO> entityMap = retrieveOidToEntityMap(entities,
                        topologyContextId, topologyId);
        cacheLock.writeLock().lock();
        try {
            settingsByEntityAndSpecName.clear();
            settingsByEntityAndSpecName.putAll(newSettings);
            oidToEntityMap.clear();
            oidToEntityMap.putAll(entityMap);
        } finally {
            cacheLock.writeLock().unlock();
        }
        logger.info("Refreshed entity settings cache. It now contains settings for {} entities.",
                settingsByEntityAndSpecName.size());
    }

    /**
     * Fetch entities from repository for given entities set.
     * @param entities to fetch from repository.
     * @param topologyContextId of topology.
     * @param topologyId of topology.
     * @return mapping with oid as key and TopologyEntityDTO as value.
     */
    private Map<Long, TopologyEntityDTO> retrieveOidToEntityMap(Set<Long> entities,
                    long topologyContextId, long topologyId) {
        try {
            RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(topologyId)
                .setTopologyType(TopologyType.SOURCE)
                .addAllEntityOids(entities)
                .build();
            RetrieveTopologyEntitiesResponse response = repositoryService.retrieveTopologyEntities(getEntitiesrequest);
            return response.getEntitiesList().stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        } catch (StatusRuntimeException ex) {
            logger.error("Failed to fetch entities due to exception : " + ex);
            return Collections.emptyMap();
        }
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
        cacheLock.readLock().lock();
        try {
            return settingsByEntityAndSpecName.getOrDefault(entityId, Collections.emptyMap());
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Nonnull
    public Optional<TopologyEntityDTO> getEntityFromOid(final long entityOid) {
        cacheLock.readLock().lock();
        try {
            return Optional.ofNullable(oidToEntityMap.get(entityOid));
        } finally {
            cacheLock.readLock().unlock();
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
            final GetEntitySettingsResponse response =
                    settingPolicyService.getEntitySettings(request);
            return Collections.unmodifiableMap(
                response.getSettingsList().stream()
                    .filter(settings -> settings.getSettingsCount() > 0)
                    .collect(Collectors.toMap(SettingsForEntity::getEntityId,
                        settings -> Collections.unmodifiableMap(settings.getSettingsList().stream()
                            .collect(Collectors.toMap(
                                Setting::getSettingSpecName,
                                Function.identity(),
                                (v1, v2) -> {
                                    // This shouldn't happen, because conflict resolution
                                    // gets done before entity settings are uploaded and made
                                    // available to clients.
                                    logger.error("Settings service returned two setting values for" +
                                        " entity {}.\nFirst: \n{}\nSecond:\n{}. Choosing first.",
                                        settings.getEntityId(), v1, v2);
                                    return v1;
                                }))
                        ))));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

}
