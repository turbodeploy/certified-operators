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

import com.google.common.collect.Maps;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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

    // TODO this is a temporary implementation.  Roman will have the Business Account in the Snapshot
    //      so that no explicitly call need to be made.
    private final SearchServiceBlockingStub searchService;

    private final int entityRetrievalRetryIntervalMillis;

    private final int entityRetrievalMaxRetries;

    private final long realtimeTopologyContextId;

    EntitiesAndSettingsSnapshotFactory(@Nonnull final Channel groupChannel,
                                       @Nonnull final Channel repoChannel,
                                       @Nonnull final int entityRetrievalRetryIntervalMillis,
                                       @Nonnull final int entityRetrievalMaxRetries,
                                       @Nonnull final long realtimeTopologyContextId) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repoChannel);
        this.searchService = SearchServiceGrpc.newBlockingStub(repoChannel);
        this.entityRetrievalRetryIntervalMillis = entityRetrievalRetryIntervalMillis;
        this.entityRetrievalMaxRetries = entityRetrievalMaxRetries;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * The snapshot of entities and settings required to properly initialize the various
     * fields of an action (most notably the action mode, which factors in settings, flags on
     * an entity, and commodities).
     */
    public static class EntitiesAndSettingsSnapshot {
        private final Map<Long, Map<String, Setting>> settingsByEntityAndSpecName;
        private final Map<Long, ActionPartialEntity> oidToEntityMap;
        private final long topologyContextId;

        // TODO this is a temporary implementation.  Roman will have the Business Account in the Snapshot
        //      so that no explicitly call need to be made.
        private final SearchServiceBlockingStub searchService;

        public EntitiesAndSettingsSnapshot(@Nonnull final Map<Long, Map<String, Setting>> settings,
                                           @Nonnull final Map<Long, ActionPartialEntity> entityMap,
                                           @Nonnull final long topologyContextId,
                                           @Nonnull final SearchServiceBlockingStub searchService) {
            this.settingsByEntityAndSpecName = settings;
            this.oidToEntityMap = entityMap;
            this.topologyContextId = topologyContextId;
            this.searchService = searchService;
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
         * TODO this method will be updated such that we don't need
         *      to make explicitly call to SearchService.  This is only a temporary implementation
         *
         * @param entityId the entity which is looking for the owner
         * @return Business Account
         */
        @Nonnull
        public Optional<TopologyEntityDTO> getOwnerAccountOfEntity(final long entityId) {

            SearchParameters params = SearchProtoUtil.neighborsOfType(entityId,
                TraversalDirection.CONNECTED_FROM,
                UIEntityType.BUSINESS_ACCOUNT);

            SearchEntitiesRequest request = SearchEntitiesRequest.newBuilder().addSearchParameters(params).build();

            return RepositoryDTOUtil.topologyEntityStream(searchService.searchEntitiesStream(request))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList()).stream().findFirst();
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
        return new EntitiesAndSettingsSnapshot(newSettings, entityMap, topologyContextId, searchService);
    }

    /**
     * Creates an empty snapshot. It only has a topology context id.
     *
     * @param topologyContextId The topology context id
     * @return An empty {@link EntitiesAndSettingsSnapshot}
     */
    @Nonnull
    public EntitiesAndSettingsSnapshot emptySnapshot(final long topologyContextId) {
        return new EntitiesAndSettingsSnapshot(Collections.emptyMap(), Maps.newHashMap(), topologyContextId, searchService);
    }

    /**
     * Fetch entities from repository for given entities set.
     *
     * @param entities to fetch from repository.
     * @param topologyContextId of topology.
     * @param topologyId of topology.
     * @return mapping with oid as key and {@link ActionPartialEntity} as value.
     */
    private Map<Long, ActionPartialEntity> retrieveOidToEntityMap(Set<Long> entities,
                    long topologyContextId, long topologyId) {
        try {
            RetrieveTopologyEntitiesRequest.Builder getEntitiesRequestBuilder = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(topologyId)
                .addAllEntityOids(entities)
                .setReturnType(PartialEntity.Type.ACTION);
            if (topologyContextId == realtimeTopologyContextId) {
                getEntitiesRequestBuilder.setTopologyType(TopologyType.SOURCE);
            } else {
                // This is a plan so we need the TopologyType to be PROJECTED
                getEntitiesRequestBuilder.setTopologyType(TopologyType.PROJECTED);
            }

            Map<Long, ActionPartialEntity> entitiesMap = waitUntilRepositoryReturnsData(getEntitiesRequestBuilder.build());

            if (entitiesMap.size() == 0) {
                logger.error("After retrying for 30m, still the requested topology "+topologyContextId+" doesn't exist in the repository");
            }
            return entitiesMap;
        } catch (StatusRuntimeException ex) {
            logger.error("Failed to fetch entities due to exception : " + ex);
            return Collections.emptyMap();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Set the interrupt status on the thread.
            logger.error("Failed to wait for repository to return data due to exception : " + e);
            return Collections.emptyMap();
        }
    }

    /**
     * Waits for the repository to return entities data for the given request. The method will wait
     * until the defined number of retries in MAXIMUM_RETRIES. If the repository still doesn't have
     * the requested data, an empty map will be returned
     *
     * @param request An object of class {@link RetrieveTopologyEntitiesRequest}
     *                                  that holds the repository request
     * @return mapping with oid as key and {@link ActionPartialEntity} as value.
     * @throws InterruptedException
     */
    private Map<Long, ActionPartialEntity> waitUntilRepositoryReturnsData(@Nonnull final RetrieveTopologyEntitiesRequest request)
        throws InterruptedException {
        Map<Long, ActionPartialEntity> entitiesMap = Collections.emptyMap();
        for (int currentTry = 0; currentTry < entityRetrievalMaxRetries; ++currentTry) {
            entitiesMap = RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(request))
                .map(PartialEntity::getAction)
                .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
            if (entitiesMap.isEmpty()) {
                Thread.sleep(entityRetrievalRetryIntervalMillis);
            } else {
                break;
            }
        }
        return entitiesMap;
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
            return Collections.unmodifiableMap(SettingDTOUtil.indexSettingsByEntity(
                SettingDTOUtil.flattenEntitySettings(
                    settingPolicyService.getEntitySettings(request))));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

}
