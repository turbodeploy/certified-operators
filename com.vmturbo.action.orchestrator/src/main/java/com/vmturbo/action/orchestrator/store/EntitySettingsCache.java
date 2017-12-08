package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The {@link EntitySettingsCache} stores the list of settings for each entity involved
 * in actions in the {@link LiveActionStore}. Every re-population of the store is responsible
 * for updating the cache (via the {@link EntitySettingsCache#update(Set, long, long)} call)
 * so that the action orchestrator always has the latest settings for the entities it cares about.
 * <p>
 * The reasons we cache the settings instead of getting them on demand are:
 *    1) We just need one call to the setting policy service.
 *    2) Settings are static within one realtime topology broadcast, so we don't need to worry
 *       about cache invalidation.
 *          Note - settings are static because we don't apply changes until the next broadcast
 *                 as an intentional design choice.
 */
@ThreadSafe
public class EntitySettingsCache {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final RestTemplate restTemplate;

    private final ExecutorService executorService;

    private final String repositoryHost;

    private final int repositoryPort;

    private final long entityTypeRetryIntervalMillis;

    private final long entityTypeMaxRetries;

    @GuardedBy("cacheLock")
    private final Map<Long, List<Setting>> settingsByEntity = new HashMap<>();

    @GuardedBy("cacheLock")
    private Future<Map<Long, EntityType>> entityTypeMap = CompletableFuture.completedFuture(new HashMap<>());

    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    EntitySettingsCache(@Nonnull final Channel groupChannel,
                        @Nonnull final RestTemplate restTemplate,
                        @Nonnull final String repositoryHost,
                        final int repositoryPort,
                        @Nonnull final ExecutorService executorService,
                        final long maxRetries,
                        final long retryInterval) {
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.restTemplate = Objects.requireNonNull(restTemplate);
        this.repositoryHost = repositoryHost;
        this.repositoryPort = repositoryPort;
        this.executorService = executorService;
        this.entityTypeMaxRetries = maxRetries;
        this.entityTypeRetryIntervalMillis = retryInterval;
    }

    /**
     * Update the cache to obtain action-related settings for a new set of entities from the
     * setting policy service. There should be exactly one cache update for every new realtime
     * topology broadcast.
     * <p>
     * Once this method returns, all old information will no longer be in the cache.
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
        final Map<Long, List<Setting>> newSettings = retrieveEntityToSettingListMap(entities,
                topologyContextId, topologyId);
        cacheLock.writeLock().lock();
        try {
            entityTypeMap = retrieveEntityTypes(entities, topologyContextId);
            settingsByEntity.clear();
            settingsByEntity.putAll(newSettings);
        } finally {
            cacheLock.writeLock().unlock();
        }
        logger.info("Refreshed entity settings cache. It now contains settings for {} entities.",
                settingsByEntity.size());
    }

    /**
     * Get the list of action-orchestrator related settings associated with an entity.
     *
     * @param entityId The ID of the entity.
     * @return A list of settings associated with the entity. This may be empty,
     * but will not be null.
     */
    @Nonnull
    public List<Setting> getSettingsForEntity(final long entityId) {
        cacheLock.readLock().lock();
        try {
            return settingsByEntity.getOrDefault(entityId, Collections.emptyList());
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Get the type of an entity.
     * @param entityId the id of the entity to look up
     * @return an optional containing the associated EntityType if it exists
     */
    public Optional<EntityType> getTypeForEntity(final long entityId) {
        cacheLock.readLock().lock();
        try {
            return Optional.ofNullable(entityTypeMap.get().get(entityId));
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Encountered error when retrieving entity type.", e);
            return Optional.empty();
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Nonnull
    private Future<Map<Long, EntityType>> retrieveEntityTypes(final Set<Long> entities,
                                                              final long topologyContextId) {
        final String uri = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(repositoryHost)
                .port(repositoryPort)
                .path("/repository/serviceentity/query/id")
                .queryParam("projected", true)
                .queryParam("contextId", topologyContextId)
                .build().toUriString();

        final RepositoryRequest request = new RepositoryRequest(entities, uri);
        return executorService.submit(request);
    }

    @Nonnull
    private Map<Long, List<Setting>> retrieveEntityToSettingListMap(final Set<Long> entities,
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
                                    settings -> Collections.unmodifiableList(
                                            settings.getSettingsList()))));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * This class is used to make a request to the repository component for information
     * about entities, and then to process it and return a map of entity ID to entity type.
     *
     * TODO: the HTTP endpoint called by this request should be converted to a GRPC service
     * located within the repository component. this class will likely no longer be necessary
     * when that occurs.
     * TODO: configure a RepositoryListener to listen for onProjectedTopologyAvailable and
     * onProjectedTopologyFailure instead of using retry logic.
     */
    private class RepositoryRequest implements Callable<Map<Long, EntityType>> {

        private final String uri;
        private final Set<Long> entities;
        private ResponseEntity<List<ServiceEntityApiDTO>> response =
                new ResponseEntity<>(Collections.emptyList(), HttpStatus.INTERNAL_SERVER_ERROR);

        private RepositoryRequest(@Nonnull final Set<Long> entities,
                                  @Nonnull final String uri) {
            this.uri = uri;
            this.entities = entities;
        }

        /**
         * {@inheritDoc}
         *
         * This method makes a request to the repository component and, if the request
         * fails or returns an empty result, retries the request within the configured limit.
         * Once the request returns a successful result or hits the retry limit, the result
         * is processed and returned.
         * @return a map of each entityId to EntityType, if found in the repository component.
         * There are no entries for entities not found. If nothing is found or the request fails,
         * an empty map is returned.
         */
        @Override
        public Map<Long, EntityType> call() {
            long iterations = 0;
            boolean keepGoing = true;
            while ((iterations < entityTypeMaxRetries) && keepGoing) {
                keepGoing = runIteration();
                if (keepGoing) {
                    logger.info("Retrying entity types retrieval");
                    try {
                        Thread.sleep(entityTypeRetryIntervalMillis);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while waiting to retry entity fetch", e);
                    }
                }

                iterations++;
            }
            Set<Long> foundEntities = new HashSet<>();
            Map<Long, EntityType> result = new HashMap<>();
            if (response.getBody() != null) {
                response.getBody().forEach(dto -> {
                    long id = Long.valueOf(dto.getUuid());
                    foundEntities.add(id);
                    result.put(id, ClassicEnumMapper.entityType(dto.getClassName()));
                });

                if (!foundEntities.containsAll(entities)) {
                    Set<Long> notFoundEntities = new HashSet<>(entities);
                    notFoundEntities.removeAll(foundEntities);
                    logger.warn("Entities {} not found in the repository component", notFoundEntities);
                }
                logger.info("Finished retrieving entity types for {} entities.", result.size());
            } else {
                logger.error("Could not retrieve entity types " +
                        "from the repository component at {}.", uri);
            }
            return result;
        }

        /**
         * Makes one request to the repository component, and stores the result.
         * @return true if the request succeeded and returned a nontrivial result, and false if
         * the request failed or returned an empty result.
         */
        private boolean runIteration() {
            try {
                response = restTemplate.exchange(uri, HttpMethod.POST, new HttpEntity<>(entities),
                        new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {});
                return response.getBody() != null && response.getBody().isEmpty();
            } catch (RestClientException e) {
                logger.error("Error retrieving service entities by ID from {}:",
                        uri, e);
                return true;
            }
        }
    }
}
