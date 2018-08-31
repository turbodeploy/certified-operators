package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * The {@link EntitySettingStore} is responsible for managing the storage and retrieval
 * of settings applied to entities. It does not do setting resolution (settings must be resolved
 * elsewhere and injected via {@link EntitySettingStore#storeEntitySettings(long, long, Stream)}.
 * It only provides an interface to query available entity -> setting mappings.
 */
@ThreadSafe
public class EntitySettingStore {

    private static final DataMetricSummary ENTITY_SETTING_STORE_UPDATE_DURATION = DataMetricSummary
            .builder()
            .withName("group_entity_setting_update_duration_seconds")
            .withHelp("Duration in seconds it takes to update the entity setting store")
            .build()
            .register();

    private static final DataMetricCounter ENTITY_SETTING_STORE_QUERY_HIT_COUNT = DataMetricCounter
            .builder()
            .withName("group_entity_setting_hit_count")
            .withHelp("Number of query operations attempted on the entity setting store.")
            .build()
            .register();

    private static final DataMetricCounter ENTITY_SETTING_STORE_QUERY_ERROR_COUNT = DataMetricCounter
            .builder()
            .withName("group_entity_setting_error_count")
            .withHelp("Number of query errors encountered by the entity setting store.")
            .build()
            .register();

    private static final DataMetricSummary ENTITY_SETTING_STORE_QUERY_DURATION = DataMetricSummary
            .builder()
            .withName("group_entity_setting_query_duration_seconds")
            .withHelp("Duration in seconds it takes to update the entity setting store")
            .build()
            .register();

    private final Logger logger = LogManager.getLogger();

    /**
     * Topology context ID -> snapshot cache for the context.
     */
    private final Map<Long, ContextSettingSnapshotCache> entitySettingSnapshots =
            Collections.synchronizedMap(new HashMap<>());

    private final EntitySettingSnapshotFactory snapshotFactory;

    private final ContextSettingSnapshotCacheFactory cacheFactory;

    private final SettingStore settingStore;

    private final long realtimeTopologyContextId;

    public EntitySettingStore(final long realtimeTopologyContextId,
                              @Nonnull final SettingStore settingStore) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.snapshotFactory = EntitySettingSnapshot::new;
        this.cacheFactory = ContextSettingSnapshotCache::new;
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    @VisibleForTesting
    EntitySettingStore(final long realtimeTopologyContextId,
                       @Nonnull final SettingStore settingStore,
                       @Nonnull final EntitySettingSnapshotFactory snapshotFactory,
                       @Nonnull final ContextSettingSnapshotCacheFactory cacheFactory) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.snapshotFactory = Objects.requireNonNull(snapshotFactory);
        this.cacheFactory = Objects.requireNonNull(cacheFactory);
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    /**
     * Store resolved entity settings in the {@link EntitySettingStore}. Settings are always
     * resolved in the context of some (contextId, topologyId) pair, and users of this method are
     * required to provide those.
     * <p>
     * The {@link EntitySettingStore} only keeps settings for a number of topologies per context ID.
     * Storing entity settings for the same (contextId, topologyId) pair will overwrite any existing
     * settings. Storing settings with different topology IDs within the same context IDs may cause
     * settings for older topologies to disappear, if the number of settings for that context
     * exceeds the threshold.
     *
     * @param topologyContextId The context ID for the settings.
     * @param topologyId The topology ID for the settings.
     * @param entitySettings A stream of settings applied to entities in this topology.
     * @throws org.jooq.exception.DataAccessException If there is an error connecting to the
     *        database (required to retrieve default setting policies).
     */
    public void storeEntitySettings(final long topologyContextId,
                                    final long topologyId,
                                    @Nonnull final Stream<EntitySettings> entitySettings) {
        try (final DataMetricTimer timer = ENTITY_SETTING_STORE_UPDATE_DURATION.startTimer()) {
            final Map<Long, SettingPolicy> defaultPolicies = settingStore.getSettingPolicies(
                    SettingPolicyFilter.newBuilder()
                            .withType(Type.DEFAULT)
                            .build())
                    .collect(Collectors.toMap(SettingPolicy::getId, Function.identity()));
            final EntitySettingSnapshot newSnapshot =
                    snapshotFactory.createSnapshot(entitySettings, defaultPolicies);
            final ContextSettingSnapshotCache settingCache =
                    entitySettingSnapshots.computeIfAbsent(topologyContextId,
                            k -> cacheFactory.newSnapshotCache());
            final Optional<EntitySettingSnapshot> existing =
                    settingCache.addSnapshot(topologyId, newSnapshot);
            existing.ifPresent(existingSnapshot -> logger.warn("Replacing existing entity setting" +
                    " snapshot for context {} and topology {}", topologyContextId, topologyId));
        }
    }

    /**
     * Get entity settings stored via
     * {@link EntitySettingStore#storeEntitySettings(long, long, Stream)}.
     *
     * @param topologySelection The {@link TopologySelection} to apply to get the topology to
     *                          get settings from.
     * @param filter The {@link EntitySettingFilter} to use to filter which settings to return.
     * @return A map from entity OID to the list of settings for that entity.
     *         All entities specified in the {@link EntitySettingFilter} will have entries in the map.
     * @throws NoSettingsForTopologyException If there is no setting information for entities in
     *      the topology specified by the input filter.
     */
    @Nonnull
    public Map<Long, Collection<Setting>> getEntitySettings(@Nonnull final TopologySelection topologySelection,
                                                      @Nonnull final EntitySettingFilter filter)
            throws NoSettingsForTopologyException {
        ENTITY_SETTING_STORE_QUERY_HIT_COUNT.increment();
        try (final DataMetricTimer timer = ENTITY_SETTING_STORE_QUERY_DURATION.startTimer()) {
            final long contextId = topologySelection.hasTopologyContextId() ?
                    topologySelection.getTopologyContextId() : realtimeTopologyContextId;
            final ContextSettingSnapshotCache contextCache = entitySettingSnapshots.get(contextId);
            if (contextCache == null) {
                throw new NoSettingsForTopologyException(contextId);
            }
            final EntitySettingSnapshot snapshot;
            if (topologySelection.hasTopologyId()) {
                snapshot = contextCache.getSnapshot(topologySelection.getTopologyId())
                        .orElseThrow(() -> new NoSettingsForTopologyException(contextId,
                                topologySelection.getTopologyId()));
            } else {
                snapshot = contextCache.getLatestSnapshot()
                        // We may have a briefly empty setting cache that hasn't been fully initialized
                        // yet. Treat it as if it doesn't exist.
                        .orElseThrow(() -> new NoSettingsForTopologyException(contextId));
            }
            return snapshot.getFilteredSettings(filter);
        } catch (NoSettingsForTopologyException | RuntimeException e) {
            ENTITY_SETTING_STORE_QUERY_ERROR_COUNT.increment();
            throw e;
        }
    }

    /**
     * Exception thrown when no entity settings are found for a topology specified by a
     * {@link TopologySelection}.
     */
    public static class NoSettingsForTopologyException extends Exception {
        public NoSettingsForTopologyException(final long contextId) {
            super("No settings for topology context " + contextId);
        }

        public NoSettingsForTopologyException(final long contextId, final long topologyId) {
            super("No settings for topology " + topologyId + " in context " + contextId);
        }
    }

    /**
     * A factory for {@link ContextSettingSnapshotCache} to allow mock injection
     * for unit testing.
     */
    @FunctionalInterface
    interface ContextSettingSnapshotCacheFactory {
        ContextSettingSnapshotCache newSnapshotCache();
    }

    /**
     * The {@link ContextSettingSnapshotCache} keeps the last X {@link EntitySettingSnapshot}s,
     * in order of insertion.
     */
    @VisibleForTesting
    @ThreadSafe
    static class ContextSettingSnapshotCache {

        /**
         * The number of snapshots to retain.
         */
        private static final int SNAPSHOTS_TO_RETAIN = 2;

        @GuardedBy("cacheLock")
        private final Map<Long, EntitySettingSnapshot> cache = new LinkedHashMap<Long, EntitySettingSnapshot>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, EntitySettingSnapshot> eldest) {
                // The method is called AFTER an insertion, so return true only
                // if the insertion took us over the number of topologies to retain.
                return size() > SNAPSHOTS_TO_RETAIN;
            }
        };

        /**
         * The latest {@link EntitySettingSnapshot} inserted into the cache.
         */
        @GuardedBy("cacheLock")
        private EntitySettingSnapshot latestSnapshot = null;

        /**
         * A lock to allow updating the cache and the latest snapshot at the same time.
         */
        private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

        /**
         * Add a snapshot to the cache. Drop an old snapshot if the number of snapshots for the
         * topology ID exceeds the number of snapshots to retain.
         *
         * @param topologyId The ID of the topology that identifies the snapshot. If there is
         *                   already a snapshot for this ID, the new snapshot will replace the
         *                   old one.
         * @param snapshot The {@link EntitySettingSnapshot}.
         * @return An optional containing the existing snapshot for this topology ID, if any.
         *         This behaves just like {@link Map#put(Object, Object)}.
         */
        @Nonnull
        public Optional<EntitySettingSnapshot> addSnapshot(final long topologyId,
                                               @Nonnull final EntitySettingSnapshot snapshot) {
            cacheLock.writeLock().lock();
            try {
                latestSnapshot = snapshot;
                return Optional.ofNullable(cache.put(topologyId, snapshot));
            } finally {
                cacheLock.writeLock().unlock();
            }
        }

        /**
         * Get the {@link EntitySettingSnapshot} associated with a particular topology.
         * @param topologyId The ID of the topology.
         * @return An optional containing the {@link EntitySettingSnapshot}, or an empty optional
         *         if there is no data for that topology.
         */
        @Nonnull
        public Optional<EntitySettingSnapshot> getSnapshot(final long topologyId) {
            cacheLock.readLock().lock();
            try {
                return Optional.ofNullable(cache.get(topologyId));
            } finally {
                cacheLock.readLock().unlock();
            }
        }

        /**
         * Get the last {@link EntitySettingSnapshot} inserted into the cache.
         *
         * @return An optional containing the {@link EntitySettingSnapshot}, or an empty optional
         *         if there is no in the cache.
         */
        @Nonnull
        public Optional<EntitySettingSnapshot> getLatestSnapshot() {
            cacheLock.readLock().lock();
            try {
                return Optional.ofNullable(latestSnapshot);
            } finally {
                cacheLock.readLock().unlock();
            }
        }
    }

    /**
     * A factory for {@link EntitySettingSnapshot} to allow mock injection during unit tests.
     */
    @VisibleForTesting
    @FunctionalInterface
    interface EntitySettingSnapshotFactory {
        EntitySettingSnapshot createSnapshot(@Nonnull final Stream<EntitySettings> entitySettings,
                                         @Nonnull final Map<Long, SettingPolicy> defaultPolicies);
    }

    /**
     * The {@link EntitySettingSnapshot} represents the settings assigned to an entity
     * in a particular (contextId, topologyId) pair.
     */
    @VisibleForTesting
    @Immutable
    @ThreadSafe
    static class EntitySettingSnapshot {
        private static final Logger LOGGER = LogManager.getLogger();

        private final Map<Long, EntitySettings> settingsByEntity;

        private final Map<Long, SettingPolicy> defaultPolicies;

        @VisibleForTesting
        EntitySettingSnapshot(@Nonnull final Stream<EntitySettings> entitySettingsStream,
                              @Nonnull final Map<Long, SettingPolicy> defaultPolicies) {
            final ImmutableMap.Builder<Long, EntitySettings> settingsByEntityBuilder =
                    ImmutableMap.builder();
            entitySettingsStream.forEach(entitySettings -> {
                if (!entitySettings.hasEntityOid()) {
                    LOGGER.warn("Skipping entity with no OID.");
                    // "return" acts as "continue" in foreach.
                    return;
                }

                if (entitySettings.hasDefaultSettingPolicyId()) {
                    final long defaultSettingPolicyId = entitySettings.getDefaultSettingPolicyId();
                    if (!defaultPolicies.containsKey(defaultSettingPolicyId)) {
                        LOGGER.warn("Entity settings for entity {} specify invalid default policy ID {}",
                                entitySettings.getEntityOid(), defaultSettingPolicyId);
                        // "return" acts as "continue" in foreach.
                        return;
                    }
                }
                settingsByEntityBuilder.put(entitySettings.getEntityOid(), entitySettings);
            });
            this.settingsByEntity = settingsByEntityBuilder.build();
            this.defaultPolicies = defaultPolicies;
        }

        /**
         * Get the settings that match a filter.
         *
         * @param filter The filter to apply to the settings.
         * @return A map from entityId to the list of settings that match the input filter for
         *         that entity. If the filter contains an explicit set of IDs, the map will
         *         contain an entry for every specified ID, with some values being empty.
         *         If the filter does not contain an explicit set of IDs, the map will contain an
         *         entry for every entity that has settings.
         */
        @Nonnull
        public Map<Long, Collection<Setting>> getFilteredSettings(final EntitySettingFilter filter) {
            final Set<Long> ids = filter.getEntitiesList().isEmpty() ?
                    settingsByEntity.keySet() : Sets.newHashSet(filter.getEntitiesList());
            return ids.stream()
                    .collect(Collectors.toMap(Function.identity(),
                            this::getEntitySettings));
        }

        @Nonnull
        private Collection<Setting> getEntitySettings(@Nonnull final Long id) {
            final EntitySettings userSettings = settingsByEntity.get(id);
            if (userSettings == null) {
                return Collections.emptyList();
            }

            final Collection<Setting> settings = new HashSet<>();

            // First add all user settings
            settings.addAll(userSettings.getUserSettingsList());
            final Set<String> specsPresent = settings.stream().map(Setting::getSettingSpecName)
                .collect(Collectors.toSet());

            // Fill in default settings, if any.
            if (userSettings.hasDefaultSettingPolicyId()) {
                final SettingPolicy defaultSettingPolicy =
                        defaultPolicies.get(userSettings.getDefaultSettingPolicyId());
                if (defaultSettingPolicy != null) {
                    defaultSettingPolicy.getInfo().getSettingsList().stream()
                        .filter(setting -> !specsPresent.contains(setting.getSettingSpecName()))
                        .forEach(settings::add);
                } else {
                    // This shouldn't happen, because we checked that the default setting policy
                    // exists when constructing the snapshot.
                    LOGGER.error("Default setting policy {} somehow missing from snapshot.",
                            userSettings.getDefaultSettingPolicyId());
                }
            }

            return settings;
        }
    }

    /**
     * Returns the {@link realtimeTopologyContextId}.
     *
     * @return realtimeTopologyContextId
     */
    public long getRealtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }
}
