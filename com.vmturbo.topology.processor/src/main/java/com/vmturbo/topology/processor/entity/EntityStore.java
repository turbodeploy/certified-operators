package com.vmturbo.topology.processor.entity;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 * Stores discovered entities.
 */
@ThreadSafe
public class EntityStore {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Map of entityId -> Entity object describing the entity.
     */
    private Map<Long, Entity> entityMap = new ConcurrentHashMap<>();

    /**
     * Map of targetId -> target-specific entity ID information for the target.
     */
    private Map<Long, TargetEntityIdInfo> targetEntities = new HashMap<>();

    /**
     * Lock for writes to the repository.
     */
    private final Object topologyUpdateLock = new Object();

    /**
     * Identity provider to assign ID's to incoming entities.
     */
    private final IdentityProvider identityProvider;

    /**
     * The clock used to generate timestamps for when target information is updated.
     */
    private final Clock clock;

    /**
     * The target store which contains target specific information for the entity.
     */
    private final TargetStore targetStore;

    public EntityStore(@Nonnull final TargetStore targetStore,
                       @Nonnull final IdentityProvider identityProvider,
                       @Nonnull final Clock clock) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.clock = Objects.requireNonNull(clock);
        targetStore.addListener(new TargetStoreListener() {
            @Override
            public void onTargetRemoved(@Nonnull final Target target) {
                EntityStore.this.purgeTarget(target.getId(), null);
            }
        });
    }

    /**
     * Get the entity associated with a given id.
     *
     * The returned entity may be removed from the topology at
     * any point. Users should not store references to the entity
     * for prolonged periods of time to minimize the risk of
     * working with entities that no longer exist.
     *
     * @param entityOid The ID of the entity.
     * @return An optional containing the entity, or an empty optional if the entity is not found.
     */
    @Nonnull
    public Optional<Entity> getEntity(final long entityOid) {
        return Optional.ofNullable(entityMap.get(entityOid));
    }

    /**
     * For a specific target, return a map of entity ID's discovered
     * by that target and the OID's they map to.
     *
     * @param targetId The target in question.
     * @return A map where the key is the "id" field of the EntityDTO discovered
     * on the target, and the value is the oid assigned to that entity.
     */
    @Nonnull
    public Optional<Map<String, Long>> getTargetEntityIdMap(final long targetId) {
        // TODO (roman, July 2016): Investigate whether this lock can cause
        // sending out topology to be slow in large environments.
        synchronized (topologyUpdateLock) {
            final TargetEntityIdInfo targetEntityIdInfo = targetEntities.get(targetId);
            if (targetEntityIdInfo != null) {
                return Optional.of(targetEntityIdInfo.getLocalIdToEntityId());
            } else {
                return Optional.empty();
            }
        }
    }

    @Nonnull
    public Collection<Entity> getAllEntities() {
        return entityMap.values();
    }

    public int entityCount() {
        return entityMap.size();
    }

    /**
     * Constructs the topology based on the entities currently in
     * the repository.
     *
     * @return A map of oid -> {@link TopologyEntityDTO}s representing the entities
     *  in the topology.
     */
    @Nonnull
    public Map<Long, TopologyEntity.Builder> constructTopology() {
        // TODO (roman, July 2016): Investigate the performance
        // effects of doing this synchronously as discovery results come
        // in. Right now assuming the simplicity of implementation makes
        // the intermittent performance hit acceptable.
        synchronized (topologyUpdateLock) {
            return entityMap.values().stream()
                    .map(entity -> entity.constructTopologyDTO(this))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toMap(TopologyEntityDTO.Builder::getOid,
                        TopologyEntity::newBuilder));
        }
    }

    /**
     * Construct a graph suitable for stitching composed of a forest of disconnected graphs consisting
     * of the entities discovered by each individual target. See {@link TopologyStitchingGraph} for
     * further details.
     *
     * @return A {@link TopologyStitchingGraph} suitable for stitching together the graphs in the discovered
     *         individual-targets into a unified topology.
     */
    @Nonnull
    public StitchingContext constructStitchingContext() {
        final StitchingContext.Builder builder = StitchingContext.newBuilder(entityMap.size());
        TargetStitchingDataMap stitchingDataMap;

        synchronized (topologyUpdateLock) {
            stitchingDataMap = new TargetStitchingDataMap(targetEntities);

            // This will populate the stitching data map.
            entityMap.entrySet().stream()
                    .forEach(entry -> {
                        final Long oid = entry.getKey();
                        entry.getValue().getPerTargetInfo().stream()
                                .map(targetInfoEntry ->
                                        StitchingEntityData.newBuilder(targetInfoEntry.getValue().getEntityInfo().toBuilder())
                                                .oid(oid)
                                                .targetId(targetInfoEntry.getKey())
                                                .lastUpdatedTime(getTargetLastUpdatedTime(targetInfoEntry.getKey()).orElse(0L))
                                                .cloud(isCloud(targetInfoEntry.getKey()))
                                                .build())
                                .forEach(stitchingDataMap::put);
                    });
        }

        stitchingDataMap.allStitchingData()
                .forEach(stitchingEntityData -> builder.addEntity(
                        stitchingEntityData,
                        stitchingDataMap.getTargetIdToStitchingDataMap(stitchingEntityData.getTargetId())
                ));

        return builder.build();
    }

    /**
     * Check if the probe type of the given target is cloud (AWS/Azure) or not. This is used in
     * {@link TopologyStitchingGraph#addStitchingData(StitchingEntityData, Map)} to add connected
     * entity. This logic can be removed once EntityDTO itself supports connected relationship.
     *
     * @param targetId the id of the target
     * @return true if the target is AWS or Azure, otherwise false
     */
    private boolean isCloud(long targetId) {
        Optional<SDKProbeType> optionalProbeType = targetStore.getProbeTypeForTarget(targetId);
        if (optionalProbeType.isPresent()) {
            final SDKProbeType probeType = optionalProbeType.get();
            return probeType == SDKProbeType.AWS || probeType == SDKProbeType.AZURE;
        }
        return false;
    }

    /**
     * Remove entities associated with a target from the repository.
     *
     * @param targetId The target to purge.
     * @param shouldRemove Optional. If provided, remove only entities
     *                     where applying this function returns true.
     */
    private void purgeTarget(final long targetId,
                             @Nullable final Function<Entity, Boolean> shouldRemove) {
        synchronized (topologyUpdateLock) {
            final TargetEntityIdInfo idInfo = targetEntities.get(targetId);
            if (idInfo != null) {
                idInfo.getEntityIds().stream()
                    .map(entityMap::get)
                    .filter(Objects::nonNull)
                    .forEach(entity -> {
                        // Remove this target's info.
                        entity.removeTargetInfo(targetId);
                        if (entity.getNumTargets() == 0 && (shouldRemove == null || shouldRemove.apply(entity))) {
                            entityMap.remove(entity.getId());
                        }
                    });

            }
            targetEntities.remove(targetId);
        }
    }

    /**
     * Validates the incoming {@link EntityDTO}s and determines the OIDs of the entities
     * they represent.
     *
     * <p>The validation mainly checks the fields of the {@link EntityDTO} for illegal
     * values. Currently, this method also replaces illegal values with legal placeholders.
     *
     * @param probeId The ID of the probe the entities were discovered on.
     * @param targetId The ID of the target that discovered the {@link EntityDTO}s.
     * @param entityDTOList The list of discovered {@link EntityDTO}s.
     * @return A map from OID to {@link EntityDTO}. If multiple {@link EntityDTO}s map to the
     *         same OID only one of them will appear in this map.
     * @throws EntitiesValidationException If the input list contains {@link EntityDTO}s that
     *      contain illegal values that we don't have workarounds for.
     * @throws IdentityProviderException If during id assignment, those assignments cannot be
     *                                   persisted.
     */
    @Nonnull
    private Map<Long, EntityDTO> assignIdsToEntities(final long probeId,
                                                     final long targetId,
                                                     @Nonnull final List<EntityDTO> entityDTOList)
        throws IdentityUninitializedException,
                    IdentityMetadataMissingException, IdentityProviderException {
        // There may be duplicate entries (though that's a bug in the probes),
        // and we should deal with that without throwing exceptions.
        final Map<Long, EntityDTO> finalEntitiesById = new HashMap<>();

        identityProvider.getIdsForEntities(probeId, entityDTOList)
            .forEach((entityId, entityDto) -> {

            final EntityDTO existingEntry = finalEntitiesById.putIfAbsent(entityId, entityDto);
            if (existingEntry != null) {
                logger.error("Entity with ID {} and local ID {} appears " +
                        "more than once in discovered entities for target {}! The descriptions are {}.\n" +
                        "Entity 1: {}\n Entity 2: {}\n",
                    entityId, existingEntry.getId(), targetId,
                    existingEntry.equals(entityDto) ? "equal" : "not equal",
                    existingEntry, entityDto);
            }
        });

        return finalEntitiesById;
    }

    /**
     * Add entity information to the repository.
     *
     * This will overwrite any existing information associated with the specified target.
     *
     * @param probeId The probe that the target belongs to.
     * @param targetId The target that discovered the entities. Existing entities discovered by this
     *                 target will be purged from the repository.
     * @param entityDTOList The discovered {@link EntityDTO} objects.
     * @throws IdentityUninitializedException If the identity service is uninitialized, and we are
     *  unable to assign IDs to discovered entities.
     * @throws IdentityMetadataMissingException if asked to assign an ID to an {@link EntityDTO}
     *         for which there is no identity metadata.
     */
    public void entitiesDiscovered(final long probeId,
                                   final long targetId,
                                   @Nonnull final List<EntityDTO> entityDTOList)
        throws IdentityUninitializedException, IdentityMetadataMissingException,
                IdentityProviderException {

        final Map<Long, EntityDTO> entitiesById =
            assignIdsToEntities(probeId, targetId, entityDTOList);

        synchronized (topologyUpdateLock) {
            purgeTarget(targetId,
                    // If the entity is not present in the incoming snapshot, then remove it.
                    (entity) -> !entitiesById.containsKey(entity.getId()));

            final ImmutableSet.Builder<Long> newTargetEntitiesBuilder = new ImmutableSet.Builder<>();
            final Map<String, Long> newEntitiesByLocalId = new HashMap<>(entitiesById.size());

            // We want to find the PM that virtual machines are on in order
            // to set the "host" of the entity, which is required for some probes
            // (e.g. hyperV). However, probes don't necessarily set the
            // provider type in the CommoditiesBought.
            // To deal with this:
            //    1) Record the local ID -> EntityType for every discovered entity
            //    2) Record the local ID's of all entities that provide resources to the probe
            //    3) After going through all the entities, cross-reference 2) with 1)
            //       to find the PhysicalMachine that the VM belongs to.

            final Map<String, EntityType> localIdToType = new HashMap<>();
            final Map<Long, List<String>> vmToProviderLocalIds = new HashMap<>();
            final Map<Long, List<String>> containerToProviderLocalIds = new HashMap<>();

            // Assemble the new list of entities associated with this target.
            entitiesById.entrySet().forEach(entry -> {
                Entity entity = entityMap.get(entry.getKey());
                if (entity == null) {
                    entity = new Entity(entry.getKey(), entry.getValue().getEntityType());
                    entityMap.put(entry.getKey(), entity);
                }
                entity.addTargetInfo(targetId, entry.getValue());
                newTargetEntitiesBuilder.add(entity.getId());
                final String localId = entry.getValue().getId();
                if (newEntitiesByLocalId.containsKey(localId)) {
                    // This situation can be caused by multiple entities discovered by a single target
                    // having the same local ID (UUID) with different OIDs. This situation indicates
                    // a bug in the probe.
                    Long existingEntityLocalId = newEntitiesByLocalId.get(entry.getValue().getId());
                    logger.error("Duplicate local ID {} for entities {} and {}",
                        localId, entityMap.get(existingEntityLocalId), entityMap.get(entry.getKey()));
                } else {
                    newEntitiesByLocalId.put(entry.getValue().getId(), entity.getId());
                }

                localIdToType.put(entry.getValue().getId(), entry.getValue().getEntityType());

                if (entry.getValue().getEntityType() == EntityType.VIRTUAL_MACHINE) {
                    vmToProviderLocalIds.put(entry.getKey(), entry.getValue().getCommoditiesBoughtList().stream()
                            .map(CommodityBought::getProviderId)
                            .collect(Collectors.toList()));
                }
                if (entry.getValue().getEntityType() == EntityType.CONTAINER) {
                    containerToProviderLocalIds.put(entry.getKey(), entry.getValue().getCommoditiesBoughtList().stream()
                            .map(CommodityBought::getProviderId)
                            .collect(Collectors.toList()));
                }
            });

            final TargetEntityIdInfo targetIdInfo = new TargetEntityIdInfo(newTargetEntitiesBuilder.build(),
                newEntitiesByLocalId, clock.millis());
            targetEntities.put(targetId, targetIdInfo);

            // Fill in the hosted-by relationships.
            vmToProviderLocalIds.forEach((entityId, localIds) ->
                localIds.stream()
                    .filter(localId -> localIdToType.get(localId) == EntityType.PHYSICAL_MACHINE)
                    .findFirst()
                    .ifPresent(localId -> {
                        // If this is null then the probe's entity information is invalid,
                        // since the VM is buying commodities from a PM that doesn't exist.
                        long pmId = Objects.requireNonNull(targetIdInfo.getLocalIdToEntityId().get(localId));
                        Objects.requireNonNull(entityMap.get(entityId)).setHostedBy(targetId, pmId);
                    })
            );
            containerToProviderLocalIds.forEach((entityId, localIds) ->
                    localIds.stream()
                            .filter(localId -> localIdToType.get(localId) == EntityType.CONTAINER_POD ||
                                    localIdToType.get(localId)==EntityType.VIRTUAL_MACHINE)
                            .findFirst()
                            .ifPresent(localId -> {
                                // If this is null then the probe's entity information is invalid,
                                // since the Container is buying commodities from a Pod or VM that doesn't exist.
                                long pmId = Objects.requireNonNull(targetIdInfo.getLocalIdToEntityId().get(localId));
                                Objects.requireNonNull(entityMap.get(entityId)).setHostedBy(targetId, pmId);
                            })
            );
        }
    }

    /**
     * Puts restored entities in a target's entities map.
     * Also populates the (global) entities map with the new entities.
     *
     * If entities with same OIDs already exist in the global map, then add the additional
     * data as new per-target information just as we would for the same entity being
     * discovered by multiple targets.
     *
     * @param targetId the ID of the target with which the entities are associated
     * @param restoredMap a map from entity OID to entity
     */
    public void entitiesRestored(long targetId, long lastUpdatedTime, Map<Long, EntityDTO> restoredMap) {
        // Create a new per-target map with the restored entities
        final ImmutableSet.Builder<Long> newTargetEntitiesBuilder = new ImmutableSet.Builder<>();
        final ImmutableMap.Builder<String, Long> newEntitiesByLocalIdBuilder = new ImmutableMap.Builder<>();
        for (Entry<Long, EntityDTO> entry : restoredMap.entrySet()) {
            final EntityDTO dto = entry.getValue();
            final long oid = entry.getKey();
            final Entity existingEntity = entityMap.get(oid);

            newTargetEntitiesBuilder.add(oid);
            newEntitiesByLocalIdBuilder.put(dto.getId(), oid);

            if (existingEntity == null) {
                // No information about this entity yet. Create new information to add.
                final Entity newEntity = new Entity(oid, entry.getValue().getEntityType());
                newEntity.addTargetInfo(targetId, dto);
                entityMap.put(oid, newEntity);
            } else {
                // The entity already exists. Append additional per-target information
                // to the existing information.
                existingEntity.addTargetInfo(targetId, dto);
            }
        }

        TargetEntityIdInfo idInfo = new TargetEntityIdInfo(newTargetEntitiesBuilder.build(),
            newEntitiesByLocalIdBuilder.build(),
            lastUpdatedTime);
        // Get rid of the old per-target map and instead use the new one with the restored entities
        targetEntities.put(targetId, idInfo );
    }

    /**
     * Contains target-specific Entity ID information.
     */
    @Immutable
    @ThreadSafe
    private static class TargetEntityIdInfo {
        /*
         * The OIDs of entities this target discovered.
         */
        private ImmutableSet<Long> entityIds;

        /**
         * The map from:
         *    The id of a discovered entity as reported by the probe in EntityDTO.id.
         * To:
         *    The global OID of that entity.
         */
        private Map<String, Long> localIdToEntityId;

        /**
         * The time in millis when this target's entities were last updated.
         */
        private final long lastUpdatedTime;

        TargetEntityIdInfo(@Nonnull final ImmutableSet<Long> entityIds,
                           @Nonnull final Map<String, Long> localIdToEntityId,
                           final long lastUpdatedTime) {
            this.entityIds = Objects.requireNonNull(entityIds);
            this.localIdToEntityId = Collections.unmodifiableMap(localIdToEntityId);
            this.lastUpdatedTime = lastUpdatedTime;
        }

        @Nonnull
        Map<String, Long> getLocalIdToEntityId() {
            return localIdToEntityId;
        }

        Set<Long> getEntityIds() {
            return entityIds;
        }

        public int getDiscoveredEntitiesCount() {
            return localIdToEntityId.size();
        }

        public long getLastUpdatedTime() {
            return lastUpdatedTime;
        }
    }

    /**
     * Return the entity DTOs that were discovered by a target, mapped by OIDs.
     * @param targetId the id of the {@link Target} for which to return the DTOs
     * @return a Map from OID to entity DTO, of the entity DTOs that were discovered
     * by the specified target
     */
    public Map<Long, EntityDTO> discoveredByTarget(long targetId) {
        Map<Long, EntityDTO> map = Maps.newHashMap();
        final Optional<Map<String, Long>> targetEntityIdMap = getTargetEntityIdMap(targetId);
        if (targetEntityIdMap.isPresent()) {
            for (long entityOid : targetEntityIdMap.get().values()) {
                Optional<Entity> entity = getEntity(entityOid);
                if (entity.isPresent()) {
                    Optional<PerTargetInfo> perTargetInfo = entity.get().getTargetInfo(targetId);
                    perTargetInfo.ifPresent(targetInfo ->
                        map.put(entityOid, targetInfo.getEntityInfo()));
                }
            }
        }
        return map;
    }

    public Optional<Long> getTargetLastUpdatedTime(long targetId) {
        synchronized (topologyUpdateLock) {
            return Optional.ofNullable(targetEntities.get(targetId))
                .map(TargetEntityIdInfo::getLastUpdatedTime);
        }
    }

    /**
     * A helper class that retains a map of targetId -> Map<localId, StitchingEntityData>
     */
    private static class TargetStitchingDataMap {
        private final Map<Long, Map<String, StitchingEntityData>> targetDataMap;

        /**
         * Create a new TargetStitchingDataMap given the original mapping of targets to their data.
         *
         * @param sourceMap the original mapping of targets to their discovered data.
         */
        public TargetStitchingDataMap(@Nonnull final Map<Long, TargetEntityIdInfo> sourceMap) {
            targetDataMap = new HashMap<>(sourceMap.size());
            sourceMap.entrySet().forEach(entry ->
                targetDataMap.put(entry.getKey(), new HashMap<>(entry.getValue().getDiscoveredEntitiesCount())));
        }

        public void put(@Nonnull final StitchingEntityData entityData) {
            // get or create the target's entity local id -> stitching entity map
            Map<String,StitchingEntityData> stitchingDataByLocalId
                    = targetDataMap.computeIfAbsent(entityData.getTargetId(), k -> new HashMap<>());
            stitchingDataByLocalId.put(entityData.getEntityDtoBuilder().getId(), entityData);
        }

        public Map<String, StitchingEntityData> getTargetIdToStitchingDataMap(final Long targetId) {
            return targetDataMap.get(targetId);
        }

        public Stream<StitchingEntityData> allStitchingData() {
            return targetDataMap.values().stream()
                .flatMap(targetMap -> targetMap.values().stream());
        }
    }
}
