package com.vmturbo.topology.processor.entity;

import static com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter.entityState;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
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
    private final Map<Long, Entity> entityMap = new ConcurrentHashMap<>();

    /**
     * Map of targetId -> target-specific entity ID information for the target.
     */
    private final Map<Long, TargetEntityIdInfo> targetEntities = new HashMap<>();

    /**
     * Map from targetId to a map of incremental discovered entities ordered by time.
     */
    @GuardedBy("topologyUpdateLock")
    private final Map<Long, TargetIncrementalEntities> targetIncrementalEntities = new HashMap<>();

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
     *
     *  TODO (OM-51214): Remove this dependency to eliminate the circular dependency between
     * EntityStore and {@link CachingTargetStore}.
     * We should never acquire the TargetStore lock within this class. And since which methods
     * are guarded by a lock is implementation-dependent, we really shouldn't be making calls to the
     * TargetStore from this class at all. We should refactor to remove this dependency altogether.
     */
    private final TargetStore targetStore;

    /**
     * Used to send notifications.
     */
    private final TopologyProcessorNotificationSender sender;

    /**
     * All the probe types which support converting layered over and consists of to connected to relationship.
     */
    private static final Set<SDKProbeType> SUPPORTED_CONNECTED_TO_PROBE_TYPES = ImmutableSet.of(
            SDKProbeType.AWS,
            SDKProbeType.AWS_BILLING,
            SDKProbeType.AZURE,
            SDKProbeType.AZURE_EA,
            SDKProbeType.AZURE_STORAGE_BROWSE,
            SDKProbeType.GCP,
            SDKProbeType.VCENTER,
            SDKProbeType.VC_STORAGE_BROWSE,
            SDKProbeType.HYPERV,
            SDKProbeType.VMM);

    /**
     * All the probe categories which support converting layered over and consists of to connected to relationship.
     */
    private static final Set<ProbeCategory> SUPPORTED_CONNECTED_TO_PROBE_CATEGORIES = ImmutableSet.of(
        ProbeCategory.CLOUD_NATIVE);

    /**
     * Mapping from entity type to the list of operations which are performed to apply entities
     * discovered from incremental discoveries to the full topology.
     */
    private static final Map<Integer, List<BiConsumer<EntityDTO.Builder, EntityDTO>>>
        INCREMENTAL_ENTITY_UPDATE_FUNCTIONS = ImmutableMap.of(
            EntityType.PHYSICAL_MACHINE_VALUE, ImmutableList.of(
                (fullEntityBuilder, incrementalEntity) -> {
                    if (incrementalEntity.hasMaintenance()) {
                        fullEntityBuilder.setMaintenance(incrementalEntity.getMaintenance());
                    }
                })
    );

    public EntityStore(@Nonnull final TargetStore targetStore,
                       @Nonnull final IdentityProvider identityProvider,
                       @Nonnull final TopologyProcessorNotificationSender sender,
                       @Nonnull final Clock clock) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.sender = Objects.requireNonNull(sender);
        this.clock = Objects.requireNonNull(clock);
        targetStore.addListener(new TargetStoreListener() {
            @Override
            public void onTargetRemoved(@Nonnull final Target target) {
                final long targetId = target.getId();
                logger.info("Deleting target '{}' ({}) and all related entity data.",
                        target.getDisplayName(), targetId);
                EntityStore.this.purgeTarget(targetId, null);
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
     * Choose an EntityDTO from the raw entity to use to represent this entity
     *
     * See {@link #chooseEntityDTO(Entity)} for more details on how the selection is performed
     *
     * @param entityOid the ID of an entity
     * @return an EntityDTO representing the data discovered by the primary target for the entity
     */
    public EntityDTO chooseEntityDTO(final long entityOid) {
        // choose an entityDTO to use based on its origin. The 'DISCOVERED' entityDTO is preferred.
        return getEntity(entityOid).map(this::chooseEntityDTO).orElseThrow(() ->
            new EntityNotFoundException("Could not find matching entity with oid " + entityOid +
                " in the store of raw discovered entity data."));
    }

    /**
     * Choose an EntityDTO from the raw entity to use to represent this entity
     * An EntityDTO with origin set to 'discovered' will be preferred, followed by origins of
     * "replaceable" and finally "proxy".
     * In cases where multiple EntityDTOs have the same origin value, the first EntityDTO found will
     * be chosen.
     *
     * Example: For a VM, the EntityDTO with origin of "DISCOVERED" would generally have been
     *   discovered by a Hypervisor or a Cloud target
     *
     * @param entity an object representing the data discovered by all targets for the entity
     * @return an EntityDTO representing the data discovered by the primary target for the entity
     */
    public EntityDTO chooseEntityDTO(final Entity entity) {
        // This could alternatively be achieved by sorting the EntityDTOs by origin type and then
        // picking the first element from the list. However, the intent seems more clear in the
        // current implementation.
        return findEntityDTObyOrigin(entity, EntityOrigin.DISCOVERED)
            .orElseGet(() -> findEntityDTObyOrigin(entity, EntityOrigin.REPLACEABLE)
                .orElseGet(() -> findEntityDTObyOrigin(entity, EntityOrigin.PROXY)
                    .orElseThrow(() -> new EntityNotFoundException("Could not find EntityDTO " +
                        "of origin 'DISCOVERED', 'REPLACEABLE' or 'PROXY' in the raw " +
                        "entity data for entity " + entity.getId()))));
    }

    /**
     * Find the first EntityDTO in the Entity that matches the provided EntityOrigin
     *
     * @param entity the Entity containing EntityDTOs to be searched
     * @param entityOrigin the origin type being selected on
     * @return the first EntityDTO in the Entity that matches the provided EntityOrigin
     */
    private Optional<EntityDTO> findEntityDTObyOrigin(final Entity entity,
                                                      final EntityOrigin entityOrigin) {
        return entity.allTargetInfo().stream()
            .map(PerTargetInfo::getEntityInfo)
            // Find the first EntityDTO whose origin matches the provided EntityOrigin
            .filter(entityDTO -> entityOrigin.equals(entityDTO.getOrigin()))
            .findFirst();
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
        final StitchingContext.Builder builder = StitchingContext
            .newBuilder(entityMap.size(), targetStore)
            .setIdentityProvider(identityProvider);
        TargetStitchingDataMap stitchingDataMap;

        synchronized (topologyUpdateLock) {
            stitchingDataMap = new TargetStitchingDataMap(targetEntities);

            // This will populate the stitching data map.
            entityMap.forEach((oid, entity) ->
                entity.getPerTargetInfo().stream()
                    .map(targetInfoEntry -> {
                        final long targetId = targetInfoEntry.getKey();
                        // apply changes of cached entities from incremental discovery
                        final EntityDTO.Builder entityDTO = applyIncrementalChanges(
                            targetInfoEntry.getValue().getEntityInfo().toBuilder(), oid, targetId);
                        return StitchingEntityData.newBuilder(entityDTO)
                            .oid(oid)
                            .targetId(targetId)
                            .lastUpdatedTime(getTargetLastUpdatedTime(targetId).orElse(0L))
                            .supportsConnectedTo(supportsConnectedTo(targetId))
                            .build();
                    }).forEach(stitchingDataMap::put));
        }

        stitchingDataMap.allStitchingData()
            .forEach(stitchingEntityData -> {
                try {
                    builder.addEntity(
                        stitchingEntityData,
                        stitchingDataMap.getTargetIdToStitchingDataMap(stitchingEntityData.getTargetId()));
                } catch (IllegalArgumentException | NullPointerException e) {
                    // We want to make sure we don't block the whole broadcast if one entity
                    // encounters an error.
                    logger.error("Failed to add entity " +
                        stitchingEntityData + " to stitching context due to error.", e);
                }
            });

        return builder.build();
    }

    /**
     * Apply cached entities discovered from incremental discoveries to the given EntityDTO.
     * It takes all incremental responses which happened after last successful full discovery
     * (older ones have already been cleared once a full discovery finishes), and apply them
     * in the order of discovery request, the last one wins.
     *
     * <p>We don't support adding new entity or deleting existing entity from incremental results
     * for now. We only support updating partial fields of existing entity, like updating the
     * maintenance field for host.
     *
     * @param entityDTO the builder of the EntityDTO to apply incremental changes on
     * @param entityOid assigned oid for the entity
     * @param targetId id of the target where this entity comes from
     * @return the updated EntityDTO builder
     */
    @GuardedBy("topologyUpdateLock")
    private EntityDTO.Builder applyIncrementalChanges(@Nonnull EntityDTO.Builder entityDTO,
                                                      long entityOid, long targetId) {
        final Optional<TargetIncrementalEntities> incrementalEntities = getIncrementalEntities(targetId);
        if (!incrementalEntities.isPresent()) {
            // nothing to apply
            return entityDTO;
        }

        final List<BiConsumer<EntityDTO.Builder, EntityDTO>> updateFunctions =
            INCREMENTAL_ENTITY_UPDATE_FUNCTIONS.get(entityDTO.getEntityType().getNumber());
        if (updateFunctions == null) {
            // no update functions
            logger.debug("No incremental update functions provided for entity type {}",
                entityDTO.getEntityType());
            return entityDTO;
        }

        incrementalEntities.get().applyEntitiesInDiscoveryOrder(entityOid,
            (messageId, incrementalEntity) -> {
                switch (incrementalEntity.getUpdateType()) {
                    case UPDATED:
                    case PARTIAL:
                        logger.debug("Applying incremental EntityDTO (target: {}, message id: {}):{}",
                            () -> targetId, () -> messageId, () -> incrementalEntity);
                        updateFunctions.forEach(updateFunction ->
                            updateFunction.accept(entityDTO, incrementalEntity));
                        break;
                    default:
                        logger.error("Unsupported update type {} for entity {} from target {}, message id: {}",
                            incrementalEntity.getUpdateType(), incrementalEntity, targetId, messageId);
                }
            });

        return entityDTO;
    }

    /**
     * Check if the probe type or probe category of the given target uses layeredOver and consistsOf
     * in the DTO to represent normal connectedTo relationships and owns connectedTo relationships
     * respectively or not. This is used in
     * {@link TopologyStitchingGraph#addStitchingData(StitchingEntityData, Map)} to add connected
     * entity. This logic can be removed once EntityDTO itself supports connected relationship.
     *
     * @param targetId the id of the target
     * @return true if the target is in {@link #SUPPORTED_CONNECTED_TO_PROBE_TYPES} or
     * {@link #SUPPORTED_CONNECTED_TO_PROBE_CATEGORIES} otherwise false
     */
    private boolean supportsConnectedTo(long targetId) {
        return targetStore.getProbeTypeForTarget(targetId)
            .map(SUPPORTED_CONNECTED_TO_PROBE_TYPES::contains).orElse(false) ||
             targetStore.getProbeCategoryForTarget(targetId)
                 .map(SUPPORTED_CONNECTED_TO_PROBE_CATEGORIES::contains).orElse(false);
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
                logger.info("Purging entity data from target {}.", targetId);
                idInfo.getEntityIds().stream()
                    .map(entityMap::get)
                    .filter(Objects::nonNull)
                    .forEach(entity -> {
                        // Remove this target's info.
                        entity.removeTargetInfo(targetId);
                        final long entityId = entity.getId();
                        final EntityType entityType = entity.getEntityType();
                        if (entity.getNumTargets() == 0 &&
                                (shouldRemove == null || shouldRemove.apply(entity))) {
                            logger.debug("Removing entity {} of type {} from the topology due to "
                                    + "the purging of target {}.", entityId, entityType, targetId);
                            entityMap.remove(entityId);
                        } else {
                            logger.trace("Skipping removal of entity {} of type {} from the topology.",
                                    entityId, entityType);
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
     * Insert the entities for the given target into existing store.
     *
     * @param targetId id of the target to insert entities for
     * @param messageId mediation message identifier that is relating the response to the initial
     *                  request, which is used to indicate relative timing of the discovery order
     * @param entitiesById map from entity oid to {@link EntityDTO}
     * @throws TargetNotFoundException if the target can not be found
     */
    private void setFullDiscoveryEntities(final long targetId, final int messageId,
                                      @Nonnull final Map<Long, EntityDTO> entitiesById)
            throws TargetNotFoundException {
        synchronized (topologyUpdateLock) {
            // Ensure that the target exists; avoid adding entities for targets that have been removed
            // TODO (OM-51214): Remove this check when we have better overall synchronization of
            // target operations. This call would lead to a deadlock if it grabbed the storeLock
            // while holding the topologyUpdateLock. Currently, TargetStore::getTarget does not
            // acquire the storeLock, allowing this check to work as a stop-gap.
            final Optional<Target> target = targetStore.getTarget(targetId);
            if (!target.isPresent()) {
                throw new TargetNotFoundException(targetId);
            }

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
                    logger.debug("Adding new entity {} of type {} to the topology.", entity.getId(),
                            entity.getEntityType());
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

            // clear all incremental responses which are triggered before current successful full discovery
            getIncrementalEntities(targetId).ifPresent(incrementalEntities ->
                incrementalEntities.clearEntitiesDiscoveredBefore(messageId));

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
     * Add entity information to the repository.
     *
     * This will overwrite any existing information associated with the specified target.
     *
     * @param probeId The probe that the target belongs to.
     * @param targetId The target that discovered the entities. Existing entities discovered by this
     *                 target will be purged from the repository.
     * @param messageId mediation message identifier that is relating the response to the initial
     *                  request, which is used to indicate relative timing of the discovery order
     * @param discoveryType type of the discovery
     * @param entityDTOList The discovered {@link EntityDTO} objects.
     * @throws IdentityUninitializedException If the identity service is uninitialized, and we are
     *  unable to assign IDs to discovered entities.
     * @throws IdentityMetadataMissingException if asked to assign an ID to an {@link EntityDTO}
     *         for which there is no identity metadata.
     * @throws IdentityProviderException If during id assignment, those assignments cannot be
     *         persisted.
     * @throws TargetNotFoundException if no target exists with the provided targetId
     */
    public void entitiesDiscovered(final long probeId, final long targetId, final int messageId,
        @Nonnull DiscoveryType discoveryType, @Nonnull final List<EntityDTO> entityDTOList)
            throws IdentityUninitializedException, IdentityMetadataMissingException,
                    IdentityProviderException, TargetNotFoundException {
        // this applies to all discovery types, since there may be new entities in incremental response
        final Map<Long, EntityDTO> entitiesById =
            assignIdsToEntities(probeId, targetId, entityDTOList);

        if (discoveryType == DiscoveryType.FULL) {
            setFullDiscoveryEntities(targetId, messageId, entitiesById);
        } else if (discoveryType == DiscoveryType.INCREMENTAL) {
            // Send partial entityDTOs of entity that changed state
            sendEntitiesWithNewState(entitiesById);
            // cache the entities from incremental discovery response
            addIncrementalDiscoveryEntities(targetId, messageId, entitiesById);
        }
    }

    private void sendEntitiesWithNewState(Map<Long, EntityDTO> entitiesById) {
        Builder entitiesWithNewStateBuilder = EntitiesWithNewState.newBuilder();
        for (Long entityOid: entitiesById.keySet()) {
            EntityDTO entityDTO = entitiesById.get(entityOid);
            if (entityDTO.getEntityType() == EntityType.PHYSICAL_MACHINE) {
                entitiesWithNewStateBuilder.addTopologyEntity(TopologyEntityDTO.newBuilder()
                    .setOid(entityOid)
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setEntityState(entityState(entityDTO))
                    .build());
            }
        }
        if (entitiesWithNewStateBuilder.getTopologyEntityCount() > 0) {
            entitiesWithNewStateBuilder.setStateChangeId(identityProvider.generateTopologyId());
            EntitiesWithNewState entitiesWithNewStateMessage = entitiesWithNewStateBuilder.build();
            try {
                sender.onEntitiesWithNewState(entitiesWithNewStateMessage);
            } catch (Exception e) {
                logger.error("Problem occurred while sending entity state change message {}",
                    entitiesWithNewStateMessage);
            }
        }
    }

    /**
     * Add the entities discovered from incremental discovery to the cache.
     *
     * @param targetId id of the target to insert entities for
     * @param messageId mediation message identifier that is relating the response to the initial
     *                  request, which is used to indicate relative timing of the discovery order
     * @param entitiesById map from entity oid to {@link EntityDTO}
     * @throws TargetNotFoundException if the target can not be found
     */
    private void addIncrementalDiscoveryEntities(final long targetId, int messageId,
            @Nonnull final Map<Long, EntityDTO> entitiesById) throws TargetNotFoundException {
        synchronized (topologyUpdateLock) {
            final Optional<Target> target = targetStore.getTarget(targetId);
            if (!target.isPresent()) {
                throw new TargetNotFoundException(targetId);
            }

            final Map<Long, EntityDTO> newEntityDTOsByOid = new HashMap<>(entitiesById.size());
            final Set<String> localIds = new HashSet<>(entitiesById.size());
            // sanity check for duplicate local ids in response, which indicates a bug in probe
            entitiesById.forEach((entityOid, entityDTO) -> {
                final String localId = entityDTO.getId();
                if (localIds.contains(entityDTO.getId())) {
                    logger.error("Duplicate local ID {} for entity {} and {}, keeping the first one",
                        localId, newEntityDTOsByOid.get(entityOid), entityDTO);
                } else {
                    localIds.add(entityDTO.getId());
                    newEntityDTOsByOid.put(entityOid, entityDTO);
                }
            });

            // add to incremental cache
            targetIncrementalEntities.computeIfAbsent(targetId, k -> new TargetIncrementalEntities())
                .addEntities(messageId, newEntityDTOsByOid);
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
     * @param lastUpdatedTime unused. entity lastUpdatedTime will be set to the current clock time
     * @param restoredMap a map from entity OID to entity
     * @throws TargetNotFoundException if no target exists with the provided targetId
     */
    public void entitiesRestored(long targetId, long lastUpdatedTime,
                                 Map<Long, EntityDTO> restoredMap) throws TargetNotFoundException {
        logger.info("Restoring {} entities for target {}", restoredMap.size(), targetId);
        // the messageId doesn't matter here, since there will not be any incremental discoveries
        // after loading diags
        setFullDiscoveryEntities(targetId, 0, restoredMap);
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
     * Get the cached entities for a target discovered from incremental discoveries.
     *
     * @param targetId id of the target to get incremental entities for
     * @return optional incremental entities, or empty if not any
     */
    @VisibleForTesting
    public synchronized Optional<TargetIncrementalEntities> getIncrementalEntities(long targetId) {
        return Optional.ofNullable(targetIncrementalEntities.get(targetId));
    }

    /**
     * Contains entities discovered from incremental discovery ordered by discovery.
     */
    @ThreadSafe
    public static class TargetIncrementalEntities {

        /**
         * Map from entity oid to an ordered map of incremental entities by message id.
         */
        private final Map<Long, TreeMap<Integer, EntityDTO>> orderedIncrementalEntities = new TreeMap<>();

        /**
         * Add the given entities and related discovery message id to the cache. If a discovery
         * happens later, it will have larger message id.
         *
         * @param messageId mediation message identifier that is relating the response to the initial
         *                  request, which is used to indicate relative timing of the discovery order
         * @param entitiesById map of entities
         */
        public synchronized void addEntities(int messageId, @Nonnull Map<Long, EntityDTO> entitiesById) {
            entitiesById.forEach((oid, entity) ->
                orderedIncrementalEntities.computeIfAbsent(oid, k -> new TreeMap<>())
                    .put(messageId, entity));
        }

        /**
         * Clear all entities which are discovered before the given messageId.
         *
         * @param messageId mediation message identifier that is relating the response to the initial
         *                  request, which is used to indicate relative timing of the discovery order
         */
        public synchronized void clearEntitiesDiscoveredBefore(int messageId) {
            orderedIncrementalEntities.values().forEach(orderedEntities ->
                orderedEntities.headMap(messageId).clear());
        }

        /**
         * Apply all the incremental changes for a given entity, in the order of discovery request.
         *
         * @param entityOid oid of the entity to apply incremental changes to
         * @param operation the operation that applies incremental changes to full entity dto
         */
        public synchronized void applyEntitiesInDiscoveryOrder(long entityOid,
                @Nonnull BiConsumer<Integer, EntityDTO> operation) {
            final TreeMap<Integer, EntityDTO> entityInDiscoveryOrder =
                orderedIncrementalEntities.get(entityOid);
            if (entityInDiscoveryOrder != null) {
                entityInDiscoveryOrder.forEach(operation);
            }
        }

        /**
         * Get all the incremental EntityDTOs for a given entity, ordered by discovery request.
         * This is only used for testing purpose.
         *
         * @param entityOid oid of the entity to get incremental dtos for
         * @return incremental entities (EntityDTO by message id) ordered by discovery request.
         */
        @VisibleForTesting
        public synchronized SortedMap<Integer, EntityDTO> getEntitiesInDiscoveryOrder(long entityOid) {
            final TreeMap<Integer, EntityDTO> entityInDiscoveryOrder =
                orderedIncrementalEntities.get(entityOid);
            if (entityInDiscoveryOrder == null) {
                return Collections.emptySortedMap();
            }
            return new TreeMap<>(entityInDiscoveryOrder);
        }

        /**
         * Get a map where each message id is mapped to the corresponding incremental EntityDTOs.
         *
         * @return the map
         */
        public synchronized LinkedHashMap<Integer, Collection<EntityDTO>> getEntitiesByMessageId() {
            final LinkedHashMap<Integer, Collection<EntityDTO>> messageIdToEntityDtos =
                new LinkedHashMap<>();
            for (TreeMap<Integer, EntityDTO> messageIdToEntity : orderedIncrementalEntities.values()) {
                for (Integer messageId : messageIdToEntity.keySet()) {
                    if (messageIdToEntityDtos.containsValue(messageId)) {
                        Collection<EntityDTO> currentEntityDTOs = messageIdToEntityDtos.get(messageId);
                        currentEntityDTOs.add(messageIdToEntity.get(messageId));
                        messageIdToEntityDtos.put(messageId,currentEntityDTOs);
                    } else {
                        messageIdToEntityDtos.put(messageId,
                            Collections.singletonList(messageIdToEntity.get(messageId)));
                    }
                }
            }
            return messageIdToEntityDtos;
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
