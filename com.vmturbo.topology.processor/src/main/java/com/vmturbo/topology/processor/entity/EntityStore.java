package com.vmturbo.topology.processor.entity;

import static com.vmturbo.common.protobuf.topology.UIMapping.getUserFacingCategoryString;
import static com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter.entityState;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import common.HealthCheck.HealthState;
import it.unimi.dsi.fastutil.longs.Long2IntArrayMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Pair;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.logmessagegrouper.LogMessageGrouper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PhysicalMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
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
    private static final TargetHealth DEFAULT_TARGET_HEALTH =
            TargetHealth.newBuilder().setHealthState(HealthState.NORMAL).build();

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

    private final Map<Long, Map<Long, EntityIdentifyingPropertyValues>> targetEntityIdentifyingPropertyValues
        = new ConcurrentHashMap<>();

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

    private final Set<EntityType> reducedEntityTypes;

    private final boolean useSerializedEntities;

    @Autowired
    private TargetHealthRetriever targetHealthRetriever;

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

    private final InternalDuplicateTargetDetector duplicateTargetDetector;

    private final List<EntitiesWithNewStateListener> entitiesWithNewStateListeners;

    /**
     * Enable entity details support.
     */
    private boolean entityDetailsEnabled = false;

    /**
     * All the probe types which support converting layered over and consists of to connected to relationship.
     */
    private static final Set<SDKProbeType> SUPPORTED_CONNECTED_TO_PROBE_TYPES = ImmutableSet.of(
            SDKProbeType.AWS,
            SDKProbeType.AWS_BILLING,
            SDKProbeType.AZURE,
            SDKProbeType.AZURE_EA,
            SDKProbeType.AZURE_STORAGE_BROWSE,
            SDKProbeType.GCP_SERVICE_ACCOUNT,
            SDKProbeType.GCP_PROJECT,
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

    private static final Set<ProbeCategory> TRACK_STALENESS_PROBE_CATEGORIES =
                    ImmutableSet.of(ProbeCategory.HYPERVISOR,
                                    ProbeCategory.STORAGE_BROWSING,
                                    ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE,
                                    ProbeCategory.STORAGE,
                                    ProbeCategory.PRIVATE_CLOUD,
                                    ProbeCategory.HYPERCONVERGED);
    private static final Map<HealthState, Set<TargetHealthSubCategory>> STALENESS_TARGET_HEALTH_TO_SUBCATEGORY =
            ImmutableMap.of(HealthState.CRITICAL,
                    ImmutableSet.of(TargetHealthSubCategory.VALIDATION,
                            TargetHealthSubCategory.DISCOVERY, TargetHealthSubCategory.DUPLICATION),
                    HealthState.MAJOR, ImmutableSet.of(TargetHealthSubCategory.DELAYED_DATA));


    /**
     * Exports the entity count per target category, target type and entity type from entity store.
     */
    static final DataMetricGauge DISCOVERED_ENTITIES_GAUGE = DataMetricGauge.builder()
        .withName(StringConstants.METRICS_TURBO_PREFIX + "discovered_entities")
        .withHelp("Number of (pre-stitching) entities that were discovered per entity type and " +
            "category and type of probe that discovered them.")
        .withLabelNames("target_category", "target_type", "entity_type")
        .build()
        .register();

    /**
     * Exports the number of targets per target category and target type from the stitching context.
     */
    static final DataMetricGauge TARGET_COUNT_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "targets")
            .withHelp("Number of targets per target category, target type, and target state")
            .withLabelNames("target_category", "target_type", "target_state", "target_state_category")
            .build()
            .register();

    protected static final DataMetricHistogram DUPLICATE_CHECK_TIMES = DataMetricHistogram.builder()
            .withName("tp_target_check_duplicates_time")
            .withHelp("Count of duplicate checks for targets by the time in seconds it took to"
                    + " perform the check. A check is performed on the completion of every full "
                    + "discovery for every target.")
            .withLabelNames("target_type", "duplicate_detected")
            .withBuckets(new double[]{0.01, 0.1, 0.2, 0.5, 1.0, 2.0})
            .build()
            .register();

    public EntityStore(@Nonnull final TargetStore targetStore, @Nonnull final IdentityProvider identityProvider,
            final float duplicateTargetOverlapRatio, final boolean mergeKubernetesTypesForDuplicateDetection,
            @Nonnull final List<EntitiesWithNewStateListener> entitiesWithNewStateListeners, @Nonnull final Clock clock,
            @Nonnull Set<EntityType> reducedEntityTypes,
            boolean useSerializedEntities) {

        this.targetStore = Objects.requireNonNull(targetStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.duplicateTargetDetector = new InternalDuplicateTargetDetector(entityMap, targetStore,
                duplicateTargetOverlapRatio, mergeKubernetesTypesForDuplicateDetection);
        this.entitiesWithNewStateListeners = Objects.requireNonNull(entitiesWithNewStateListeners);
        this.clock = Objects.requireNonNull(clock);
        this.reducedEntityTypes = ImmutableSet.copyOf(reducedEntityTypes);
        this.useSerializedEntities = useSerializedEntities;

        targetStore.addListener(new TargetStoreListener() {
            @Override
            public void onTargetRemoved(@Nonnull final Target target) {
                final long targetId = target.getId();
                logger.info("Deleting target '{}' ({}) and all related entity data.",
                        target.getDisplayName(), targetId);
                EntityStore.this.purgeTarget(targetId, null);
            }
        });
        final Supplier<Set<Long>> currentOidsSupplier = () -> {
            final Stream<Long> oidsFromEntityIdentifyingPropValues = targetEntityIdentifyingPropertyValues.values()
                .stream().map(Map::keySet).flatMap(Collection::stream);
            final Stream<Long> oidsFromEntityMap = entityMap.keySet().stream();
            return Stream.concat(oidsFromEntityMap, oidsFromEntityIdentifyingPropValues)
                .collect(Collectors.toSet());
        };
        this.identityProvider.initializeStaleOidManager(currentOidsSupplier);
    }

    public int expireOids() throws InterruptedException, ExecutionException, TimeoutException {
        return this.identityProvider.expireOids();
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
        return getEntityDTOs(entity.getId())
            // Find the first EntityDTO whose origin matches the provided EntityOrigin
            .filter(entityDTO -> entityOrigin.equals(entityDTO.getOrigin()))
            .findFirst();
    }

    /**
     * For a specific target, return a map of entity ID's discovered
     * by that target and the OID's they map to. If the target is linked to other targets, the entity ID
     * map will be extended to include linked entity IDs.
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

                final SortedSet<Long> linkedTargetIds = targetStore.getLinkedTargetIds(targetId);

                if (linkedTargetIds.isEmpty()) {
                    return Optional.of(targetEntityIdInfo.getLocalIdToEntityId());
                } else {
                    final Map<String, Long> mergeTargetEntityIdMap = new HashMap<>(targetEntityIdInfo.getLocalIdToEntityId());

                    linkedTargetIds.forEach(linkedTargetId -> {
                        final TargetEntityIdInfo linkedEntityIdInfo = targetEntities.get(linkedTargetId);
                        if (linkedEntityIdInfo != null) {
                            linkedEntityIdInfo.getLocalIdToEntityId().forEach(mergeTargetEntityIdMap::putIfAbsent);
                        }
                    });

                    return Optional.of(mergeTargetEntityIdMap);
                }
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Get entity {@code Long} ID based on local ID discovered by a specific target. ID is searched
     * in the maps for all targets.
     *
     * @param localId Local ID.
     * @return {@code Long} ID if it is found in target entities map.
     */
    @Nonnull
    public Optional<Long> getEntityIdByLocalId(final String localId) {
        synchronized (topologyUpdateLock) {
            return targetEntities.values().stream()
                    .map(TargetEntityIdInfo::getLocalIdToEntityId)
                    .map(localIdToEntityId -> localIdToEntityId.get(localId))
                    .filter(Objects::nonNull)
                    .findAny();
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
     * Construct a graph suitable for stitching composed of a forest of disconnected graphs consisting
     * of the entities discovered by each individual target. See {@link TopologyStitchingGraph} for
     * further details.
     *
     * @return A {@link TopologyStitchingGraph} suitable for stitching together the graphs in the discovered
     *         individual-targets into a unified topology.
     */
    @Nonnull
    public StitchingContext constructStitchingContext() {
        return constructStitchingContext(new StalenessInformationProvider() {
            @Override
            public TargetHealth getLastKnownTargetHealth(long targetOid) {
                return DEFAULT_TARGET_HEALTH;
            }
        });
    }

    /**
     * Construct a graph suitable for stitching composed of a forest of disconnected graphs consisting
     * of the entities discovered by each individual target. See {@link TopologyStitchingGraph} for
     * further details.
     * Graph is constructed on the basis of previously supplied through {@link #entitiesDiscovered} entities.
     *
     * @param stalenessProvider provides information about entities that are not up to date
     * @return A {@link TopologyStitchingGraph} suitable for stitching together the graphs in the discovered
     *         individual-targets into a unified topology.
     */
    @Nonnull
    public StitchingContext constructStitchingContext(StalenessInformationProvider stalenessProvider) {
        final StitchingContext.Builder builder = StitchingContext
            .newBuilder(entityMap.size(), targetStore)
            .setIdentityProvider(identityProvider);
        TargetStitchingDataMap stitchingDataMap;

        synchronized (topologyUpdateLock) {
            // only set staleness for on-prem entities
            // TODO this probe type-dependent logic should be temporary until a finer line for staleness is drawn
            final Set<Long> stalenessApplicableTargetIds = targetStore.getAll().stream()
                .map(Target::getId)
                .filter(targetId -> targetStore.getProbeCategoryForTarget(targetId)
                    .filter(Objects::nonNull)
                    .map(category -> TRACK_STALENESS_PROBE_CATEGORIES.contains(category)
                                    || ProbeCategory.isAppOrContainerCategory(category))
                    .orElse(false))
                .collect(Collectors.toSet());

            stitchingDataMap = new TargetStitchingDataMap(targetEntities);
            final Long2ObjectOpenHashMap<TargetCacheEntry> cache = new Long2ObjectOpenHashMap<>();
            // This will populate the stitching data map.
            Map<Long, List<Pair<Long, EntityDTO.Builder>>> deserializedEntityMap = desirializeEntities(this.entityMap);
            try (TracingScope scope = Tracing.trace("populateStitchingDataMap")) {
                for (Entry<Long, List<Pair<Long, EntityDTO.Builder>>> entry : deserializedEntityMap.entrySet()) {
                    final long entityOid = entry.getKey();
                    entry.getValue().stream().map(targetInfoEntry -> {
                        final long targetId = targetInfoEntry.first;
                        final EntityDTO.Builder entityBuilder = targetInfoEntry.second;
                        final TargetCacheEntry cacheEntry = cache.computeIfAbsent(targetId,
                            id -> new TargetCacheEntry(targetId));
                        // apply changes of cached entities from incremental discovery
                        final EntityDTO.Builder entityDTO = cacheEntry.incrementalEntities.map(
                                incrementalEntities -> applyIncrementalChanges(
                                    entityBuilder, entityOid, targetId,
                                    incrementalEntities))
                            .orElse(entityBuilder);
                        return StitchingEntityData.newBuilder(entityDTO)
                            .oid(entityOid)
                            .targetId(targetId)
                            .lastUpdatedTime(cacheEntry.lastUpdatedTime)
                            .supportsConnectedTo(cacheEntry.supportsConnectedTo)
                            .setStale(isDiscoveredEntityStale(stalenessProvider, targetId, entityDTO, stalenessApplicableTargetIds))
                            .build();
                    }).forEach(stitchingDataMap::put);
                }
            }
        }

        try (TracingScope scope = Tracing.trace("addEntitiesToContext")) {

            stitchingDataMap.getTargetIds().forEach(targetId -> {

                final Map<String, StitchingEntityData> targetStitchingDataMap = stitchingDataMap.getTargetIdToStitchingDataMap(
                        targetId, targetStore.getLinkedTargetIds(targetId));

                stitchingDataMap.getStitchingDataForTarget(targetId)
                        .forEach(stitchingEntityData -> {
                            try {
                                builder.addEntity(stitchingEntityData, targetStitchingDataMap);
                            } catch (IllegalArgumentException | NullPointerException e) {
                                // We want to make sure we don't block the whole broadcast if one entity
                                // encounters an error.
                                logger.error("Failed to add entity " +
                                        stitchingEntityData + " to stitching context due to error.", e);
                            }
                        });

            });
        }

        final LogMessageGrouper msgGrouper = LogMessageGrouper.getInstance();
        final List<String> logMessages = msgGrouper.getMessages(TopologyStitchingGraph.LOGMESSAGEGROUPER_SESSION_ID);
        logMessages.forEach(logger::warn);
        msgGrouper.clear(TopologyStitchingGraph.LOGMESSAGEGROUPER_SESSION_ID);

        // pass whether entity details are supported to the stitching context.
        builder.setEntityDetailsEnabled(entityDetailsEnabled);
        final Map<Long, Map<String, Long>> targetEntityOidByLocalId = targetEntityIdentifyingPropertyValues.entrySet()
                .stream()
                    .collect(Collectors.toMap(
                        Entry::getKey,
                        entry -> {
                            final Map<String, Long> result = new HashMap<>();
                            entry.getValue().forEach((oid, idProps) -> result.put(idProps.getEntityId(), oid));
                            return result;
                        }
                    ));
        builder.setTargetEntityLocalIdToOid(targetEntityOidByLocalId);
        return builder.build();
    }

    public void setEntityDetailsEnabled(boolean entityDetailsEnabled) {
        this.entityDetailsEnabled = entityDetailsEnabled;
    }

    private static boolean isDiscoveredEntityStale(StalenessInformationProvider stalenessProvider,
                    long targetId, EntityDTO.Builder entityDTO, Set<Long> stalenessApplicableTargetIds) {
        if (!FeatureFlags.DELAYED_DATA_HANDLING.isEnabled()) {
            return false;
        }
        /**
         * For now, staleness is defined at target level but exposed to stitching at entity level.
         * Currently only on_prem entities are expected to have staleness flag ever set
         * (this is subject to change later when we define staleness at finer granularity).
         */
        if (!stalenessApplicableTargetIds.contains(targetId)) {
            return false;
        }
        TargetHealth lastKnownTargetHealth =
                        stalenessProvider.getLastKnownTargetHealth(targetId);
        return lastKnownTargetHealth != null &&
                STALENESS_TARGET_HEALTH_TO_SUBCATEGORY.getOrDefault(
                        lastKnownTargetHealth.getHealthState(), Collections.emptySet()).contains(
                        lastKnownTargetHealth.getSubcategory());
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
     * @param incrementalEntities Incremental entities for the target that discovered the entityDTO
     * @return the updated EntityDTO builder
     */
    @GuardedBy("topologyUpdateLock")
    private EntityDTO.Builder applyIncrementalChanges(@Nonnull EntityDTO.Builder entityDTO,
                                                      long entityOid, long targetId,
                                                      @Nonnull final TargetIncrementalEntities incrementalEntities) {
        final List<BiConsumer<EntityDTO.Builder, EntityDTO>> updateFunctions =
            INCREMENTAL_ENTITY_UPDATE_FUNCTIONS.get(entityDTO.getEntityType().getNumber());
        if (updateFunctions == null) {
            // no update functions
            logger.debug("No incremental update functions provided for entity type {}",
                entityDTO.getEntityType());
            return entityDTO;
        }

        incrementalEntities.applyEntitiesInDiscoveryOrder(entityOid,
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

    private Map<Long, List<Pair<Long, EntityDTO.Builder>>> desirializeEntities(Map<Long, Entity> entityMap) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try (TracingScope scope = Tracing.trace("desirializeEntities")) {
            Map<Long, List<Pair<Long, EntityDTO.Builder>>> deserializedEntityMap = new ConcurrentHashMap<>();
            entityMap.values().parallelStream().forEach(entity -> {

                final Optional<EntityDTO.Builder> consolidatedBuilder = entity.consolidatedEntity()
                        .map(EntityDTO::toBuilder);

                entity.getTargets().forEach(target -> {
                    if (entity.getEntityInfo(target).isPresent()) {
                        try {
                            List<Pair<Long, EntityDTO.Builder>> entities = deserializedEntityMap.computeIfAbsent(entity.getId(), k -> new ArrayList<>());
                            //We need to protect against concurrent insertions to the inner List datastructures
                            // in the  deserializedEntityMap otherwise we may in rare circumstances wind up
                            // with a ConcurrentModificationException.
                            synchronized (entities) {

                                final EntityDTO.Builder entityBuilder;
                                if (consolidatedBuilder.isPresent()) {
                                    entityBuilder = consolidatedBuilder.get();
                                } else {
                                    entityBuilder = entity.getEntityInfo(target).get().getEntityInfoBuilder();
                                }
                                entities.add(new Pair<>(target, entityBuilder));
                            }
                        } catch (InvalidProtocolBufferException e) {
                            logger.error(entity.getConversionError());
                        }
                    }
                });
            });
            logger.info("Decompressed {} entities in {} ", deserializedEntityMap.size(), stopwatch);
            return deserializedEntityMap;
        }
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
     * @throws IdentityServiceException If during id assignment, those assignments cannot be
     *                                   persisted.
     */
    @Nonnull
    private Map<Long, EntityDTO> assignIdsToEntities(final long probeId,
                                                     final long targetId,
                                                     @Nonnull final List<EntityDTO> entityDTOList)
        throws IdentityServiceException {
        // There may be duplicate entries (though that's a bug in the probes),
        // and we should deal with that without throwing exceptions.
        final Map<Long, EntityDTO> finalEntitiesById = new HashMap<>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Long, EntityDTO> oidToEntity = identityProvider.getIdsForEntities(probeId, entityDTOList);
        oidToEntity.forEach((entityId, entityDto) -> {
            final EntityDTO existingEntry = finalEntitiesById.putIfAbsent(entityId, entityDto);
            if (existingEntry != null) {
                logger.error("Entity with ID {} and local ID {} appears "
                                + "more than once in discovered entities for target {}! The DTOs are {}.\n"
                                + "Entity 1: {}\n Entity 2: {}\n",
                    entityId, existingEntry.getId(), targetId,
                    existingEntry.equals(entityDto) ? "equal" : "not equal",
                    existingEntry, entityDto);
            }
        });
        logger.info("Assigned {} oids for target {} in {}", oidToEntity.size(), targetId,
                stopwatch);
        return finalEntitiesById;
    }

    private DuplicateTargetException createDuplicateTargetException(long duplicateTargetId,
            Set<Long> conflictingTargetIds) {
        final String dupeTarget = targetStore.getTarget(duplicateTargetId).map(Target::toString)
                .orElse(Long.toString(duplicateTargetId));
        final Set<String> conflictingTargets = conflictingTargetIds.stream().map(
                targetid -> targetStore.getTarget(targetid)
                        .map(Target::toString)
                        .orElse(Long.toString(targetid))).collect(Collectors.toSet());
        return new DuplicateTargetException(dupeTarget, conflictingTargets);
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
            throws TargetNotFoundException, DuplicateTargetException {
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

            if (duplicateTargetDetector.isEnabled()) {
                // Check if this target is a duplicate of a target that was added before it
                long startTime = System.currentTimeMillis();
                Set<Long> originalTargets = duplicateTargetDetector.scanForDuplicateTargets(target.get(), entitiesById);
                logger.debug(
                        "Time to calculate duplicate target set for target {} is {} milliseconds.",
                        () -> target.get(), () -> System.currentTimeMillis() - startTime);
                DUPLICATE_CHECK_TIMES.labels(duplicateTargetDetector.adjustProbeType(
                        target.get().getProbeInfo().getProbeType()),
                        Boolean.toString(!originalTargets.isEmpty())).observe(
                        ((double)System.currentTimeMillis() - startTime)
                                / TimeUnit.SECONDS.toMillis(1L));
                if (!originalTargets.isEmpty()) {
                    logger.debug("Returned duplicate target set {}", () -> originalTargets);
                    // if there are multiple duplicate targets, we will keep processing only the
                    // target that had the earliest created OID. For other targets, we purge them
                    // and throw a duplicate target exception.
                    if (originalTargets.stream().mapToLong(IdentityGenerator::toMilliTime).min().orElse(IdentityGenerator.toMilliTime(targetId))
                            < IdentityGenerator.toMilliTime(targetId)) {
                        purgeTarget(targetId, null);
                        throw createDuplicateTargetException(targetId, originalTargets);
                    }
                }
            }

            final Map<EntityType, Collection<Entity>> deletedEntities = new HashMap<>();
            purgeTarget(targetId,
                    // If the entity is not present in the incoming snapshot, then remove it.
                    (entity) -> {
                        final boolean result = !entitiesById.containsKey(entity.getId());
                        if (result) {
                            deletedEntities.computeIfAbsent(entity.getEntityType(),
                                    t -> new HashSet<>()).add(entity);
                        }
                        return result;
                    });
            entitiesLogging(targetId, deletedEntities, "Deleted");

            final ImmutableSet.Builder<Long> newTargetEntitiesBuilder =
                    new ImmutableSet.Builder<>();
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

            final Map<EntityType, Collection<Entity>> addedEntities = new HashMap<>();
            // Assemble the new list of entities associated with this target.
            entitiesById.entrySet().forEach(entry -> {
                Entity entity = entityMap.get(entry.getKey());
                if (entity == null) {
                    entity = new Entity(entry.getKey(), entry.getValue().getEntityType(), reducedEntityTypes, useSerializedEntities);
                    addedEntities.computeIfAbsent(entity.getEntityType(), t -> new HashSet<>()).add(
                            entity);
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
                    logger.error("Duplicate local ID {} for entities {} and {}", localId,
                            entityMap.get(existingEntityLocalId), entityMap.get(entry.getKey()));
                } else {
                    newEntitiesByLocalId.put(entry.getValue().getId(), entity.getId());
                }

                localIdToType.put(entry.getValue().getId(), entry.getValue().getEntityType());

                if (entry.getValue().getEntityType() == EntityType.VIRTUAL_MACHINE) {
                    vmToProviderLocalIds.put(entry.getKey(), entry.getValue()
                            .getCommoditiesBoughtList()
                            .stream()
                            .map(CommodityBought::getProviderId)
                            .collect(Collectors.toList()));
                }
                if (entry.getValue().getEntityType() == EntityType.CONTAINER) {
                    containerToProviderLocalIds.put(entry.getKey(), entry.getValue()
                            .getCommoditiesBoughtList()
                            .stream()
                            .map(CommodityBought::getProviderId)
                            .collect(Collectors.toList()));
                }
            });
            entitiesLogging(targetId, addedEntities, "Added");

            final TargetEntityIdInfo targetIdInfo = new TargetEntityIdInfo(
                    newTargetEntitiesBuilder.build(), newEntitiesByLocalId, clock.millis());
            targetEntities.put(targetId, targetIdInfo);

            // clear all incremental responses which are triggered before current successful full discovery
            getIncrementalEntities(targetId).ifPresent(
                    incrementalEntities -> incrementalEntities.clearEntitiesDiscoveredBefore(
                            messageId));

            // Fill in the hosted-by relationships.
            vmToProviderLocalIds.forEach((entityId, localIds) -> localIds.stream()
                    .filter(localId -> localIdToType.get(localId) == EntityType.PHYSICAL_MACHINE)
                    .findFirst()
                    .ifPresent(localId -> {
                        // If this is null then the probe's entity information is invalid,
                        // since the VM is buying commodities from a PM that doesn't exist.
                        long pmId = Objects.requireNonNull(
                                targetIdInfo.getLocalIdToEntityId().get(localId));
                        Objects.requireNonNull(entityMap.get(entityId)).setHostedBy(targetId, pmId);
                    }));
            containerToProviderLocalIds.forEach((entityId, localIds) -> localIds.stream()
                    .filter(localId -> localIdToType.get(localId) == EntityType.CONTAINER_POD
                            || localIdToType.get(localId) == EntityType.VIRTUAL_MACHINE)
                    .findFirst()
                    .ifPresent(localId -> {
                        // If this is null then the probe's entity information is invalid,
                        // since the Container is buying commodities from a Pod or VM that doesn't exist.
                        long pmId = Objects.requireNonNull(
                                targetIdInfo.getLocalIdToEntityId().get(localId));
                        Objects.requireNonNull(entityMap.get(entityId)).setHostedBy(targetId, pmId);
                    }));
        }
    }

    private void entitiesLogging(long targetId, Map<EntityType, Collection<Entity>> entities,
            String logPrefix) {
        if (logger.isDebugEnabled()) {
            for (Entry<EntityType, Collection<Entity>> entry : entities.entrySet()) {
                Collection<Entity> entitiesList = entry.getValue();
                if (logger.isTraceEnabled()) {
                    StringJoiner joiner = new StringJoiner(System.lineSeparator());
                    for (Entity entity : entitiesList) {
                        Optional<PerTargetInfo> entityInfo = entity.getEntityInfo(targetId);
                        if (entityInfo.isPresent()) {
                            String displayName = "missing display name";
                            try {
                                displayName = entityInfo.get().getEntityInfo().getDisplayName();
                            } catch (InvalidProtocolBufferException e) {
                                logger.error("Could not get dto from entity with id {}", entity.getId(), e);
                            }
                            joiner.add(displayName);
                        }
                    }
                    logger.trace("{} {} {} entities from target {} : {}", logPrefix, entities.size(), entry.getKey(),
                            targetId, joiner.toString());
                } else {
                    logger.debug("{} {} {} entities from target {}", logPrefix, entities.size(), entry.getKey(),
                            targetId);
                }
            }
        }
    }

    /**
     * Process entityIdentifyingPropertyValues by assigning OIDs to them and storing them to the repository.
     *
     * @param probeId of the probe by which the entityIdentifyingPropertyValues are discovered.
     * @param targetId of the target from which the entityIdentifyingPropertyValues are discovered.
     * @param entityIdentifyingPropertyValues that are being processed.
     * @throws IdentityServiceException If error encountered while assigning OIDs.
     */
    public void entityIdentifyingPropertyValuesDiscovered(final long probeId, final long targetId,
                                                          @Nonnull final List<EntityIdentifyingPropertyValues>
                                                              entityIdentifyingPropertyValues)
        throws IdentityServiceException {
        final Map<Long, EntityIdentifyingPropertyValues> assignedIds = identityProvider
            .getIdsFromIdentifyingPropertiesValues(probeId, Objects.requireNonNull(entityIdentifyingPropertyValues));
        targetEntityIdentifyingPropertyValues.compute(targetId, (key, existingIdentifyingPropValues) -> {
            if (existingIdentifyingPropValues == null) {
                return new HashMap<>(assignedIds);
            } else {
                existingIdentifyingPropValues.putAll(assignedIds);
                return existingIdentifyingPropValues;
            }
        });
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
     * @throws IdentityServiceException If error occurred assigning OIDs
     * @throws TargetNotFoundException if no target exists with the provided targetId
     */
    public void entitiesDiscovered(final long probeId, final long targetId, final int messageId,
        @Nonnull DiscoveryType discoveryType, @Nonnull final List<EntityDTO> entityDTOList)
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {
        // this applies to all discovery types, since there may be new entities in incremental response
        final Map<Long, EntityDTO> entitiesById =
            assignIdsToEntities(probeId, targetId, entityDTOList);

        if (discoveryType == DiscoveryType.FULL) {
            setFullDiscoveryEntities(targetId, messageId, entitiesById);
        } else if (discoveryType == DiscoveryType.INCREMENTAL) {
            Optional<Target> target = targetStore.getTarget(targetId);
            if (target.isPresent() && duplicateTargetDetector.isEnabled()) {
                synchronized (topologyUpdateLock) {
                    // find duplicate targets that are older than this target
                    final long targetCreationTime = IdentityGenerator.toMilliTime(targetId);
                    Set<Long> originalTargets = duplicateTargetDetector.getKnownDuplicateTargets(
                            targetId).stream()
                            .filter(targId -> IdentityGenerator.toMilliTime(targId)
                                    < targetCreationTime)
                            .collect(Collectors.toSet());
                    if (!originalTargets.isEmpty()) {
                        throw createDuplicateTargetException(targetId, originalTargets);
                    }
                }
            }
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
                TopologyEntityDTO.Builder topologyEntityDTO = TopologyEntityDTO.newBuilder()
                    .setOid(entityOid)
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setEntityState(entityState(entityDTO));

                // Send automation level
                getEntityDTOs(entityOid)
                    .filter(EntityDTO::hasPhysicalMachineData).map(EntityDTO::getPhysicalMachineData)
                    .filter(PhysicalMachineData::hasAutomationLevel).map(PhysicalMachineData::getAutomationLevel)
                    .findFirst().ifPresent(automationLevel ->
                        topologyEntityDTO.setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                            PhysicalMachineInfo.newBuilder().setAutomationLevel(automationLevel))));

                entitiesWithNewStateBuilder.addTopologyEntity(topologyEntityDTO);
            }
        }
        if (entitiesWithNewStateBuilder.getTopologyEntityCount() > 0) {
            entitiesWithNewStateBuilder.setStateChangeId(identityProvider.generateTopologyId());
            EntitiesWithNewState entitiesWithNewStateMessage = entitiesWithNewStateBuilder.build();
            logger.trace("Sending entity state change message {}", entitiesWithNewStateMessage);
            entitiesWithNewStateListeners.forEach(listener -> {
                try {
                    listener.onEntitiesWithNewState(entitiesWithNewStateMessage);
                } catch (CommunicationException | InterruptedException e) {
                    logger.error("Problem occurred while sending entity state change message {}", e);
                }
            });
        }
    }

    private Stream<EntityDTO> getEntityDTOs(long oid) {
        Optional<Entity> entity = getEntity(oid);
        Collection<EntityDTO> dtos = new ArrayList<>();
        if (entity.isPresent()) {
            for (PerTargetInfo targetInfo : entity.get().allTargetInfo()) {
                try {
                    dtos.add(targetInfo.getEntityInfo());
                } catch (InvalidProtocolBufferException e) {
                    logger.error(entity.get().getConversionError());
                }
            }
        }
        return dtos.stream();
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
                                 Map<Long, EntityDTO> restoredMap) throws TargetNotFoundException,
            DuplicateTargetException {
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
                    messageIdToEntityDtos.computeIfAbsent(messageId, ArrayList::new).add(messageIdToEntity.get(messageId));
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
                    Optional<PerTargetInfo> perTargetInfoOptional = entity.get().getTargetInfo(targetId);
                    if (perTargetInfoOptional.isPresent()) {
                        PerTargetInfo perTargetInfo = perTargetInfoOptional.get();
                        try {
                            map.put(entityOid, perTargetInfo.getEntityInfo());
                        } catch (InvalidProtocolBufferException e) {
                            logger.error(entity.get().getConversionError());
                        }
                    }
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
        private final Long2ObjectOpenHashMap<Map<String, StitchingEntityData>> targetDataMap;

        /**
         * Create a new TargetStitchingDataMap given the original mapping of targets to their data.
         *
         * @param sourceMap the original mapping of targets to their discovered data.
         */
        public TargetStitchingDataMap(@Nonnull final Map<Long, TargetEntityIdInfo> sourceMap) {
            targetDataMap = new Long2ObjectOpenHashMap<>(sourceMap.size());
            sourceMap.entrySet().forEach(entry ->
                targetDataMap.put(entry.getKey().longValue(),
                    new HashMap<>(entry.getValue().getDiscoveredEntitiesCount())));
        }

        public void put(@Nonnull final StitchingEntityData entityData) {
            // get or create the target's entity local id -> stitching entity map
            Map<String,StitchingEntityData> stitchingDataByLocalId
                    = targetDataMap.computeIfAbsent(entityData.getTargetId(), k -> new HashMap<>());
            stitchingDataByLocalId.put(entityData.getEntityDtoBuilder().getId(), entityData);
        }

        @Nonnull
        public Map<String, StitchingEntityData> getTargetIdToStitchingDataMap(final long targetId,
                                                                              final SortedSet<Long> linkedTargetIds) {
            if (linkedTargetIds.isEmpty()) {
                return targetDataMap.get(targetId);
            } else {

                logger.info("Linking target ID {} to the following targets:\n{}", () -> targetId, () -> Joiner.on(",").join(linkedTargetIds));

                final Map<String, StitchingEntityData> mergedTargetDataMap = new HashMap<>(targetDataMap.get(targetId));

                linkedTargetIds.forEach(linkedTargetId ->
                        // cast to long to avoid using deprecated getOrDefault() method.
                        targetDataMap.getOrDefault((long)linkedTargetId, Collections.emptyMap())
                                .forEach(mergedTargetDataMap::putIfAbsent));


                return mergedTargetDataMap;
            }
        }

        public Set<Long> getTargetIds() {
            return targetDataMap.keySet();
        }

        public Collection<StitchingEntityData> getStitchingDataForTarget(long targetId) {
            return targetDataMap.getOrDefault(targetId, Collections.emptyMap()).values();
        }
    }

    /**
     * Push entity and target related information to topology-processor metrics endpoint.
     */
    public void sendMetricsEntityAndTargetData() {
        TARGET_COUNT_GAUGE.getLabeledMetrics().forEach((key, val) -> val.setData(0.0));
        Map<Long, TargetHealth> targetHealth =
                targetHealthRetriever.getTargetHealth(Collections.emptySet(), true);
        targetStore.getAll().forEach((target) -> {
            Pair<String, String> targetHealthLabels =
                    getUserFacingTargetHealthState(targetHealth.get(target.getId()));
            TARGET_COUNT_GAUGE.labels(
                    getUserFacingCategoryString(target.getProbeInfo().getUiProbeCategory()),
                    target.getProbeInfo().getProbeType(),
                    targetHealthLabels.first, targetHealthLabels.second)
                .increment();
        });

        DISCOVERED_ENTITIES_GAUGE.getLabeledMetrics().forEach((key, val) -> val.setData(0.0));
        synchronized (topologyUpdateLock) {
            targetEntities.forEach((targetId, targetEntityIdInfo) ->
                targetStore.getTarget(targetId).ifPresent(target -> {
                    final String targetCategory = getUserFacingCategoryString(target.getProbeInfo().getUiProbeCategory());
                    final String targetType = target.getProbeInfo().getProbeType();
                    targetEntityIdInfo.getEntityIds().forEach(entityId -> {
                        final Entity entity = entityMap.get(entityId);
                        if (entity != null) {
                            final String entityType = entity.getEntityType().toString();
                            DISCOVERED_ENTITIES_GAUGE.labels(targetCategory, targetType, entityType).increment();
                        }
                    });
                }));
        }
    }

    /**
     * Return a string representing the health of a target.
     *
     * @param targetHealth  TargetHealth entry
     * @return return a pair of strings representing the health of the target. The first element is
     *      the health state and the second element is the health state category.
     *
     *      Return "Unknown" for values that are not available.
     */
    private Pair<String, String> getUserFacingTargetHealthState(@Nullable TargetHealth targetHealth) {
        String targetState = StringConstants.UNKNOWN;
        String targetStateCategory = StringConstants.UNKNOWN;
        if (targetHealth != null) {
            targetState = targetHealth.getHealthState().toString();
            if (targetHealth.hasSubcategory()) {
                targetStateCategory = targetHealth.getSubcategory().toString();
            }
        }
        return new Pair<>(targetState, targetStateCategory);
    }

    /**
     * Small helper cache when building StitchingContext.
     */
    private class TargetCacheEntry {
        public final boolean supportsConnectedTo;
        public final long lastUpdatedTime;
        public final Optional<TargetIncrementalEntities> incrementalEntities;

        private TargetCacheEntry(final long targetId) {
            this.supportsConnectedTo = supportsConnectedTo(targetId);
            this.lastUpdatedTime = getTargetLastUpdatedTime(targetId).orElse(0L);
            this.incrementalEntities = getIncrementalEntities(targetId);
        }
    }

    /**
     * Detect duplicate targets according to the following heuristic:
     * For each probe category that we want to test for duplication, we choose an entity type.
     * For each target in one of these probe categories, we check if any other targets of the
     * same probe type overlap in the given entity type by more than some configurable percentage.
     * The default percentage is 30%.
     */
    private static class InternalDuplicateTargetDetector {

        private final Map<Long, Entity> entityMap;

        private final TargetStore targetStore;

        private final float overlapRatio;

        private final boolean mergeKubernetesTypes;

        /**
         * Set of sets where each set within globalDuplicateTargetSet represents an equivalence
         * set of targets that are all duplicates of each other.
         */
        private final Set<Set<Long>> globalDuplicateTargetSet = new HashSet<>();

        /**
         * Map of probe categories for which we will try to detect duplicate targets to the EntityType
         * to count for determining that two targets are the same. If two targets of the same category
         * and type overlap by more than some configurable percentage in this entity type, we
         * consider them duplicates and will suppress the newer target and notify the user of the
         * duplicate target.
         */
        private final Map<String, EntityType> PROBE_CATEGORY_ENTITY_TYPE_MAP =
                ImmutableMap.of(ProbeCategory.HYPERVISOR.getCategoryInUpperCase(),
                        EntityType.VIRTUAL_MACHINE,
                        ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase(),
                        EntityType.VIRTUAL_MACHINE,
                        ProbeCategory.CLOUD_NATIVE.getCategoryInUpperCase(), EntityType.NAMESPACE,
                        ProbeCategory.BILLING.getCategoryInUpperCase(), EntityType.BUSINESS_ACCOUNT
                );

        private static final String KUBERNETES = "Kubernetes";

        /**
         * Construct a InternalDuplicateTargetDetector.
         *
         * @param entityMap map of entity OID to Entity for all the discovered targets
         * @param targetStore the store of all the targets
         * @param overlapRatio minimum overlap ration between targets to consider
         * them duplicates
         * @param mergeKubernetesTypes flag indicating whether or not to consider all Kubernetes
         * probes to be the same type even if they have different suffixes
         */
        public InternalDuplicateTargetDetector(@Nonnull Map<Long, Entity> entityMap,
                @Nonnull TargetStore targetStore, float overlapRatio,
                boolean mergeKubernetesTypes) {
            this.overlapRatio = overlapRatio;
            this.mergeKubernetesTypes = mergeKubernetesTypes;
            this.entityMap = entityMap;
            this.targetStore = targetStore;
            targetStore.addListener(new TargetStoreListener() {
                @Override
                public void onTargetRemoved(@Nonnull Target target) {
                    targetRemoved(target.getId());
                }
            });
        }

        /**
         * If mergeKuberenetesTypes is true, convert any String that starts with "Kubernetes"
         * to "Kubernetes". Otherwise, just return the String that is passed in. The purpose
         * of this is to treat all the Kubernetes types as the same.
         *
         * @param probeType the type of the probe.
         * @return the converted probeType according to whether we are merging Kubernetes types
         * or not.
         */
        @Nonnull
        public String adjustProbeType(@Nonnull String probeType) {
            if (mergeKubernetesTypes && probeType.startsWith(KUBERNETES)) {
                return KUBERNETES;
            } else {
                return probeType;
            }
        }

        /**
         * Indicate whether or not duplicate targets are being detected.
         *
         * @return True if duplicate target detection is happening; false otherwise.
         */
        public boolean isEnabled() {
            return overlapRatio > 0.0F;
        }

        private LongSet getTargetIdsForProbeType(@Nonnull String probeType) {
            final String adjustedProbeType = adjustProbeType(probeType);
            return targetStore.getAll().stream()
                    .filter(target -> adjustedProbeType.equals(
                            adjustProbeType(target.getProbeInfo().getProbeType())))
                    .mapToLong(Target::getId)
                    .collect(LongArraySet::new, LongArraySet::add, LongArraySet::addAll);
        }

        /**
         * Return the set of targetIds already detected to be a duplicate of the target
         * represented by this targetId. The targetId itself will not be in the set. If
         * there are no duplicates of this target, return the empty set.
         *
         * @param targetId the OID of the target in question.
         * @return Set of target OIDs of targets that are duplicates of the target in
         * question or the empty set if there are no duplicates.
         */
        public Set<Long> getKnownDuplicateTargets(long targetId) {
            synchronized (globalDuplicateTargetSet) {
                final Set<Long> duplicates = globalDuplicateTargetSet.stream()
                        .filter(set -> set.contains(targetId))
                        .findFirst()
                        .orElse(Collections.emptySet());
                return duplicates.stream().filter(targId -> targId != targetId)
                        .collect(Collectors.toSet());
            }
        }

        /**
         * Look for a duplicate target of the given target and return a set of OIDs
         * of targets that this target is a duplicate of. Note that the target
         * itself will not be in the returned set. So if targets with OIDs A and B are
         * duplicates, for example, getDuplicateTargetOids(A) returns a set
         * containing B and getDuplicateTargetOids(B) returns a set containing A.
         *
         * @param target the target to check.
         * @param entitiesById a list of entities discovered by the target.
         * @return Set of targetIds of targets that are considered duplicates of this one.
         */
        @GuardedBy("topologyUpdateLock")
        public Set<Long> scanForDuplicateTargets(@Nonnull Target target,
                @Nonnull Map<Long, EntityDTO> entitiesById) {
            // For incremental discoveries, entitiesById will be empty.
            // Just look up the duplicate set in the map and return it.
            if (overlapRatio <= 0.0F) {
                return Collections.emptySet();
            }
            final long targetId = target.getId();
            final Set<Long> duplicateTargets = new HashSet<>();
            final String probeType = target.getProbeInfo().getProbeType();
            final LongSet targetsToCompareAgainst = getTargetIdsForProbeType(probeType);
            targetsToCompareAgainst.remove(target.getId());
            final EntityType entityType = PROBE_CATEGORY_ENTITY_TYPE_MAP
                    .get(target.getProbeInfo().getProbeCategory().toUpperCase());
            if (entityType != null) {
                final LongSet entitiesToCheck = entitiesById.entrySet()
                        .stream()
                        .filter(entry -> entry.getValue().getEntityType()
                                == entityType)
                        .mapToLong(Entry::getKey)
                        .collect(LongArraySet::new, LongArraySet::add, LongArraySet::addAll);
                // keep track of count of overlapping key entities by target id
                final Long2IntMap overlappingEntityCountByTargetId = new Long2IntArrayMap();
                LongIterator iterator = entitiesToCheck.iterator();
                while (iterator.hasNext()) {
                    final long nextOid = iterator.nextLong();
                    Optional.ofNullable(entityMap.get(nextOid)).ifPresent(nextEntity ->
                        nextEntity.getTargets().stream()
                                .mapToLong(Long::longValue)
                                .filter(targetsToCompareAgainst::contains)
                                .forEach(overlappingTargetId -> {
                                    overlappingEntityCountByTargetId.compute(overlappingTargetId,
                                            (id, currVal) -> currVal == null ? 1 : currVal + 1);
                                })
                    );
                }
                final int sizeOfOverlapForDuplicate = (int) Math.ceil(entitiesToCheck.size()
                        * overlapRatio);
                overlappingEntityCountByTargetId.long2IntEntrySet().stream().forEach(entry -> {
                    if (entry.getIntValue() >= sizeOfOverlapForDuplicate) {
                        duplicateTargets.add(entry.getLongKey());
                    }
                });
            }

            synchronized (globalDuplicateTargetSet) {
                final Optional<Set<Long>> existingDuplicates = globalDuplicateTargetSet.stream()
                        .filter(nxtSet -> !Sets.intersection(nxtSet, duplicateTargets).isEmpty())
                        .findFirst();
                if (!duplicateTargets.isEmpty()) {
                    duplicateTargets.add(targetId);
                    if (existingDuplicates.isPresent()) {
                        existingDuplicates.get().addAll(duplicateTargets);
                        duplicateTargets.addAll(existingDuplicates.get());
                    } else {
                        globalDuplicateTargetSet.add(duplicateTargets);
                    }
                } else {
                    existingDuplicates.ifPresent(set -> {
                        set.remove(targetId);
                        if (set.isEmpty() || set.size() == 1) {
                            globalDuplicateTargetSet.remove(set);
                        }
                    });
                }
            }
            // Return a set giving all target IDs for targets that are duplicates of the target.
            return duplicateTargets.stream()
                    .filter(targId -> targId != targetId)
                    .collect(Collectors.toSet());
        }

        /**
         * Process a target deletion by updating the globalDuplicateTargetMap.
         *
         * @param targetId the id of the deleted target.
         */
        private void targetRemoved(long targetId) {
            synchronized (globalDuplicateTargetSet) {
                final Iterator<Set<Long>> iter = globalDuplicateTargetSet.iterator();
                while (iter.hasNext()) {
                    final Set<Long> nextSet = iter.next();
                    if (nextSet.remove(targetId)) {
                        if (nextSet.isEmpty() || nextSet.size() == 1) {
                            iter.remove();
                        }
                    }
                }
            }
        }
    }

    public void setTargetHealthRetriever(final TargetHealthRetriever targetHealthRetriever) {
        this.targetHealthRetriever = targetHealthRetriever;
    }
}
