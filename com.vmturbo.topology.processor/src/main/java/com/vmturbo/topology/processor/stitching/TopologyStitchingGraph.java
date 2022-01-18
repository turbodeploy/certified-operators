package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.logmessagegrouper.LogMessageGrouper;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.CopyActionEligibility;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

/**
 * A mutable graph built from the forest of partial topologies discovered by individual targets.
 * The graph should be a mutable DAG (though the DAG-ness is not enforced by this class).
 *
 * The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same provider results in only a single edge between the two in the graph.
 *
 * The EntityDTO.Builder objects within the graph's {@link TopologyStitchingEntity}s may be edited,
 * but edits to buying/selling relationships on the DTOs are NOT automatically reflected in the
 * relationships available on the graph itself.
 *
 * After the graph is initially constructed, it may be mutated in specifically allowed ways.
 *
 * Mutations permitted to the graph:
 * 1. Removing an entity - Removing an entity from the graph propagates a change to all buyers of commodities
 *                         from the entity being removed.
 * 2. Commodities bought - Commodities bought by an entity may be added or removed. These changes will
 *                         automatically be propagated to the sellers of the commodities.
 * 3. The creation of new entities - Adding a new entity to the graph during the pre-stitching stage
 *
 * Mutations NOT permitted to the graph:
 * 1. Commodities sold - No destructive mutations are permitted to commodities sold (that is, changes
 *                       that would change or remove relationships to buyers of the commodities being changed).
 *                       If a use case for this arises, we may consider supporting it in the future.
 *
 * Note that the graph itself only contains methods for constructing the graph. Mutate the graph and
 * the entities within the graph via the appropriate classes in {@link TopologyStitchingChanges}. See
 * also {@link StitchingResultBuilder} which contains methods for constructing and queueing
 * change objects during the stitching processing phase.
 *
 * The graph retains references to its constituent {@link TopologyStitchingEntity}s via a map that uses
 * object-references to the builders to the {@link EntityDTO} objects that describe the entities associated
 * with each individual {@link TopologyStitchingEntity}.
 */
@NotThreadSafe
public class TopologyStitchingGraph {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A map permitting lookup from the original entity builder to its corresponding stitching entity.
     */
    private final Map<EntityDTO.Builder, TopologyStitchingEntity> stitchingEntities;

    /**
     * Session ID for LogMessageGrouper so that we can group certain log messages together to
     * avoid log pollution.
     */
    public static final String LOGMESSAGEGROUPER_SESSION_ID = "TopologyStitchingGraph";

    /**
     * logType for log message when provider of a bought commodity is missing from the topology.
     */
    private static final String PROVIDER_MISSING = "ProviderMissing";

    /**
     * Create a new stitchingGraph.
     *
     * Note that stitchingGraph construction does not validate the consistency of the input - for example,
     * if an entity in the input is buying from an entity not in the input, no error will be
     * generated.
     */
    public TopologyStitchingGraph(int expectedSize) {
        stitchingEntities = new IdentityHashMap<>(expectedSize);
    }

    /**
     * Get a stream of all {@link TopologyStitchingEntity}s in the stitchingGraph.
     *
     * @return A stream of all {@link TopologyStitchingEntity}s in the stitchingGraph.
     */
    public Stream<TopologyStitchingEntity> entities() {
        return stitchingEntities.values().stream();
    }

    /**
     * Retrieve the {@link TopologyStitchingEntity} for an entity in the stitchingGraph by its
     * associated entity builder. Returns {@link Optional#empty()} if the no such {@link TopologyStitchingEntity}
     * is in the stitchingGraph.
     *
     * @param entityBuilder The builder for the entity of the {@link TopologyStitchingEntity} in the stitchingGraph.
     * @return The {@link TopologyStitchingEntity} associated with the corresponding entityBuilder.
     */
    public Optional<TopologyStitchingEntity> getEntity(EntityDTO.Builder entityBuilder) {
        return Optional.ofNullable(stitchingEntities.get(entityBuilder));
    }

    /**
     * Get the number of entities in the stitchingGraph.
     *
     * @return The number of entities in the stitchingGraph.
     */
    public int entityCount() {
        return stitchingEntities.size();
    }

    /**
     * Look up the target ID for a given {@link EntityDTO} in the graph.
     *
     * @param entityBuilder The builder whose target ID should be looked up.
     * @return The targetID of the target that originally discovered by the builder.
     *         Returns {@link Optional#empty()} if the builder is not known to the graph.
     */
    public Optional<Long> getTargetId(@Nonnull final EntityDTO.Builder entityBuilder) {
        return getEntity(entityBuilder)
            .map(TopologyStitchingEntity::getTargetId);
    }

    /**
     * Add a {@link TopologyStitchingEntity} corresponding to the input {@link StitchingEntityData}.
     * Adds consumes edges in the stitchingGraph for all entities the input is consuming from.
     * Adds produces edges in the stitchingGraph for all entities providing commodities this entity is consuming.
     *
     * Clients should never attempt to add a {@link TopologyStitchingEntity} for an entity already in the
     * stitchingGraph. This may not be checked.
     *
     * Note that this method uses {@link LogMessageGrouper} to consolidate log messages,
     * so callers must make sure to dump the accumulated log messages and clear the messages
     * when they are done.
     *
     * @param entityData The entity to add a {@link TopologyStitchingEntity} for.
     * @param entityMap The map of localId -> {@link StitchingEntityData} for the target that discovered
     *                  this entity.
     */
    public TopologyStitchingEntity addStitchingData(@Nonnull final StitchingEntityData entityData,
                                 @Nonnull final Map<String, StitchingEntityData> entityMap) {
        final TopologyStitchingEntity entity = getOrCreateStitchingEntity(entityData);
        final EntityDTO.Builder entityDtoBuilder = entityData.getEntityDtoBuilder();

        final Map<StitchingErrorCode, MutableInt> errorsByCategory = new HashMap<>();

        final LogMessageGrouper msgGrouper = LogMessageGrouper.getInstance();
        msgGrouper.register(LOGMESSAGEGROUPER_SESSION_ID, PROVIDER_MISSING,
                "The following entities are buying commodities from non-existent "
                        + "providers. Turn on debug logging for more details. ");


        if (!entityDtoBuilder.getCommoditiesBoughtList().isEmpty()) {
            final List<Integer> invalidCommBought = new ArrayList<>();
            Iterator<CommodityBought.Builder> iterator;
            int i;

            for (iterator = entityDtoBuilder.getCommoditiesBoughtBuilderList().iterator(), i = 0;
                iterator.hasNext(); i++) {
                final CommodityBought.Builder commodityBought = iterator.next();
                final String providerId = commodityBought.getProviderId();
                final StitchingEntityData providerData = entityMap.get(providerId);
                if (providerData == null) {
                    // This error happens often when unstitched proxies are deleted after stitching.
                    // Log it at debug level and create one consolidated message to log at error
                    // level summarizing the issue, just in case it is a real problem.
                    logger.debug("Entity {} (local id: {}) buying commodities {} from non-existent "
                                    + "provider {}", entityData.getOid(), entityData.getLocalId(),
                            commodityBought.getBoughtList(), providerId);
                    msgGrouper.log(LOGMESSAGEGROUPER_SESSION_ID, PROVIDER_MISSING,
                            String.valueOf(entityData.getOid()));
                    errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_BOUGHT,
                        k -> new MutableInt(0)).increment();
                    invalidCommBought.add(i);
                    continue;
                }

                final TopologyStitchingEntity provider = getOrCreateStitchingEntity(providerData);
                if (!provider.getLocalId().equals(providerId)) {
                    // The only way this would happen is if there is already a different entity
                    // in the stitching graph.
                    // Serious error, and the IDs will be helpful to diagnose where it came
                    // from.
                    logger.error("Entity {} (local id: {}) - Map key {} does not match provider localId value {}",
                        entityData.getOid(), entityData.getLocalId(), providerId, provider.getLocalId());
                    logger.debug("Provider entity: {}",
                        entityData.getEntityDtoBuilder(), provider.getEntityBuilder());
                    errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                        k -> new MutableInt(0)).increment();
                    errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_BOUGHT,
                        k -> new MutableInt(0)).increment();
                    invalidCommBought.add(i);
                    continue;
                }

                // add commodity bought for this provider, there may be multiple set of commodity
                // bought from same provider
                CommoditiesBought bought = new CommoditiesBought(
                        commodityBought.getBoughtList().stream()
                                .map(CommodityDTO::toBuilder)
                                .collect(Collectors.toList()));
                // Pass on the action eligibility settings that are provided in the
                // CommodityBought section of the SDK's EntityDTO
                CopyActionEligibility.transferActionEligibilitySettingsFromEntityDTO(
                                                                    commodityBought, bought);

                entity.addProviderCommodityBought(provider, bought);
                provider.addConsumer(entity);
            }

            // Remove in reverse order so that all indices continue to be valid.
            // i.e. if we want to remove index 1 and 3, removing index 1 first will mean index
            // 3 needs to be re-interpreted as index 2. Reverse order also reduces shifting.
            for (final Integer invalidIdx : Lists.reverse(invalidCommBought)) {
                entityDtoBuilder.removeCommoditiesBought(invalidIdx);
            }
        }

        if (!entityDtoBuilder.getCommoditiesSoldList().isEmpty()) {
            final List<Integer> invalidCommSold = new ArrayList<>();
            Iterator<CommodityDTO.Builder> iterator;
            int i;

            for (iterator = entityDtoBuilder.getCommoditiesSoldBuilderList().iterator(), i = 0;
                iterator.hasNext(); ++i) {
                final CommodityDTO.Builder commoditySold = iterator.next();

                final Optional<String> accessingIdOpt =
                    SdkToTopologyEntityConverter.parseAccessKey(commoditySold);
                final TopologyStitchingEntity accessingEntity;
                if (accessingIdOpt.isPresent()) {
                    final String accessingLocalId = accessingIdOpt.get();
                    final StitchingEntityData accessingData = entityMap.get(accessingLocalId);
                    if (accessingData == null) {
                        // Serious error, and the IDs will be helpful to diagnose where it came
                        // from.
                        logger.error("Entity {} (local id: {}) accessed by entity {} which does not exist.",
                            entity.getOid(), entityData.getLocalId(), accessingLocalId);
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_SOLD,
                            k -> new MutableInt(0)).increment();
                        invalidCommSold.add(i);
                        continue;
                    }

                    accessingEntity = getOrCreateStitchingEntity(accessingData);
                    if (!accessingEntity.getLocalId().equals(accessingLocalId)) {
                        // Serious error, and the IDs will be helpful to diagnose where it came
                        // from.
                        logger.error("Entity {} (local id: {}) - Map key {} does not " +
                            "match accessing localId value {}", entityData.getOid(),
                            entityData.getLocalId(), accessingLocalId, accessingEntity.getLocalId());
                        logger.debug("Accessing entity: {}",
                            entityData.getEntityDtoBuilder(), accessingEntity.getEntityBuilder());
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_SOLD,
                            k -> new MutableInt(0)).increment();
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                            k -> new MutableInt(0)).increment();
                        invalidCommSold.add(i);
                        continue;
                    }
                } else {
                    accessingEntity = null;
                }

                entity.getTopologyCommoditiesSold()
                    .add(new CommoditySold(commoditySold, accessingEntity));

            }

            for (final Integer invalidIdx : Lists.reverse(invalidCommSold)) {
                entityDtoBuilder.removeCommoditiesSold(invalidIdx);
            }
        }

        // add connected entities
        addConnections(entityData, entityMap, entity, entityDtoBuilder, errorsByCategory);

        if (entityData.supportsDeprecatedConnectedTo()) {
            // A deprecated mode for expressing entity connections overloaded other relationships
            // such as "LayeredOver" and "ConsistsOf". Deprecated in favor of the SDK ConnectedEntity
            // but we need to continue to support this mode until all probes have been converted
            // over.
            addDeprecatedConnections(entityData, entityMap, entity, entityDtoBuilder, errorsByCategory);
        }

        // Log and record the high-level error summary.
        if (!errorsByCategory.isEmpty()) {
            logger.debug("Invalid entity added to stitching graph " +
                "(see above for specific errors): {}", entityData.getEntityDtoBuilder());
            errorsByCategory.forEach((category, numErrors) -> {
                entity.recordError(category);
                Metrics.ERROR_COUNT.labels(category.name().toLowerCase()).increment(numErrors.doubleValue());
            });
        }

        return entity;
    }

    private void addConnections(
        final @Nonnull StitchingEntityData entityData,
        final @Nonnull Map<String, StitchingEntityData> entityMap,
        final TopologyStitchingEntity entity,
        final Builder entityDtoBuilder,
        final Map<StitchingErrorCode, MutableInt> errorsByCategory) {

        // Map of connections we've already processed. Used for detecting duplicates.
        final Map<ConnectedEntity.ConnectionType, Set<String>> processedConnections = new HashMap<>();
        // Duplicates detected.
        final Map<ConnectedEntity.ConnectionType, MutableInt> duplicateCounts = new HashMap<>();

        for (ConnectedEntity connection : entityDtoBuilder.getConnectedEntitiesList()) {
            final Set<String> processed = processedConnections.computeIfAbsent(
                connection.getConnectionType(), type -> new HashSet<>());

            // Check if we have already processed an identical connection
            if (processed.contains(connection.getConnectedEntityId())) {
                duplicateCounts.computeIfAbsent(connection.getConnectionType(),
                    k -> new MutableInt()).increment();

            } else {
                // Look up and validate the connection.
                final TopologyStitchingEntity connectedEntity = lookupAndValidateConnectedEntity(
                    entityData, entityMap, entity, errorsByCategory, connection);

                // Add the connection.
                if (connectedEntity != null) {
                    final ConnectionType xlConnectionType = xlConnectionTypeFor(connection.getConnectionType());
                        entity.addConnectedTo(xlConnectionType, connectedEntity);
                        connectedEntity.addConnectedFrom(xlConnectionType, entity);
                }
            }
        }

        if (!duplicateCounts.isEmpty()) {
            // Log eliminated duplicates by category.
            logger.info("Entity {} (local id: {}) Removed duplicate connections {}.",
                entity.getOid(), entity.getLocalId(), duplicateCounts);
        }
    }

    // Cloud probes generally use "layeredOver" to represent aggregation,
    // and "consistsOf" to represent ownership. This method creates the
    // appropriate connections. Deprecated in favor of having the probes create
    // these connections themselves. Once all probes are properly creating
    // the new relationships, remove this functionality.
    private void addDeprecatedConnections(
        final @Nonnull StitchingEntityData entityData,
        final @Nonnull Map<String, StitchingEntityData> entityMap,
        final TopologyStitchingEntity entity,
        final Builder entityDtoBuilder,
        final Map<StitchingErrorCode, MutableInt> errorsByCategory) {

        // translate layeredOver relations to aggregations
        if (!entityDtoBuilder.getLayeredOverList().isEmpty()) {

            final List<String> invalidLayeredOver = new ArrayList<>();
            final List<String> validLayeredOver = new ArrayList<>(entityDtoBuilder.getLayeredOverCount());
            int duplicateLayeredOverCount = 0;

            for (final String layeredOverId : entityDtoBuilder.getLayeredOverList()) {
                final TopologyStitchingEntity layeredOverEntity = getLayeredOverData(
                    layeredOverId, entityData, entityMap, entity, errorsByCategory);
                if (layeredOverEntity == null) {
                    // We were unable to retrieve a valid entity for the layeredOverId. Report the error.
                    invalidLayeredOver.add(layeredOverId);
                } else {
                    // translate layeredOver relations to aggregation/normal connections.
                    if (translateLayeredOver(entity, layeredOverEntity)) {
                        validLayeredOver.add(layeredOverId);
                    } else {
                        // The same layeredOverId is repeated more than once. Report the duplicates
                        // as an error.
                        duplicateLayeredOverCount++;
                    }
                }

                // check and report on validity of layeredOver relations and
                // update the entityDTOBuilder accordingly
                if (invalidLayeredOver.size() > 0) {
                    logger.error("Entity {} (local id: {}) layered over invalid entities: {}", entity::getOid, entity::getLocalId, () -> invalidLayeredOver);
                    entityDtoBuilder.clearLayeredOver();
                    entityDtoBuilder.addAllLayeredOver(validLayeredOver);
                } else if (duplicateLayeredOverCount > 0) {
                    // Remove duplicates, so downstream uses of entityDtoBuilder don't have to
                    // deal with this error.
                    logger.info("Entity {} (local id: {}) Removing {} duplicate entries from layer over list.", entity.getOid(), entity.getLocalId(), duplicateLayeredOverCount);
                    entityDtoBuilder.clearLayeredOver();
                    entityDtoBuilder.addAllLayeredOver(validLayeredOver);
                }
            }
        }

        // translate consistsOf relations to ownerships
        if (!entityDtoBuilder.getConsistsOfList().isEmpty()) {
            final List<String> invalidConsistsOf = new ArrayList<>();
            final List<String> validConsistsOf = new ArrayList<>(entityDtoBuilder.getConsistsOfCount());
            int duplicateConsistsOfCount = 0;

            for (String consistsOfId : entityDtoBuilder.getConsistsOfList()) {
                final TopologyStitchingEntity consistsOfEntity = getConsistsOfData(
                    entityData, entityMap, entity, errorsByCategory, consistsOfId);

                if (consistsOfEntity == null) {
                    // We were unable to retrieve a valid entity for the consistsOfId. Report the error.
                    invalidConsistsOf.add(consistsOfId);
                } else {
                    // translate consistsOf relations to owns connections.
                    final boolean added = entity.addConnectedTo(ConnectionType.OWNS_CONNECTION, consistsOfEntity)
                        && consistsOfEntity.addConnectedFrom(ConnectionType.OWNS_CONNECTION, entity);

                    if (added) {
                        validConsistsOf.add(consistsOfId);
                    } else {
                        // The same consistsOf is repeated more than once. Report the duplicates as an error.
                        duplicateConsistsOfCount++;
                    }
                }

                // check and report on validity of consistsOf relations and
                // update the entityDTOBuilder accordingly
                if (invalidConsistsOf.size() > 0) {
                    logger.error("Entity {} (local id: {}) consists of invalid entities: {}", entity::getOid, entity::getLocalId, () -> invalidConsistsOf);
                    entityDtoBuilder.clearConsistsOf();
                    entityDtoBuilder.addAllConsistsOf(validConsistsOf);
                } else if (duplicateConsistsOfCount > 0) {
                    // Remove duplicates, so downstream uses of entityDtoBuilder don't have to
                    // deal with this error.
                    logger.info("Entity {} (local id: {}) Removing {} duplicate entries from consists of list.", entity.getOid(), entity.getLocalId(), duplicateConsistsOfCount);
                    entityDtoBuilder.clearConsistsOf();
                    entityDtoBuilder.addAllConsistsOf(validConsistsOf);
                }
            }
        }
    }

    private boolean translateLayeredOver(@Nonnull TopologyStitchingEntity entity,
                                      @Nonnull TopologyStitchingEntity layeredOverEntity) {
        // in general, layeredOver should be translated as aggregation, but there are special cases
        if (mustBeNormalConnection(entity.getEntityType(), layeredOverEntity.getEntityType())) {
            return entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, layeredOverEntity) &&
                    layeredOverEntity.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, entity);
        } else {
            return  layeredOverEntity.addConnectedFrom(ConnectionType.AGGREGATED_BY_CONNECTION, entity) &&
                    entity.addConnectedTo(ConnectionType.AGGREGATED_BY_CONNECTION, layeredOverEntity);
        }
    }

    /**
     * Decides if a relationship between two entities connected by "layeredOver"
     * must be a "normal" one or an aggregation.
     *
     * <p>The rules are:
     * <ul>
     *     <li>Relationships between tiers should be normal</li>
     *     <li>Relationships from VMs to volumes should be normal</li>
     *     <li>Relationships from volumes to storage or storage tier
     *         should be normal</li>
     * </ul>
     * Everything else should be translated into an aggregation.
     * </p>
     *
     * @param currentEntityType type of one of the entities
     * @param layeredOverEntityType type of the layered-over entity
     * @return true iff this should be translated to a normal connection
     */
    private boolean mustBeNormalConnection(@Nonnull EntityType currentEntityType,
                                           @Nonnull EntityType layeredOverEntityType) {
        return (TopologyDTOUtil.isTierEntityType(currentEntityType.getNumber())
                        && TopologyDTOUtil.isTierEntityType(layeredOverEntityType.getNumber()))
                    || (layeredOverEntityType == EntityType.VIRTUAL_VOLUME
                            && currentEntityType == EntityType.VIRTUAL_MACHINE)
                    || (currentEntityType == EntityType.VIRTUAL_VOLUME &&
                            isStorageType(layeredOverEntityType));
    }

    private boolean isStorageType(@Nonnull EntityType entityType) {
        return entityType == EntityType.STORAGE || entityType == EntityType.STORAGE_TIER;
    }

    @Nullable
    private TopologyStitchingEntity lookupAndValidateConnectedEntity(
        final @Nonnull StitchingEntityData entityData,
        final @Nonnull Map<String, StitchingEntityData> entityMap,
        final @Nonnull TopologyStitchingEntity entity,
        final @Nonnull Map<StitchingErrorCode, MutableInt> errorsByCategory,
        final @Nonnull ConnectedEntity connection) {
        final StitchingEntityData connectedEntityData = entityMap.get(connection.getConnectedEntityId());
        if (connectedEntityData == null) {
            // The final list of invalid entities gets printed at error-level
            // below, so this can be at debug.
            logger.debug("Entity {} (local id: {}) - connection {} "
                + " does not exist.", entity.getOid(), entity.getLocalId(), connection);
            errorsByCategory.computeIfAbsent(stitchingErrorCodeFor(connection.getConnectionType()),
                k -> new MutableInt(0)).increment();
            return null;
        }

        final TopologyStitchingEntity connectedEntity = getOrCreateStitchingEntity(connectedEntityData);
        if (!connectedEntity.getLocalId().equals(connection.getConnectedEntityId())) {
            // The final list of invalid entities gets printed at error-level
            // below, so this can be at debug.
            logger.debug("Entity {} (local id: {}) - Map key {} does not "
                    + "match connection localId value {}", entityData.getOid(),
                entityData.getLocalId(), connection.getConnectedEntityId(),
                connectedEntity.getLocalId());
            errorsByCategory.computeIfAbsent(stitchingErrorCodeFor(connection.getConnectionType()),
                k -> new MutableInt()).increment();
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                k -> new MutableInt()).increment();
            return null;
        }
        return connectedEntity;
    }

    static StitchingErrorCode stitchingErrorCodeFor(final @Nonnull ConnectedEntity.ConnectionType type) {
        switch (type) {
            case NORMAL_CONNECTION:
                return StitchingErrorCode.INVALID_NORMAL_CONNECTION;
            case OWNS_CONNECTION:
                return StitchingErrorCode.INVALID_OWNS_CONNECTION;
            case AGGREGATED_BY_CONNECTION:
                return StitchingErrorCode.INVALID_AGGREGATED_BY_CONNECTION;
            case CONTROLLED_BY_CONNECTION:
                return StitchingErrorCode.INVALID_CONTROLLED_BY_CONNECTION;
            default:
                logger.error("Unknown connection type " + type);
                return StitchingErrorCode.UNKNOWN;
        }
    }

    static ConnectionType xlConnectionTypeFor(final @Nonnull ConnectedEntity.ConnectionType type) {
        switch (type) {
            case NORMAL_CONNECTION:
                return ConnectionType.NORMAL_CONNECTION;
            case OWNS_CONNECTION:
                return ConnectionType.OWNS_CONNECTION;
            case AGGREGATED_BY_CONNECTION:
                return ConnectionType.AGGREGATED_BY_CONNECTION;
            case CONTROLLED_BY_CONNECTION:
                return ConnectionType.CONTROLLED_BY_CONNECTION;
            default:
                logger.error("Unknown connection type " + type);
                return ConnectionType.forNumber(type.getNumber());
        }
    }

    private TopologyStitchingEntity getConsistsOfData(
            final @Nonnull StitchingEntityData entityData,
            final @Nonnull Map<String, StitchingEntityData> entityMap,
            final TopologyStitchingEntity entity,
            final Map<StitchingErrorCode, MutableInt> errorsByCategory,
            final String consistsOfId) {
        final StitchingEntityData consistsOfData = entityMap.get(consistsOfId);
        if (consistsOfData == null) {
            // The final list of invalid entities gets printed at error-level
            // below, so this can be at debug.
            logger.debug("Entity {} (local id: {}) - consists of entity {}" +
                    " which does not exist.", entity.getOid(), entity.getLocalId(),
                consistsOfId);
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_CONSISTS_OF,
                k -> new MutableInt(0)).increment();
            return null;
        }

        final TopologyStitchingEntity consistsOfEntity = getOrCreateStitchingEntity(consistsOfData);
        if (!consistsOfEntity.getLocalId().equals(consistsOfId)) {
            // The final list of invalid entities gets printed at error-level
            // below, so this can be at debug.
            logger.debug("Entity {} (local id: {}) - Map key {} does not " +
                    "match consists-of localId value {}", entityData.getOid(),
                entityData.getLocalId(), consistsOfId, consistsOfEntity.getLocalId());
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_CONSISTS_OF,
                k -> new MutableInt(0)).increment();
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                k -> new MutableInt(0)).increment();
            return null;
        }
        return consistsOfEntity;
    }

    @Nullable
    private TopologyStitchingEntity getLayeredOverData(
            final @Nonnull String layeredOverId,
            final @Nonnull StitchingEntityData entityData,
            final @Nonnull Map<String, StitchingEntityData> entityMap,
            final TopologyStitchingEntity entity,
            final Map<StitchingErrorCode, MutableInt> errorsByCategory) {
        final StitchingEntityData layeredOverData = entityMap.get(layeredOverId);
        if (layeredOverData == null) {
            // The final list of invalid entities gets printed at error-level
            // below, so this can be at debug.
            logger.debug("Entity {} (local id: {}) - layered over entity {}" +
                " which does not exist.", entity.getOid(), entity.getLocalId(),
                layeredOverId);
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_LAYERED_OVER,
                k -> new MutableInt(0)).increment();
            return null;
        }
        final TopologyStitchingEntity layeredOverEntity = getOrCreateStitchingEntity(layeredOverData);
        if (!layeredOverEntity.getLocalId().equals(layeredOverId)) {
            // The final list of invalid entities gets printed at error-level
            // below, so this can be at debug.
            logger.debug("Entity {} (local id: {}) - Map key {} does not " +
                    "match layered over localId value {}", entityData.getOid(),
                entityData.getLocalId(), layeredOverId, layeredOverEntity.getLocalId());
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_LAYERED_OVER,
                k -> new MutableInt(0)).increment();
            errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                k -> new MutableInt(0)).increment();
            return null;
        }
        return layeredOverEntity;
    }

    /**
     * Remove a {@link TopologyStitchingEntity} from the graph.
     *
     * For each {@link TopologyStitchingEntity} in the graph that has a provider or consumer relation to
     * the {@link TopologyStitchingEntity} being removed, those {@link TopologyStitchingEntity}s are updated
     * by deleting their relationships to the {@link TopologyStitchingEntity} that is being removed.
     *
     * Entities known to be buying from the removed {@link TopologyStitchingEntity} have their commodities updated
     * so that the commodities they used to buy from the {@link TopologyStitchingEntity} are also removed.
     *
     * Attempting to remove an entity not in the graph is treated as a no-op.
     *
     * @param toRemove The entity to remove.
     * @return The set of localIds for all entities affected by the removal. The localId of the removed
     *         entity is always in this set unless no entity was actually removed.
     */
    public Set<TopologyStitchingEntity> removeEntity(@Nonnull final TopologyStitchingEntity toRemove) {
        final Set<TopologyStitchingEntity> affected = new HashSet<>();

        final TopologyStitchingEntity removedEntity = stitchingEntities.remove(toRemove.getEntityBuilder());
        if (removedEntity != null) {
            Preconditions.checkArgument(toRemove == removedEntity); // Must be equal by reference.
            affected.add(toRemove);

            // Fix up relationships on providers and consumers, recording every entity affected by the change.
            toRemove.getTopologyProviders().forEach(provider -> {
                provider.removeConsumer(toRemove);
                affected.add(provider);
            });
            toRemove.getTopologyConsumers().forEach(consumer -> {
                consumer.removeProvider(toRemove);
                affected.add(consumer);
            });

            toRemove.clearConsumers();
            toRemove.clearProviders();

            // remove connected to and connected from relationship from the other side
            toRemove.getConnectedToByType().forEach((connectionType, entities) -> entities.forEach(
                    connectedTo -> {
                        connectedTo.removeConnectedFrom(toRemove, connectionType);
                        affected.add((TopologyStitchingEntity)connectedTo);
                    }));
            toRemove.clearConnectedTo();

            toRemove.getConnectedFromByType().forEach((connectionType, entities) -> entities.forEach(
                    connectedFrom -> {
                        connectedFrom.removeConnectedTo(toRemove, connectionType);
                        affected.add((TopologyStitchingEntity)connectedFrom);
                    }));
            toRemove.clearConnectedFrom();
        }

        return affected;
    }

    /**
     * Get the {@link TopologyStitchingEntity} corresponding to an entity from the stitchingGraph, or if
     * it does not exist, create one and insert it into the stitchingGraph.
     *
     * @param entityData The entity whose corresponding {@link TopologyStitchingEntity} should be looked up
     *                   or created.
     * @return The retrieved or newly created {@link TopologyStitchingEntity} for the entity.
     */
    public TopologyStitchingEntity getOrCreateStitchingEntity(@Nonnull final StitchingEntityData entityData) {
        return stitchingEntities.computeIfAbsent(entityData.getEntityDtoBuilder(),
            builder -> new TopologyStitchingEntity(entityData));
    }

    private static class Metrics {

        private static final DataMetricCounter ERROR_COUNT = DataMetricCounter.builder()
            .withName("tp_stitching_graph_error_count")
            .withHelp("The number of errors when constructing the stitching graph.")
            .withLabelNames("type")
            .build()
            .register();
    }
}
