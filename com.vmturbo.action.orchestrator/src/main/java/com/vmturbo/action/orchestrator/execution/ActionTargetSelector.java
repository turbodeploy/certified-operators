package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Select the target/probe to execute an action against
 *
 * Uses the ActionTargetResolver to break ties when multiple targets are eligible for action execution
 */
public class ActionTargetSelector {

    private static final Logger logger = LogManager.getLogger();

    /**
     * For resolving conflicts in action execution, when the target entity has been
     * discovered by multiple targets
     */
    private final ActionTargetResolver targetResolver;

    /**
     * Chooses an entity to execute an action against
     */
    private final ActionExecutionEntitySelector entitySelector;

    /**
     * A client for making remote calls to the Topology Processor service to retrieve entity data
     */
    private final EntityServiceGrpc.EntityServiceBlockingStub entityServiceBlockingStub;

    /**
     * A client for making remote calls to the Repository service
     */
    private final RepositoryClient repositoryClient;

    /**
     * The context ID for the realtime market. Used when making remote calls to the repository service.
     */
    private final long realtimeTopologyContextId;

    /**
     * Create an ActionTargetSelector
     *
     * @param targetResolver to resolve conflicts when there are multiple targets which can
     *      execute the action
     * @param entitySelector to select a service entity to execute an action against
     * @param repositoryClient a client to the repository service, to retrieve entity type information
     * @param realtimeTopologyContextId the context ID of the realtime market
     */
    public ActionTargetSelector(@Nonnull final ActionTargetResolver targetResolver,
                                @Nonnull final ActionExecutionEntitySelector entitySelector,
                                @Nonnull final Channel topologyProcessorChannel,
                                @Nonnull final RepositoryClient repositoryClient,
                                final long realtimeTopologyContextId) {
        this.targetResolver = Objects.requireNonNull(targetResolver);
        this.entitySelector = Objects.requireNonNull(entitySelector);
        this.entityServiceBlockingStub = EntityServiceGrpc.newBlockingStub(
                Objects.requireNonNull(topologyProcessorChannel));
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Returns targetId for each provided action. If action is not supported or if support for an
     * action cannot be determined, it will be omitted from the result map.
     *
     * @param actions actions to determine targetIds of these.
     * @return provided actions and determined targetIds
     */
    @Nonnull
    public Map<Action, Long> getTargetIdsForActions(@Nonnull Collection<Action> actions) {
        return performBulkActionLookup(actions, this::getTargetId);
    }

    /**
     * Returns probeId for each provided action. If action is not supported or if support for an
     * action cannot be determined, it will be omitted from the result map.
     *
     * @param actions actions to determine probeIds of these.
     * @return provided actions and determined probeIds
     */
    @Nonnull
    public Map<Action, Long> getProbeIdsForActions(@Nonnull Collection<Action> actions) {
        return performBulkActionLookup(actions, this::getProbeIdForAction);
    }

    /**
     * Returns an id (probe or target, depending on the lookupFunction chosen) for each provided action.
     * If action is not supported or if support for an action cannot be determined, it will be
     * omitted from the result map.
     * //TODO: provide a flag to cause a failure to be reported for the action, rather than just skipping it
     *
     * @param actions actions to perform a bulk lookupFunction on
     * @return provided actions and determined Ids (either target or probe)
     */
    @Nonnull
    private Map<Action, Long> performBulkActionLookup(@Nonnull Collection<Action> actions,
                                         @Nonnull LookupIdRelatedToActionFunction lookupFunction) {
        // Get a set of all entities being acted upon
        final Set<Long> primaryEntities = getPrimaryEntities(actions);

        // Short-circuit if no entities could be retrieved from actions (or actions is empty)
        // This is necessary because requesting entity data for an empty list throws an Exception.
        if (primaryEntities.isEmpty()) {
            return Collections.emptyMap();
        }

        // Get the type of each entity (needed to find special cases in target entity selection)
        // For performance reasons, this remote call is done in bulk
        // The Map is entityId -> EntityType
        final Map<Long, EntityDTO.EntityType> entityTypes = getEntityTypeMap(primaryEntities);

        // Get a map of actions to the entity that will be used to execute the action. This may
        // differ from the primary entity if a special case applies. Generally, there is a large
        // overlap between the set of primary entities and the set of selected entities
        final Map<Action, Long> actionsToSelectedEntityIds = new HashMap<>();
        for (Action action : actions) {
            try {
                ActionDTO.Action actionDto = action.getRecommendation();
                EntityType primaryEntityType = entityTypes.get(getPrimaryEntityId(actionDto));
                // Select the entity to execute the action against
                long selectedEntity = getExecutantEntityId(actionDto, primaryEntityType);
                // Put the result in the map
                actionsToSelectedEntityIds.put(action, selectedEntity);
            } catch (EntitiesResolutionException | UnsupportedActionException e) {
                // If there was a problem determining the execution target for this action, log the
                // exception and skip this action.
                logger.warn("An entity could not be selected for executing this action. actionId: "
                        + action.getId());
                logger.debug(e.getMessage(), e);
            }
        }

        // Get info about the targets and probes related to the entities
        // For performance reasons, this remote call is done in bulk
        // The Map is entityId -> EntityInfo
        final Map<Long, EntityInfo> entityInfos =
                getEntitiesInfo(new TreeSet<>(actionsToSelectedEntityIds.values()));

        // The resultMap is Action -> id, where id is either a targetId or a probeId, depending on
        // the provided function
        Map<Action, Long> resultMap = new HashMap<>(actions.size());
        for (Entry<Action, Long> actionToSelectedEntityIdEntry : actionsToSelectedEntityIds.entrySet()) {
            try {
                final Long selectedEntityId = actionToSelectedEntityIdEntry.getValue();
                final Action action = actionToSelectedEntityIdEntry.getKey();
                final EntityInfo selectedEntity = entityInfos.get(selectedEntityId);
                final ActionDTO.Action actionDto = action.getRecommendation();
                // Get an id (either a targetId or a probeId, depending on the provided function)
                final long id = lookupFunction.lookupRelatedId(actionDto, selectedEntity);
                resultMap.put(action, id);
            } catch (TargetResolutionException e) {
                // If there was a problem determining the execution target for this action, log the
                // exception and skip this action.
                logger.warn("A target could not be found for this action. actionId: "
                        + actionToSelectedEntityIdEntry.getKey().getId());
                logger.debug(e.getMessage(), e);
            }
        }
        return resultMap;
    }

    /**
     * An interface for defining functions that can be performed on actions in bulk, to populate a
     * result map that holds actions and (some kind of) id.
     */
    @FunctionalInterface
    interface LookupIdRelatedToActionFunction {
        long lookupRelatedId(ActionDTO.Action action, EntityInfo selectedEntity)
                throws TargetResolutionException;
    }

    /**
     * Get the probe id, given an action and a selected entity for action exectuion
     *
     * @param action the action to be executed
     * @param selectedEntity the entity which will be used to execute the action
     * @return the probe id
     * @throws TargetResolutionException if no target found for this action suitable for execution
     */
    private long getProbeIdForAction(final ActionDTO.Action action, final EntityInfo selectedEntity)
            throws TargetResolutionException {
        // Select the target to execute the action against (based on the selected entity)
        long selectedTarget = getTargetId(action, selectedEntity);
        // A target was found for the selected entity; add it to the action-probe map
        final long probeId = selectedEntity.getTargetIdToProbeIdMap().get(selectedTarget);
        return probeId;
    }

    /**
     * Get a set of all entities being acted upon
     *
     * @param actions the actions for which to get the primary entities
     * @return a set of all entities being acted upon
     */
    private Set<Long> getPrimaryEntities(@Nonnull final Collection<Action> actions) {
        return actions.stream()
                .map(Action::getRecommendation)
                .map(actionDTO -> getPrimaryEntityId(actionDTO))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    /**
     *  Get the type of each entity (needed to find special cases in target entity selection)
     *  For performance reasons, this remote call is done in bulk
     *  The Map is entityId -> EntityType
     *
     * @param entities the entities for which to retrieve the type
     * @return a map of entityId -> EntityType
     */
    private Map<Long, EntityType> getEntityTypeMap(final Set<Long> entities) {
        return retrieveTopologyEntities(entities.stream().collect(Collectors.toList()))
                .stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid,
                        entityDTO -> EntityType
                                .forNumber(entityDTO.getEntityType())));
    }

    /**
     * Get the entity to use for the {@link ExecutableStep} of an {@link Action}.
     * While this defaults to the primary entity of the action--the entity being acted upon--there
     * exist special cases where a different, related entity needs to be used in order to execute
     * an action.
     *
     * @param action TopologyProcessor Action
     * @param entityType the type of the primary entity that this action is acting upon
     * @param entityInfos a Map of entityId -> EntityInfo. The purpose of this map is to provide
     *      target/probe data. This data is pre-fetched when dealing with multiple actions for
     *      perfomance reasons, as it involves a remote call.
     * @return EntityInfo for the selected entity
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     */
    private EntityInfo getExecutantEntity(@Nonnull ActionDTO.Action action,
                             @Nonnull EntityDTO.EntityType entityType,
                             @Nonnull Map<Long, EntityInfo> entityInfos)
            throws EntitiesResolutionException, UnsupportedActionException {
        long selectedEntityId = getExecutantEntityId(action, entityType);
        // Retrieve the discovered entity data from the pre-fetched map if possible, otherwise fetch
        // from the Topology Processor.
        EntityInfo entityInfo = entityInfos.containsKey(selectedEntityId) ?
                entityInfos.get(selectedEntityId) :
                getEntityInfo(selectedEntityId);
        return entityInfo;
    }

    /**
     * Get the entity ID to use for the {@link ExecutableStep} of an {@link Action}.
     * While this defaults to the primary entity of the action--the entity being acted upon--there
     * exist special cases where a different, related entity needs to be used in order to execute
     * an action.
     *
     * @param action TopologyProcessor Action
     * @param entityType the type of the primary entity that this action is acting upon
     * @return entityId for the selected entity
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     */
    private long getExecutantEntityId(@Nonnull ActionDTO.Action action,
                                          @Nonnull EntityDTO.EntityType entityType)
            throws EntitiesResolutionException, UnsupportedActionException {
        // Check for special cases using the entitySelector
        return entitySelector.getEntityId(action, entityType)
                .orElseThrow(() -> new EntitiesResolutionException(
                        "No entity could be found for this action: " + action.toString()));

    }

    /**
     * Get the ID of the probe or target for the {@link ExecutableStep} of an {@link Action}.
     * This involves making remote calls, and should not be used for a large number of actions--use
     * performBulkActionLookup instead.
     *
     * @param action TopologyProcessor Action
     * @return targetId for the action
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     * @throws UnsupportedActionException if action is not supported by XL
     * @throws TargetResolutionException if no target found for this action suitable for execution
     */
    public long getTargetId(@Nonnull ActionDTO.Action action)
            throws EntitiesResolutionException, UnsupportedActionException,
            TargetResolutionException {
        // Get the entity type associated with this action
        EntityDTO.EntityType entityType = getEntityTypeForAction(action);
        // Determine the entity to execute this action against
        // (without providing any pre-fetched entity data, thus the empty map)
        EntityInfo executantEntity = getExecutantEntity(action, entityType, Collections.emptyMap());
        // Determine the targetId
        return getTargetId(action, executantEntity);
    }

    /**
     * Get the ID of the probe or target for the {@link ExecutableStep} of an {@link Action}.
     *
     * @param action TopologyProcessor Action
     * @param executantEntity an EntityInfo that provides target/probe data which will be used to
     *                        execute this action.
     * @return targetId for the action
     * @throws TargetResolutionException if no target found for this action suitable for execution
     */
    @VisibleForTesting
    long getTargetId(@Nonnull ActionDTO.Action action,
                             @Nonnull EntityInfo executantEntity)
            throws TargetResolutionException {
        // Based on the selected entity and its associated entity info, find the target to
        // execute this action against.
        final Optional<Long> targetIdOptional = getExecutantTargetForEntity(action, executantEntity);
        final long targetId = targetIdOptional
                .orElseThrow(() -> new TargetResolutionException((
                        "Action: " +
                        action.getId() +
                        " has no associated target that supports execution of the action.")));
        return targetId;
    }

    /**
     * Helper method for getting the primary entity (object of) an action
     *
     * @param action the action whose primary entity is to be determined
     * @return the id of the primary entity (object of) the action, or Optional.empty if the action
     *      is unsupported
     */
    private static Optional<Long> getPrimaryEntityId(@Nonnull final ActionDTO.Action action) {
        try {
            return Optional.of(ActionDTOUtil.getPrimaryEntityId(action));
        } catch (UnsupportedActionException e) {
            logger.warn("Tried to determine target entity for an unsupported action: "
                    + action.toString());
            return Optional.empty();
        }
    }

    /**
     * Get the entity type associated with this action
     *
     * @param action the action whose entity type to find
     * @return the entity type associated with this action
     * @throws UnsupportedActionException if action is not supported by XL
     * @throws EntitiesResolutionException if entities related to the target failed to resolve in
     *   TopologyProcessor
     */
    private EntityDTO.EntityType getEntityTypeForAction(ActionDTO.Action action)
            throws UnsupportedActionException, EntitiesResolutionException {
        // Use the main target entity (the entity being acted upon), as retrieved by ActionDTOUtil
        final long primaryEntityId = ActionDTOUtil.getPrimaryEntityId(action);
        // Make a remote call to the Repository service to get entity data for the target entity
        final List<TopologyEntityDTO> entitiesResponse =
                retrieveTopologyEntities(Collections.singletonList(primaryEntityId));
        // Find the matching entity data (the list should have exactly one element, but it doesn't
        // hurt to check anyway), and get the entity type data
        final int entityTypeNumber = entitiesResponse.stream()
                .filter(topologyEntityDTO -> topologyEntityDTO.getOid() == primaryEntityId)
                .findFirst()
                .orElseThrow(() -> new EntitiesResolutionException("No entity could be found for this action: "
                        + action.toString()))
                .getEntityType();
        // Convert the entity type int to an Enum and return it
        return EntityDTO.EntityType.forNumber(entityTypeNumber);
    }

    /**
     * Retrieve entity data from Repository service
     *
     * @param entities the entities to fetch data about
     * @return entity data corresponding to the provided entities
     */
    private List<TopologyEntityDTO> retrieveTopologyEntities(List<Long> entities) {
        return repositoryClient.retrieveTopologyEntities(entities, realtimeTopologyContextId)
                .getEntitiesList();
    }

    /**
     * Gets the id of the target that would execute the provided action
     *
     * @param action the action that could be executad
     * @param entityInfo discovered data about the action, retrieved from the Topology Processor
     * @return id of the target that would execute the provided action
     */
    private Optional<Long> getExecutantTargetForEntity(@Nonnull ActionDTO.Action action,
                                               @Nonnull EntityInfo entityInfo) {
        // Find all targets related to the provided entity
        Set<Long> allRelatedTargets = entityInfo.getTargetIdToProbeIdMap().keySet();
        if (allRelatedTargets.isEmpty()) {
            logger.warn("Entity: {} has no target associated with it.",
                    entityInfo::getEntityId);
            return Optional.empty();
        }
        // Use the target resolver to select the specific target to use for action execution
        return Optional.of(targetResolver.resolveExecutantTarget(action, allRelatedTargets));
    }

    /**
     * Get data about the targets and probes that discovered a single entity
     *
     * @param entityId the id of the entity
     * @return an EntityInfo, containing data about the targets and probes that discovered the entity
     * @throws EntitiesResolutionException if the requested entity is not in the response
     */
    private EntityInfo getEntityInfo(long entityId) throws EntitiesResolutionException {
        final Iterable<EntityInfoOuterClass.EntityInfo> iterableResponse =
                getEntityDataFromTopologyProcessor(Collections.singleton(entityId));
        return StreamSupport.stream(iterableResponse.spliterator(), false)
                .filter(EntityInfo::hasEntityId)
                .filter(entityInfo -> entityId == entityInfo.getEntityId())
                .findFirst()
                .orElseThrow(() ->
                        new EntitiesResolutionException("Entity id: " + entityId
                                + " not found in Topology Processor."));
    }

    /**
     * Get data about the targets and probes that discovered a Set of entities
     * For performance reasons, this remote call is done in bulk
     * The Map is entityId -> EntityInfo
     *
     * @param entityIds a Set of entity ids
     * @return a Map of entityId -> EntityInfo, containing data about the targets and probes that
     *  discovered the entity
     */
    private Map<Long, EntityInfoOuterClass.EntityInfo> getEntitiesInfo(Set<Long> entityIds) {
        final Iterable<EntityInfo> iterableResponse = getEntityDataFromTopologyProcessor(entityIds);
        return StreamSupport.stream(iterableResponse.spliterator(), false)
                .collect(Collectors.toMap(EntityInfoOuterClass.EntityInfo::getEntityId,
                        Function.identity()));
    }

    /**
     * Get entity data from the Topology Processor
     * This is is helper method that contains common code used by both getEntityInfo and
     *  getEntitiesInfo
     *
     * @param entityIds a Set of entity ids
     * @return an Iterable, containing data about the targets and probes that discovered the entity
     */
    private Iterable<EntityInfo> getEntityDataFromTopologyProcessor(final Set<Long> entityIds) {
        Iterator<EntityInfo> response =
                entityServiceBlockingStub.getEntitiesInfo(
                        EntityInfoOuterClass.GetEntitiesInfoRequest.newBuilder()
                                .addAllEntityIds(entityIds)
                                .build());

        return () -> response;
    }

}
