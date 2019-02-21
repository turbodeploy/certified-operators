package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
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
     * Create an ActionTargetSelector
     *
     * @param targetResolver to resolve conflicts when there are multiple targets which can
     *      execute the action
     * @param entitySelector to select a service entity to execute an action against
     */
    public ActionTargetSelector(@Nonnull final ActionTargetResolver targetResolver,
                                @Nonnull final ActionExecutionEntitySelector entitySelector,
                                @Nonnull final Channel topologyProcessorChannel) {
        this.targetResolver = Objects.requireNonNull(targetResolver);
        this.entitySelector = Objects.requireNonNull(entitySelector);
        this.entityServiceBlockingStub = EntityServiceGrpc.newBlockingStub(
                Objects.requireNonNull(topologyProcessorChannel));
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
        // Get a map of actions to the entity that will be used to execute the action. This may
        // differ from the primary entity if a special case applies. Generally, there is a large
        // overlap between the set of primary entities and the set of selected entities
        final Map<Action, Long> actionsToSelectedEntityIds = new HashMap<>();
        for (Action action : actions) {
            try {
                final ActionDTO.Action actionDto = action.getRecommendation();
                // Select the entity to execute the action against, and put the result in the map
                actionsToSelectedEntityIds.put(action, getExecutantEntityId(actionDto));
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
                getEntitiesInfo(Sets.newHashSet(actionsToSelectedEntityIds.values()));

        // The resultMap is Action -> id, where id is either a targetId or a probeId, depending on
        // the provided function
        Map<Action, Long> resultMap = new HashMap<>(actions.size());
        for (Entry<Action, Long> actionToSelectedEntityIdEntry : actionsToSelectedEntityIds.entrySet()) {
            try {
                final Long selectedEntityId = actionToSelectedEntityIdEntry.getValue();
                final Action action = actionToSelectedEntityIdEntry.getKey();
                final EntityInfo selectedEntity = entityInfos.get(selectedEntityId);
                if (selectedEntity != null) {
                    final ActionDTO.Action actionDto = action.getRecommendation();
                    // Get an id (either a targetId or a probeId, depending on the provided function)
                    final long id = lookupFunction.lookupRelatedId(actionDto, selectedEntity);
                    resultMap.put(action, id);
                } else {
                    logger.info("Selected entity {} involved in action {} no longer present in system.",
                        selectedEntityId, action.getId());
                }
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
     * Get the entity to use for the {@link ExecutableStep} of an {@link Action}.
     * While this defaults to the primary entity of the action--the entity being acted upon--there
     * exist special cases where a different, related entity needs to be used in order to execute
     * an action.
     *
     * @param action TopologyProcessor Action
     * @return EntityInfo for the selected entity
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     */
    private EntityInfo getExecutantEntity(@Nonnull ActionDTO.Action action)
            throws EntitiesResolutionException, UnsupportedActionException {
        // Select which entity to use for action execution.
        long selectedEntityId = getExecutantEntityId(action);
        // Retrieve the discovered entity data for this entity from the Topology Processor.
        EntityInfo entityInfo = getEntityInfo(selectedEntityId);
        return entityInfo;
    }

    /**
     * Get the entity ID to use for the {@link ExecutableStep} of an {@link Action}.
     * While this defaults to the primary entity of the action--the entity being acted upon--there
     * exist special cases where a different, related entity needs to be used in order to execute
     * an action.
     *
     * @param action TopologyProcessor Action
     * @return entityId for the selected entity
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     */
    private long getExecutantEntityId(@Nonnull ActionDTO.Action action)
            throws EntitiesResolutionException, UnsupportedActionException {
        // Check for special cases using the entitySelector
        return entitySelector.getEntityId(action)
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
        // Determine the entity to execute this action against
        EntityInfo executantEntity = getExecutantEntity(action);
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
                        new EntitiesResolutionException("Entity id " + entityId
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
