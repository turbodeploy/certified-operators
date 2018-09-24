package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Executes actions by converting {@link ActionDTO.Action} objects into {@link ExecuteActionRequest}
 * and sending them to the {@link TopologyProcessor}.
 */
public class ActionExecutor implements ActionExecutionListener {
    private static final Logger logger = LogManager.getLogger();

    private final ActionExecutionServiceBlockingStub actionExecutionService;

    private final EntityServiceGrpc.EntityServiceBlockingStub entityServiceBlockingStub;

    private final ActionTargetResolver targetResolver;

    /**
     * Futures to track success or failure of actions that are executing synchronously
     * (i.e. via the {@link ActionExecutor#executeSynchronously(long, ActionDTO.Action, Optional)}
     * method).
     */
    private final Map<Long, CompletableFuture<Void>> inProgressSyncActions =
            Collections.synchronizedMap(new HashMap<>());

    /**
     * Creates an object of ActionExecutor with ActionExecutionService and EntityService.
     *
     * @param topologyProcessorChannel to create services
     * @param targetResolver to resolve conflicts when there are multiple targets which can
     * execute the action
     */
    public ActionExecutor(@Nonnull final Channel topologyProcessorChannel,
            @Nonnull final ActionTargetResolver targetResolver) {
        this.actionExecutionService = ActionExecutionServiceGrpc
                .newBlockingStub(Objects.requireNonNull(topologyProcessorChannel));
        this.entityServiceBlockingStub = EntityServiceGrpc.newBlockingStub(topologyProcessorChannel);
        this.targetResolver = Objects.requireNonNull(targetResolver);
    }

    /**
     * Returns probeId for each provided action. If action is not supported, it will be omitted in
     * the result map.
     *
     * @param actions actions to determine probeIds of these.
     * @return provided actions and determined probeIds
     * @throws EntitiesResolutionException if some entities failed to resolve agains
     *         TopologyProcessor
     */
    @Nonnull
    public Map<Action, Long> getProbeIdsForActions(@Nonnull Collection<Action> actions)
            throws EntitiesResolutionException {
        // from Set(action-recommendations), make a map from action-id -> ( entity-id -> EntityInfo )
        final Map<Long, Map<Long, EntityInfo>> actionsInvolvedEntities =
                getActionsInvolvedEntities(actions.stream()
                        .map(Action::getRecommendation)
                        .collect(Collectors.toSet()));
        // calculate map from action-recommendation-id -> action-recommendation
        final Map<Long, ActionDTO.Action> recomendationsById = actions.stream()
                .collect(Collectors.toMap(action -> action.getRecommendation().getId(),
                        Action::getRecommendation));
        final Map<Long, Long> recomendationsProbes =
                getActionDTOsProbes(recomendationsById, actionsInvolvedEntities);
        return actions.stream()
                .filter(action -> recomendationsProbes.containsKey(
                        action.getRecommendation().getId()))
                .collect(Collectors.toMap(Function.identity(),
                        action -> recomendationsProbes.get(action.getRecommendation().getId())));
    }

    /**
     * Get the ID of the probe or target for the {@link ExecutableStep} for an {@link Action}.
     *
     * @param action TopologyProcessor Action
     * @return targetId for the action.
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     * @throws UnsupportedActionException if action is not supported by XL
     * @throws TargetResolutionException if no target found for this action suitable for execution
     */
    public long getTargetId(@Nonnull ActionDTO.Action action)
            throws EntitiesResolutionException, UnsupportedActionException,
            TargetResolutionException {
        final Map<Long, EntityInfo> involvedEntityInfos = getActionInvolvedEntities(action);
        // Find the target that discovered all the involved entities.
        final Optional<Long> targetId = getEntitiesTarget(action, involvedEntityInfos);
        return targetId.orElseThrow(() -> new TargetResolutionException(
                ("Action: " + action.getId() +
                        " has no overlapping targets between the entities involved.")));
    }

    /**
     * Given a map from action-recommendation-id to action-recommendation and a
     * map action-id -> ( entity-id -> EntityInfo ), determine a map
     * (action-recommendation-id -> probe-id).
     *
     *
     * @param actions a map from action-id -> ActionDTO.Action
     * @param actionsInvolvedEntities a map from action-id -> ( entity-id -> EntityInfo )
     * @return a map from Long -> Long of action recommendation id -> probe id
     */
    @Nonnull
    private Map<Long, Long> getActionDTOsProbes(@Nonnull Map<Long, ActionDTO.Action> actions,
            @Nonnull Map<Long, Map<Long, EntityInfo>> actionsInvolvedEntities) {
        final Map<Long, Long> actionDTOsProbes = new HashMap<>();
        for (Map.Entry<Long, Map<Long, EntityInfo>> entry : actionsInvolvedEntities.entrySet()) {
            // for each action, determine the target for this action
            final Optional<Long> targetId =
                    getEntitiesTarget(actions.get(entry.getKey()), entry.getValue());
            // TODO this logic should be changed when support for cross-target actions is added
            if (targetId.isPresent()) {
                // We can just get probeId for the first entityInfo because if all provided entities
                // have common target then they have common probe for this target
                final long probeId = entry.getValue()
                        .values()
                        .iterator()
                        .next()
                        .getTargetIdToProbeIdMap()
                        .get(targetId.get());
                actionDTOsProbes.put(entry.getKey(), probeId);
            } else {
                logger.debug("There is no common target for action {} across entities {}," +
                        " skipping the action", entry::getKey, () -> entry.getValue().keySet());
            }
        }
        return actionDTOsProbes;
    }

    /**
     * Gets the id of the target that the provided entities have in common.
     * (If there are multiple targets, one is selected by the targetResolver.)
     *
     * @param action action to be passed to the targetResolver if conflict occurs
     * @param involvedEntityInfos map of entity id to entity info.
     * @return id of the common target
     */
    @Nonnull
    public Optional<Long> getEntitiesTarget(@Nonnull ActionDTO.Action action,
            @Nonnull Map<Long, EntityInfo> involvedEntityInfos) {
        // calculate overlapping targets for this action - to be improved, see Jeff
        Set<Long> overlappingTarget = null;
        for (final EntityInfo info : involvedEntityInfos.values()) {
            final Set<Long> curInfoTargets = new HashSet<>(info.getTargetIdToProbeIdMap().keySet());
            if (overlappingTarget == null) {
                overlappingTarget = new HashSet<>(curInfoTargets);
            } else {
                overlappingTarget.retainAll(curInfoTargets);
            }
        }
        if (CollectionUtils.isEmpty(overlappingTarget)) {
            // A lot of actions' entities have bothing in common. We hust need to filter them out.
            logger.warn(
                    "Entities: {} have no overlapping " + "targets between the entities involved.",
                    involvedEntityInfos::keySet);
            return Optional.empty();
        }
        return Optional.of(targetResolver.resolveExecutantTarget(action, overlappingTarget));
    }

    @Nonnull
    private Map<Long, EntityInfo> getActionInvolvedEntities(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException, EntitiesResolutionException {
        return getInvolvedEntityInfos(getIdsOfInvolvedEntitities(action));
    }

    @Nonnull
    private Map<Long, Map<Long, EntityInfo>> getActionsInvolvedEntities(
            @Nonnull final Set<ActionDTO.Action> actions) throws EntitiesResolutionException  {
        final Map<Long, Set<Long>> actionEntitiesIds = new HashMap<>();
        for (ActionDTO.Action action : actions) {
            try {
                final Set<Long> involvedEntities = getIdsOfInvolvedEntitities(action);
                actionEntitiesIds.put(action.getId(), involvedEntities);
            } catch (UnsupportedActionException e) {
                logger.warn("Ignoring action (hiding it): " + action, e);
            }
        }
        final Set<Long> allEntities = actionEntitiesIds.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final Map<Long, EntityInfo> involvedEntityInfos = getInvolvedEntityInfos(allEntities);
        final Map<Long, Map<Long, EntityInfo>> actionEntityInfos = new HashMap<>();
        actionEntitiesIds.entrySet()
                .forEach(actionIdEntitiesIds -> actionEntityInfos.put(actionIdEntitiesIds.getKey(),
                        mapEntityInfo(actionIdEntitiesIds.getValue(), involvedEntityInfos)));
        return actionEntityInfos;
    }

    @Nonnull
    private Map<Long, EntityInfo> getInvolvedEntityInfos(@Nonnull final Set<Long> entities)
            throws EntitiesResolutionException {
        final Map<Long, EntityInfo> involvedEntityInfos = getEntityInfo(entities);
        if (!involvedEntityInfos.keySet().containsAll(entities)) {
            throw new EntitiesResolutionException(
                    "Entities: " + entities + " Some entities not found in Topology Processor.");
        }
        return involvedEntityInfos;
    }

    @Nonnull
    private Set<Long> getIdsOfInvolvedEntitities(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException {
        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntities(action);
        // This shouldn't happen.
        if (involvedEntities.isEmpty()) {
            throw new IllegalStateException(
                    "Action: " + action.getId() + " has no involved entities.");
        }
        return involvedEntities;
    }

    private Map<Long, EntityInfo> mapEntityInfo(Set<Long> entitiesId,
            Map<Long, EntityInfo> involvedEntityInfos) {
        return entitiesId.stream()
                .map(id -> involvedEntityInfos.get(id))
                .collect(Collectors.toMap(EntityInfo::getEntityId, Function.identity()));
    }

    /**
     * Schedule the given {@link ActionDTO.Action} for execution and wait for completion.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's a
     *                 Workflow specified - see below)
     * @param action the Action to execute
     * @param workflowOpt an Optional specifying a Workflow to override the execution of the Action
     * @throws ExecutionStartException if the Action fails to start
     * @throws InterruptedException if the "wait for completion" is interrupted
     * @throws SynchronousExecutionException any other execute exception
     */
    public void executeSynchronously(final long targetId, @Nonnull final ActionDTO.Action action,
                                     @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt)
            throws ExecutionStartException, InterruptedException, SynchronousExecutionException {
        Objects.requireNonNull(action);
        Objects.requireNonNull(workflowOpt);
        execute(targetId, action, workflowOpt);
        final CompletableFuture<Void> future = new CompletableFuture<>();
        inProgressSyncActions.put(action.getId(), future);
        try {
            future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SynchronousExecutionException) {
                throw (SynchronousExecutionException)e.getCause();
            } else {
                throw new IllegalStateException("Unexpected execution exception!", e);
            }
        }
    }

    /**
     * Schedule execution of the given {@link ActionDTO.Action} and return immediately.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's a
     *                 Workflow specified - see below)
     * @param action the Action to execute
     * @param workflowOpt an Optional specifying a Workflow to override the execution of the Action
     * @throws ExecutionStartException
     */
    public void execute(final long targetId, @Nonnull final ActionDTO.Action action,
                        @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt)
            throws ExecutionStartException {
        Objects.requireNonNull(action);
        Objects.requireNonNull(workflowOpt);
        final ExecuteActionRequest.Builder executionRequestBuilder = ExecuteActionRequest.newBuilder()
                .setActionId(action.getId())
                .setActionInfo(action.getInfo());
        if (workflowOpt.isPresent()) {
            // if there is a Workflow for this action, then the target to execute the action
            // will be the one from which the Workflow was discovered instead of the target
            // from which the original Target Entity was discovered
            final WorkflowDTO.WorkflowInfo workflowInfo = workflowOpt.get().getWorkflowInfo();
            executionRequestBuilder.setTargetId(workflowInfo.getTargetId());
            executionRequestBuilder.setWorkflowInfo(workflowInfo);
        } else {
            // Typically, the target to execute the action is the target from which the
            // Target Entity was discovered
            executionRequestBuilder.setTargetId(targetId);
        }

        try {
            actionExecutionService.executeAction(executionRequestBuilder.build());
            logger.info("Action: {} started.", action.getId());
        } catch (StatusRuntimeException e) {
            throw new ExecutionStartException(
                    "Action: " + action.getId() + " failed to start. Failure status: " +
                            e.getStatus(), e);
        }
    }

    public Map<Long, EntityInfoOuterClass.EntityInfo> getEntityInfo(Set<Long> entityIds) {
        Iterator<EntityInfoOuterClass.EntityInfo> response =
                entityServiceBlockingStub.getEntitiesInfo(
                        EntityInfoOuterClass.GetEntitiesInfoRequest.newBuilder()
                                .addAllEntityIds(entityIds)
                                .build());

        final Iterable<EntityInfoOuterClass.EntityInfo> iterableResponse = () -> response;
        return StreamSupport.stream(iterableResponse.spliterator(), false)
                .collect(Collectors.toMap(EntityInfoOuterClass.EntityInfo::getEntityId,
                        Function.identity()));
    }

    @Override
    public void onActionProgress(@Nonnull final ActionProgress actionProgress) {
        // No one cares.
    }

    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        CompletableFuture<?> futureForAction = inProgressSyncActions.get(actionSuccess.getActionId());
        if (futureForAction != null) {
            futureForAction.complete(null);
        }

    }

    @Override
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        final CompletableFuture<Void> futureForAction =
                inProgressSyncActions.get(actionFailure.getActionId());
        if (futureForAction != null) {
            futureForAction.completeExceptionally(new SynchronousExecutionException(actionFailure));
        }
    }

    /**
     * Exception thrown when an action executed via
     * {@link ActionExecutor#executeSynchronously(long, ActionDTO.Action, Optional)} fail
     * to complete.
     */
    public static class SynchronousExecutionException extends Exception {
        private final ActionFailure actionFailure;

        private SynchronousExecutionException(@Nonnull final ActionFailure failure) {
            this.actionFailure = Objects.requireNonNull(failure);
        }

        public ActionFailure getFailure() {
            return actionFailure;
        }
    }
}
