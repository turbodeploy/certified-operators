package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
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
     * (i.e. via the {@link ActionExecutor#executeSynchronously(long, ActionDTO.Action)} method).
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
     * Returns probeId for each provided action.
     *
     * @param actions actions to determine probeIds of these.
     * @return provided actions and determined probeIds
     * @throws TargetResolutionException will be thrown if there is no probe for some of actions
     * @throws UnsupportedActionException will be thrown if type of action if unknown
     */
    @Nonnull
    public Map<Action, Long> getProbeIdsForActions(@Nonnull Collection<Action> actions)
            throws TargetResolutionException, UnsupportedActionException {
        final Map<Long, Map<Long, EntityInfo>> actionsInvolvedEntities =
                getActionsInvolvedEntities(actions.stream()
                        .map(Action::getRecommendation)
                        .collect(Collectors.toSet()));
        final Map<Long, ActionDTO.Action> recomendationsById = actions.stream()
                .collect(Collectors.toMap(action -> action.getRecommendation().getId(),
                        Action::getRecommendation));
        final Map<Long, Long> recomendationsProbes =
                getActionDTOsProbes(recomendationsById, actionsInvolvedEntities);
        return actions.stream()
                .collect(Collectors.toMap(Function.identity(),
                        action -> recomendationsProbes.get(action.getRecommendation().getId())));
    }

    /**
     * Get the ID of the probe or target for the {@link ExecutableStep} for a
     * {@link ActionDTO.Action} recommendation.
     *
     * @param action Action which target or probe id it returnes.
     * probe id.
     * @return targetId or probeId for the action.
     * @throws TargetResolutionException will be thrown if it will not able to determine id
     */
    public long getTargetId(@Nonnull ActionDTO.Action action) throws TargetResolutionException {
        try {
            final Map<Long, EntityInfo> involvedEntityInfos = getActionInvolvedEntities(action);
            // Find the target that discovered all the involved entities.
            return getEntitiesTarget(action, involvedEntityInfos);
        } catch (UnsupportedActionException e) {
            throw new TargetResolutionException(
                    "Action: " + action.getId() + " has unsupported type: " + e.getActionType(), e);
        } catch (TargetResolutionException e) {
            throw new TargetResolutionException(("Action: " + action.getId() +
                    " has no overlapping targets between the entities involved."), e);
        }
    }

    @Nonnull
    private Map<Long, Long> getActionDTOsProbes(@Nonnull Map<Long, ActionDTO.Action> actions,
            @Nonnull Map<Long, Map<Long, EntityInfo>> actionsInvolvedEntities)
            throws TargetResolutionException {
        final Map<Long, Long> actionDTOsProbes = new HashMap<>();
        for (Map.Entry<Long, Map<Long, EntityInfo>> entry : actionsInvolvedEntities.entrySet()) {
            final long targetId = getEntitiesTarget(actions.get(entry.getKey()), entry.getValue());
            // We can just get probeId for the first entityInfo because if all provided entities
            // have common target then they have common probe for this target
            final long probeId = entry.getValue().values().iterator().next()
                    .getTargetIdToProbeIdMap().get(targetId);
            actionDTOsProbes.put(entry.getKey(), probeId);
        }
        return actionDTOsProbes;
    }

    /**
     * Gets the id of the target that the provided entities have in common.
     * (If there are multiple targets, one is selected by the targetResolver.)
     * @param action action to be passed to the targetResolver if conflict occurs
     * @param involvedEntityInfos map of entity id to entity info.
     * @return id of the common target
     * @throws TargetResolutionException if there is no common target
     */
    @Nonnull
    Long getEntitiesTarget(@Nonnull ActionDTO.Action action,
            @Nonnull Map<Long, EntityInfo> involvedEntityInfos) throws TargetResolutionException {
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
            throw new TargetResolutionException(
                    "Entities: " + involvedEntityInfos.keySet() + " has no overlapping " +
                            "targets between the entities involved.");
        }
        if (overlappingTarget.size() == 1) {
            return overlappingTarget.iterator().next();
        }
        logger.debug("There are multiple targets for action {}", action);
        return targetResolver.resolveExecutantTarget(action, overlappingTarget);
    }

    @Nonnull
    private Map<Long, EntityInfo> getActionInvolvedEntities(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException, TargetResolutionException {
        return getInvolvedEntityInfos(getIdsOfInvolvedEntitities(action));
    }

    @Nonnull
    private Map<Long, Map<Long, EntityInfo>> getActionsInvolvedEntities(
            @Nonnull final Set<ActionDTO.Action> actions)
            throws UnsupportedActionException, TargetResolutionException {
        final Map<Long, Set<Long>> actionEntitiesIds = new HashMap<>();
        for (ActionDTO.Action action : actions) {
            final Set<Long> involvedEntities = getIdsOfInvolvedEntitities(action);
            actionEntitiesIds.put(action.getId(), involvedEntities);
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
            throws TargetResolutionException {
        final Map<Long, EntityInfo> involvedEntityInfos = getEntityInfo(entities);
        if (!involvedEntityInfos.keySet().containsAll(entities)) {
            throw new TargetResolutionException(
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

    public void executeSynchronously(final long targetId, @Nonnull final ActionDTO.Action action)
            throws ExecutionStartException, InterruptedException, SynchronousExecutionException {
        execute(targetId, action);
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

    public void execute(final long targetId, @Nonnull final ActionDTO.Action action)
            throws ExecutionStartException {
        final ExecuteActionRequest executionRequest = ExecuteActionRequest.newBuilder()
                .setActionId(action.getId())
                .setTargetId(targetId)
                .setActionInfo(action.getInfo())
                .build();

        try {
            actionExecutionService.executeAction(executionRequest);
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
     * {@link ActionExecutor#executeSynchronously(long, ActionDTO.Action)} fail
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
