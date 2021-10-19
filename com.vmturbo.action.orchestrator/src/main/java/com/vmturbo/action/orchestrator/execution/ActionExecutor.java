package com.vmturbo.action.orchestrator.execution;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionStateFactory.DefaultSynchronousExecutionStateFactory;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator.ActionTemplateApplicationException;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionListRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost;

/**
 * Executes actions by converting {@link ActionDTO.Action} objects into {@link ExecuteActionRequest}
 * and sending them to the {@link TopologyProcessor}.
 */
public class ActionExecutor implements ActionExecutionListener {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A client for making remote calls to the Topology Processor service to execute actions.
     */
    private final ActionExecutionServiceBlockingStub actionExecutionService;

    private final ActionExecutionStore actionExecutionStore;

    /**
     * Futures to track success or failure of actions that are executing synchronously
     * (i.e. via the
     * {@link ActionExecutor#executeSynchronously(long, List)}
     * method).
     */
    private final Map<Long, SynchronousExecutionState> inProgressSyncActions =
            Collections.synchronizedMap(new HashMap<>());

    private final int executionTimeout;

    private final TimeUnit executionTimeoutUnit;

    private final SynchronousExecutionStateFactory synchronousExecutionStateFactory;

    private final LicenseCheckClient licenseCheckClient;
    private final ActionTemplateApplicator actionTemplateApplicator;

    /**
     * Creates an instance {@link ActionExecutor}.
     *
     * @param topologyProcessorChannel TP Grpc communication channel.
     * @param actionExecutionStore the persistent store for action execution.
     * @param clock the clock.
     * @param executionTimeout Execution timeout for actions.
     * @param executionTimeoutUnit Action execution timeout unit.
     * @param licenseCheckClient the client for checking the license.
     * @param actionTemplateApplicator the action template applicator for webhook workflows.
     */
    public ActionExecutor(@Nonnull final Channel topologyProcessorChannel,
                   @Nonnull final ActionExecutionStore actionExecutionStore,
                   @Nonnull final Clock clock,
                   final int executionTimeout,
                   @Nonnull final TimeUnit executionTimeoutUnit,
                   @Nonnull final LicenseCheckClient licenseCheckClient,
                   @Nonnull final ActionTemplateApplicator actionTemplateApplicator) {
        this(topologyProcessorChannel, actionExecutionStore,
                new DefaultSynchronousExecutionStateFactory(clock), executionTimeout,
                executionTimeoutUnit, licenseCheckClient, actionTemplateApplicator);
    }

    @VisibleForTesting
    ActionExecutor(@Nonnull final Channel topologyProcessorChannel,
                   @Nonnull final ActionExecutionStore actionExecutionStore,
                   @Nonnull final SynchronousExecutionStateFactory executionStateFactory,
                   final int executionTimeout,
                   @Nonnull final TimeUnit executionTimeoutUnit,
                   @Nonnull final LicenseCheckClient licenseCheckClient,
                   @Nonnull final ActionTemplateApplicator actionTemplateApplicator) {
        this.actionExecutionService = ActionExecutionServiceGrpc.newBlockingStub(topologyProcessorChannel);
        this.actionExecutionStore = Objects.requireNonNull(actionExecutionStore);
        this.synchronousExecutionStateFactory = executionStateFactory;
        this.executionTimeout = executionTimeout;
        this.executionTimeoutUnit = executionTimeoutUnit;
        this.licenseCheckClient = licenseCheckClient;
        this.actionTemplateApplicator = Objects.requireNonNull(actionTemplateApplicator);
    }

    /**
     * Schedule the list of {@link ActionDTO.Action}s for execution and wait for completion.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's a
     *                 Workflow specified - see below)
     * @param actionList the Action list to execute
     * @throws ExecutionStartException if the Action fails to start
     * @throws InterruptedException if the "wait for completion" is interrupted
     * @throws SynchronousExecutionException any other execute exception
     */
    public void executeSynchronously(
            final long targetId,
            @Nonnull final List<ActionWithWorkflow> actionList)
            throws ExecutionStartException, InterruptedException, SynchronousExecutionException {
        Objects.requireNonNull(actionList);
        final String actionIdString = getActionIdsString(actionList);
        logger.info("Starting synchronous actions: {}", actionIdString);
        execute(targetId, actionList);
        final SynchronousExecutionState synchronousExecutionState =
                synchronousExecutionStateFactory.newState();
        for (final ActionWithWorkflow actionWithWorkflow : actionList) {
            final long actionId = actionWithWorkflow.getAction().getRecommendation().getId();
            inProgressSyncActions.put(actionId, synchronousExecutionState);
        }
        try {
            // TODO (roman, July 30 2019): OM-49081 - Handle TP restarts and dropped messages
            // without relying only on timeout.
            synchronousExecutionState.waitForActionCompletion(executionTimeout, executionTimeoutUnit);
            logger.info("Completed synchronous actions: {}", actionIdString);
        } catch (TimeoutException e) {
            throw new SynchronousExecutionException(String.format("Actions %s timed out after %s %s",
                    actionIdString, executionTimeout, executionTimeoutUnit.toString()));
        }
    }

    /**
     * Creates execute action request, suitable to send it to topology processor.
     *
     * @param targetId target to execute action on
     * @param action action to execute
     * @param workflowOpt workflow associated with this target (if any)
     * @throws ExecutionInitiationException if failed to process workflow
     * @return DTO to send request to topology processor
     */
    @Nonnull
    public ExecuteActionRequest createRequest(final long targetId,
                         @Nonnull final ActionDTO.ActionSpec action,
                         @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt)
            throws ExecutionInitiationException {
        return createRequest(targetId, action, workflowOpt, null, action.getRecommendation().getId());
    }

    /**
     * Creates execute action request, suitable to send it to topology processor.
     *
     * @param targetId target to execute action on
     * @param action action to execute
     * @param workflowOpt workflow associated with this target (if any)
     * @param explanation the explanation string describing the action
     * @param actionId the action identifier (actionId or recommendationId used for external
     *        audit/approve operations)
     * @throws ExecutionInitiationException if failed to process workflow
     * @return DTO to send request to topology processor
     */
    @Nonnull
    public ExecuteActionRequest createRequest(final long targetId,
            @Nonnull final ActionDTO.ActionSpec action,
            @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt,
            @Nullable String explanation,
            final long actionId) throws ExecutionInitiationException {
        Objects.requireNonNull(action);
        Objects.requireNonNull(workflowOpt);

        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(action.getRecommendation());

        final ExecuteActionRequest.Builder executionRequestBuilder = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setActionSpec(action)
                .setActionType(actionType);
        if (explanation != null) {
            executionRequestBuilder.setExplanation(explanation);
        }
        if (workflowOpt.isPresent()) {
            // if there is a Workflow for this action, then the target to execute the action
            // will be the one from which the Workflow was discovered instead of the target
            // from which the original Target Entity was discovered
            executionRequestBuilder.setTargetId(workflowOpt.get().getWorkflowInfo().getTargetId());

            final WorkflowDTO.Workflow workflow;
            try {
                workflow = actionTemplateApplicator
                        .addTemplateInformation(action, workflowOpt.get());
            } catch (ActionTemplateApplicationException ex) {
                throw new ExecutionInitiationException("Failed to apply template: " + ex.getMessage(),
                        ex, Status.Code.INTERNAL);
            }

            executionRequestBuilder.setWorkflow(workflow);
        } else {
            // Typically, the target to execute the action is the target from which the
            // Target Entity was discovered
            executionRequestBuilder.setTargetId(targetId);
        }
        executionRequestBuilder.setOriginTargetId(targetId);

        return executionRequestBuilder.build();
    }

    /**
     * Creates execute action list request for sending to Topology Processor.
     *
     * @param targetId Target to execute action on.
     * @param actionList Action list to execute.
     * @throws ExecutionInitiationException If failed to process workflow.
     * @return Request DTO to be sent to Topology Processor.
     */
    @Nonnull
    public ExecuteActionListRequest createRequest(
            final long targetId,
            @Nonnull final List<ActionWithWorkflow> actionList)
            throws ExecutionInitiationException {
        Objects.requireNonNull(actionList);

        final ExecuteActionListRequest.Builder request = ExecuteActionListRequest.newBuilder();

        for (final ActionWithWorkflow actionWithWorkflow : actionList) {
            final ActionSpec action = actionWithWorkflow.getAction();
            final ExecuteActionRequest actionRequest = createRequest(targetId, action,
                    actionWithWorkflow.getWorkflow(), null, action.getRecommendation().getId());
            request.addActionRequest(actionRequest);
        }

        return request.build();
    }

    /**
     * Schedule execution of the given {@link ActionDTO.Action} and return immediately.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's a
     *                 Workflow specified - see below)
     * @param action the Action to execute
     * @param workflowOpt an Optional specifying a Workflow to override the execution of the Action
     * @throws ExecutionStartException if action execution failed to start or failed to process workflow
     */
    public void execute(final long targetId, @Nonnull final ActionDTO.ActionSpec action,
                        @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt)
            throws ExecutionStartException {
        execute(targetId, Collections.singletonList(new ActionWithWorkflow(action, workflowOpt)));
    }

    /**
     * Schedule execution of the list of {@link ActionDTO.Action}s and return immediately.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's a
     *                 Workflow specified - see below)
     * @param actionList the Action list to execute
     * @throws ExecutionStartException if action execution failed to start or failed to process workflow
     */
    public void execute(final long targetId, @Nonnull final List<ActionWithWorkflow> actionList)
            throws ExecutionStartException {
        // pjs: make sure a license is available when it's time to execute an action
        if (!licenseCheckClient.hasValidNonExpiredLicense()) {
            // no valid license detected!
            // this could be ephemeral -- e.g. a valid license could be installed, or the auth
            // component or this component could be in the middle of starting up.
            throw new ExecutionStartException("No valid license was detected. Will not execute the action.");
        }
        final String actionIdString = getActionIdsString(actionList);
        try {
            // TODO (roman, July 30 2019): OM-49080 - persist the state of in-progress actions in
            // the database, so that we don't lose the information across restarts.
            logger.info("Starting actions: {}", actionIdString);
            if (actionList.size() == 1) {
                // Execute single action
                final ActionWithWorkflow actionWithWorkflow = actionList.get(0);
                final ExecuteActionRequest request = createRequest(targetId,
                        actionWithWorkflow.getAction(), actionWithWorkflow.getWorkflow());
                actionExecutionService.executeAction(request);
            } else {
                // Execute action list
                final ExecuteActionListRequest request = createRequest(targetId, actionList);
                actionExecutionService.executeActionList(request);
            }
            logger.info("Actions started: {}", actionIdString);
        } catch (StatusRuntimeException e) {
            throw new ExecutionStartException(
                    "Actions " + actionIdString
                            + " failed to start. Failure status: " + e.getStatus(), e);
        } catch (ExecutionInitiationException e) {
            throw new ExecutionStartException(
                    "Actions " + actionIdString + " failed to start.", e);
        }
    }

    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        SynchronousExecutionState futureForAction = inProgressSyncActions.get(actionSuccess.getActionId());
        if (futureForAction != null) {
            futureForAction.complete(null);
        } else {
            logger.warn("Cannot find action ID {} in inProgressSyncActions: {}",
                    actionSuccess.getActionId(), inProgressSyncActions.keySet());
        }
        actionExecutionStore.removeCompletedAction(actionSuccess.getActionId());
    }

    @Override
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        final SynchronousExecutionState futureForAction =
                inProgressSyncActions.get(actionFailure.getActionId());
        if (futureForAction != null) {
            futureForAction.complete(new SynchronousExecutionException(
                    actionFailure.getErrorDescription()));
        } else {
            logger.warn("Cannot find action ID {} in inProgressSyncActions: {}",
                    actionFailure.getActionId(), inProgressSyncActions.keySet());
        }
        actionExecutionStore.removeCompletedAction(actionFailure.getActionId());
    }

    @Override
    public void onActionsLost(@Nonnull final ActionsLost actionsLost) {
        final Set<Long> lostActions;
        if (actionsLost.getBeforeTime() > 0) {
            lostActions = new HashSet<>();
            synchronized (inProgressSyncActions) {
                inProgressSyncActions.forEach((id, executionState) -> {
                    if (executionState.startedBefore(actionsLost.getBeforeTime())) {
                        lostActions.add(id);
                    }
                });
            }
        } else if (!actionsLost.getLostActionId().getActionIdsList().isEmpty()) {
            lostActions = new HashSet<>(actionsLost.getLostActionId().getActionIdsList());
        } else {
            lostActions = Collections.emptySet();
        }

        if (!lostActions.isEmpty()) {
            logger.info("Lost {} actions.", lostActions.size());
        }

        lostActions.forEach(id -> onActionFailure(ActionFailure.newBuilder()
            .setActionId(id)
            .setErrorDescription("Topology Processor lost action state.")
            .build()));
    }

    /**
     * Exception thrown when an action executed via
     * {@link ActionExecutor#executeSynchronously(long, List)} fail
     * to complete.
     */
    public static class SynchronousExecutionException extends Exception {
        SynchronousExecutionException(@Nonnull final String message) {
            super(message);
        }
    }

    private static String getActionIdsString(@Nonnull final List<ActionWithWorkflow> actionList) {
        return actionList.stream()
                .map(actionWithWorkflow -> actionWithWorkflow.getAction().getRecommendation().getId())
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    /**
     * Helper class to hold relevant information for synchronous action execution.
     */
    @VisibleForTesting
    static class SynchronousExecutionState {

        /**
         * This future will be completed when the action succeeds/fails.
         */
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        /**
         * The time that the action started executing (i.e. the time we make the call to the
         * Topology Processor).
         *
         * <p>Primarily used to determine if an actions should be dropped when receiving
         * an {@link ActionsLost} message.
         */
        private final long executionStartTime;

        private SynchronousExecutionState(final long startTime) {
            this.executionStartTime = startTime;
        }

        void waitForActionCompletion(final long timeout, final TimeUnit timeUnit)
            throws InterruptedException, TimeoutException, SynchronousExecutionException {
            try {
                future.get(timeout, timeUnit);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof SynchronousExecutionException) {
                    throw (SynchronousExecutionException)e.getCause();
                } else {
                    throw new IllegalStateException("Unexpected execution exception!", e);
                }
            }
        }

       boolean startedBefore(final long timeMillis) {
            return executionStartTime < timeMillis;
       }

       void complete(@Nullable SynchronousExecutionException exception) {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(null);
            }
       }
    }

    /**
     * Factory class to inject mock {@link SynchronousExecutionState}s during testing.
     */
    @VisibleForTesting
    interface SynchronousExecutionStateFactory {

        @Nonnull
        SynchronousExecutionState newState();

        /**
         * Default/real implementation of {@link SynchronousExecutionStateFactory}.
         */
        @VisibleForTesting
        class DefaultSynchronousExecutionStateFactory implements SynchronousExecutionStateFactory {

            private final Clock clock;

            @VisibleForTesting
            DefaultSynchronousExecutionStateFactory(@Nonnull final Clock clock) {
                this.clock = clock;
            }

            @Nonnull
            @Override
            public SynchronousExecutionState newState() {
                return new SynchronousExecutionState(clock.millis());
            }
        }

    }
}
