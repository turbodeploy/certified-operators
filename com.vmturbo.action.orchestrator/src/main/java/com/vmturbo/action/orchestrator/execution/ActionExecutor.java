package com.vmturbo.action.orchestrator.execution;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator.ActionTemplateApplicationException;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionListRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Executes actions by converting {@link ActionDTO.Action} objects into {@link ExecuteActionRequest}
 * and sending them to the {@link TopologyProcessor}.
 */
public class ActionExecutor {

    /**
     * The constant TIMED_OUT_ACTION_EXECUTION_MSG.
     */
    @VisibleForTesting
    static final String TIMED_OUT_ACTION_EXECUTION_MSG = "Failed to execute action %d timed out after %d %s";

    private static final Logger logger = LogManager.getLogger();

    /**
     * A client for making remote calls to the Topology Processor service to execute actions.
     */
    private final ActionExecutionServiceBlockingStub actionExecutionService;

    private final ActionExecutionStore actionExecutionStore;

    private final WorkflowStore workflowStore;

    private final ActionTranslator actionTranslator;

    /**
     * Futures to track success or failure of actions that are executing synchronously
     * (i.e. via the
     * {@link ActionExecutor#executeSynchronously(long, List, ActionExecutionListener)}
     * method).
     */
    private final Map<Long, BlockingQueue> inProgressSyncActions =
            Collections.synchronizedMap(new HashMap<>());

    private final int executionTimeout;

    private final TimeUnit executionTimeoutUnit;

    private final LicenseCheckClient licenseCheckClient;
    private final ActionTemplateApplicator actionTemplateApplicator;

    /**
     * Creates an instance {@link ActionExecutor}.
     *
     * @param topologyProcessorChannel TP Grpc communication channel.
     * @param actionExecutionStore the persistent store for action execution.
     * @param executionTimeout Execution timeout for actions.
     * @param executionTimeoutUnit Action execution timeout unit.
     * @param licenseCheckClient the client for checking the license.
     * @param actionTemplateApplicator the action template applicator for webhook workflows.
     * @param workflowStore the workflow store
     * @param actionTranslator the action translator
     */
    public ActionExecutor(@Nonnull final Channel topologyProcessorChannel,
                   @Nonnull final ActionExecutionStore actionExecutionStore,
                   final int executionTimeout,
                   @Nonnull final TimeUnit executionTimeoutUnit,
                   @Nonnull final LicenseCheckClient licenseCheckClient,
                   @Nonnull final ActionTemplateApplicator actionTemplateApplicator,
                   @Nonnull final WorkflowStore workflowStore,
                   @Nonnull final ActionTranslator actionTranslator) {
        this.actionExecutionService = ActionExecutionServiceGrpc.newBlockingStub(topologyProcessorChannel);
        this.actionExecutionStore = Objects.requireNonNull(actionExecutionStore);
        this.executionTimeout = executionTimeout;
        this.executionTimeoutUnit = executionTimeoutUnit;
        this.licenseCheckClient = licenseCheckClient;
        this.actionTemplateApplicator = Objects.requireNonNull(actionTemplateApplicator);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
    }

    /**
     * Schedule the list of {@link ActionDTO.Action}s for execution and wait for completion.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's
     *         a                 Workflow specified - see below)
     * @param actionList the Action list to execute
     * @param actionExecutionListener the listener that updates the action state based on
     *         events.
     * @throws ExecutionStartException if the Action fails to start
     * @throws InterruptedException if the "wait for completion" is interrupted
     */
    public void executeSynchronously(
            final long targetId,
            @Nonnull final List<ActionWithWorkflow> actionList,
            ActionExecutionListener actionExecutionListener)
            throws ExecutionStartException, InterruptedException {
        Objects.requireNonNull(actionList);
        final String actionIdString = getActionIdsString(actionList);
        final BlockingQueue<Pair<Action, ActionState>> actionEventsQueue =
                new ArrayBlockingQueue<>(actionList.size());
        Set<Long> actionIds = new HashSet<>();
        for (final ActionWithWorkflow actionWithWorkflow : actionList) {
            final long actionId = actionWithWorkflow.getAction().getRecommendation().getId();
            inProgressSyncActions.put(actionId, actionEventsQueue);
            actionIds.add(actionId);
        }
        logger.info("Starting synchronous actions: {}", actionIdString);
        execute(targetId, actionList);


        // wait until all actions reach a final state
        while (!actionIds.isEmpty()) {
            final Pair<Action, ActionState> actionUpdate = actionEventsQueue.poll(executionTimeout, executionTimeoutUnit);
            if (actionUpdate != null) {
                if (actionUpdate.getValue() == ActionDTO.ActionState.SUCCEEDED
                        || actionUpdate.getValue() == ActionDTO.ActionState.FAILED) {
                    logger.info("The action {}:{} finished.", actionUpdate.getKey().getId(),
                            actionUpdate.getKey().getRecommendationOid());
                    actionIds.remove(actionUpdate.getKey().getId());
                    inProgressSyncActions.remove(actionUpdate.getKey().getId());
                    actionExecutionStore.removeCompletedAction(actionUpdate.getKey().getId());
                } else {
                    continueActionExecution(actionUpdate.getKey(), actionExecutionListener);
                }
            } else {
                logger.error("Failed the actions: {} timed out after {} {}.", actionIds,
                        executionTimeout, executionTimeoutUnit);
                // fail only those that have not yet reached a final state
                for (final long actionId : actionIds) {
                    inProgressSyncActions.remove(actionId);
                    actionExecutionStore.removeCompletedAction(actionId);
                    final String errorMsg = String.format(TIMED_OUT_ACTION_EXECUTION_MSG,
                            actionId, executionTimeout, executionTimeoutUnit);
                    logger.error(errorMsg);
                    actionExecutionListener.onActionFailure(ActionNotificationDTO.ActionFailure
                            .newBuilder()
                            .setActionId(actionId)
                            .setErrorDescription(errorMsg)
                            .build());
                }
                break;
            }
        }
        logger.info("Completed synchronous actions: {}", actionIdString);
    }

    /**
     * For actions with multiple steps (e.g. PRE and POST), kick off the next stage of execution.
     *
     * @param action to continue executing
     */
    private void continueActionExecution(final Action action,
                                         ActionExecutionListener actionExecutionListener) {
        final Optional<ActionDTO.Action> translatedRecommendation =
                action.getActionTranslation().getTranslatedRecommendation();
        if (translatedRecommendation.isPresent() && action.getCurrentExecutableStep().isPresent()) {
            long targetId = action.getCurrentExecutableStep().get().getTargetId();
            // execute the action, passing the workflow override (if any)
            try {
                execute(targetId, actionTranslator.translateToSpec(action),
                        action.getWorkflow(workflowStore, action.getState()));
            } catch (ExecutionStartException | WorkflowStoreException e) {
                final String errorMsg = String.format("Failed to start next executable step of action "
                    + "%s:%s due to error: %s", action.getId(), action.getRecommendationOid(),
                        e.getMessage());
                logger.error(errorMsg, e);
                actionExecutionListener.onActionFailure(ActionNotificationDTO.ActionFailure
                        .newBuilder()
                        .setActionId(action.getId())
                        .setErrorDescription(errorMsg)
                        .build());
            }
        } else if (!translatedRecommendation.isPresent()) {
            final String errorMsg = String.format("Failed to start next execution step of action "
                + "%s:%s because the translated recommendation is not present.", action.getId(),
                action.getRecommendationOid());
            logger.error(errorMsg);
            actionExecutionListener.onActionFailure(ActionNotificationDTO.ActionFailure
                .newBuilder()
                .setActionId(action.getId())
                .setErrorDescription(errorMsg)
                .build());
        } else {
            final String errorMsg = String.format("Failed to start next execution step of action "
                + "%s:%s because the next step is not present.", action.getId(),
                action.getRecommendationOid());
            logger.error(errorMsg);
            actionExecutionListener.onActionFailure(ActionNotificationDTO.ActionFailure
                    .newBuilder()
                    .setActionId(action.getId())
                    .setErrorDescription(errorMsg)
                    .build());
        }
    }

    /**
     * Creates execute action request, suitable to send it to topology processor.
     *
     * @param targetId target to execute action on
     * @param action action to execute
     * @param workflowOpt workflow associated with this target (if any)
     * @return DTO to send request to topology processor
     * @throws ExecutionInitiationException if failed to process workflow
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
     *         audit/approve operations)
     * @return DTO to send request to topology processor
     * @throws ExecutionInitiationException if failed to process workflow
     */
    @Nonnull
    public ExecuteActionRequest createRequest(final long targetId,
            @Nonnull final ActionDTO.ActionSpec action,
            @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt,
            @Nullable String explanation,
            final long actionId) throws ExecutionInitiationException {
        Objects.requireNonNull(action);
        Objects.requireNonNull(workflowOpt);
        try (Tracing.TracingScope tracingScope =
                     Tracing.trace("creating_action_request")) {
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
                logger.debug("Workflow with ID {} is set for the action. "
                        + "Sending action with ID {} to target with ID {}",
                        () -> workflowOpt.get().getId(),
                        () -> actionId,
                        () -> workflowOpt.get().getWorkflowInfo().getTargetId());
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
                logger.debug("No workflow is set. Sending action with id {} to target with {}",
                        () -> actionId,
                        () -> targetId);
                executionRequestBuilder.setTargetId(targetId);
            }
            executionRequestBuilder.setOriginTargetId(targetId);

            return executionRequestBuilder.build();
        }
    }

    /**
     * Creates execute action list request for sending to Topology Processor.
     *
     * @param targetId Target to execute action on.
     * @param actionList Action list to execute.
     * @return Request DTO to be sent to Topology Processor.
     * @throws ExecutionInitiationException If failed to process workflow.
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
     * @param targetId the ID of the Target which should execute the action (unless there's
     *         a                 Workflow specified - see below)
     * @param action the Action to execute
     * @param workflowOpt an Optional specifying a Workflow to override the execution of the
     *         Action
     * @throws ExecutionStartException if action execution failed to start or failed to
     *         process workflow
     */
    public void execute(final long targetId, @Nonnull final ActionDTO.ActionSpec action,
                        @Nonnull Optional<WorkflowDTO.Workflow> workflowOpt)
            throws ExecutionStartException {
        execute(targetId, Collections.singletonList(new ActionWithWorkflow(action, workflowOpt)));
    }

    /**
     * Schedule execution of the list of {@link ActionDTO.Action}s and return immediately.
     *
     * @param targetId the ID of the Target which should execute the action (unless there's
     *         a                 Workflow specified - see below)
     * @param actionList the Action list to execute
     * @throws ExecutionStartException if action execution failed to start or failed to
     *         process workflow
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
        try (Tracing.TracingScope tracingScope = getTracer(actionList)) {
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

    private Tracing.TracingScope getTracer(final List<ActionWithWorkflow> actionList) {
        final Tracing.TracingScope tracer = Tracing.trace("action_execution");
        if (actionList.size() > 0) {
            final ActionSpec firstAction = actionList.get(0).getAction();
            tracer.tag("action_state", firstAction.getActionState().name());
            tracer.tag("action_oid", firstAction.getRecommendationId());
            tracer.tag("action_id", firstAction.getRecommendation().getId());
        }
        return tracer;
    }

    /**
     * Sends an update for an in-progress action.
     *
     * @param action the action that is getting update.
     * @param state the new state of the action.
     */
    public void onActionUpdate(Action action, ActionState state) {
        final BlockingQueue queue = inProgressSyncActions.get(action.getId());
        if (queue != null) {
            queue.add(Pair.of(action, state));
        } else {
            logger.warn("Cannot find action ID {} in inProgressSyncActions: {}",
                    action.getId(), inProgressSyncActions.keySet());
        }
    }

    private static String getActionIdsString(@Nonnull final List<ActionWithWorkflow> actionList) {
        return actionList.stream()
                .map(actionWithWorkflow -> actionWithWorkflow.getAction().getRecommendation().getId())
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }
}
