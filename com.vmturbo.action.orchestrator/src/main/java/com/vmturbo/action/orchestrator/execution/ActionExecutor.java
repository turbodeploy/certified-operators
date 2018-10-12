package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

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
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Executes actions by converting {@link ActionDTO.Action} objects into {@link ExecuteActionRequest}
 * and sending them to the {@link TopologyProcessor}.
 */
public class ActionExecutor implements ActionExecutionListener {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A client for making remote calls to the Topology Processor service to execute actions
     */
    private final ActionExecutionServiceBlockingStub actionExecutionService;

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
     */
    public ActionExecutor(@Nonnull final Channel topologyProcessorChannel) {
        this.actionExecutionService = ActionExecutionServiceGrpc
                .newBlockingStub(Objects.requireNonNull(topologyProcessorChannel));
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
