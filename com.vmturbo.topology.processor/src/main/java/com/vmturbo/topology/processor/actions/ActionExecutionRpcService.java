package com.vmturbo.topology.processor.actions;

import static com.vmturbo.platform.sdk.common.util.WebhookConstants.HAS_TEMPLATE_APPLIED;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.TEMPLATED_ACTION_BODY;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionListRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionResponse;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteWorkflowRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteWorkflowResponse;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceImplBase;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.actions.data.context.AbstractActionExecutionContext;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContext;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.context.ContextCreationException;
import com.vmturbo.topology.processor.operation.ActionOperationRequest;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.workflow.WorkflowExecutionResult;

public class ActionExecutionRpcService extends ActionExecutionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Used to initiate actions.
     */
    private final IOperationManager operationManager;

    /**
     * The secure storage client.
     */
    private final SecureStorageClient secureStorageClient;

    /**
     * Constructs instances of ActionExecutionContext, an interface for collecting data needed for
     * action execution.
     */
    private final ActionExecutionContextFactory actionExecutionContextFactory;

    /**
     * Construct an ActionExecutionRpcService to respond to execute action requests.
     *
     * @param operationManager used to initiate actions.
     * @param secureStorageClient the secure storage client.
     * @param actionExecutionContextFactory builds an ActionExecutionContext, providing additional
     *                                      data required for action execution.
     */
    public ActionExecutionRpcService(@Nonnull final IOperationManager operationManager,
                                     @Nonnull final SecureStorageClient secureStorageClient,
                                     @Nonnull final ActionExecutionContextFactory actionExecutionContextFactory) {
        this.operationManager = Objects.requireNonNull(operationManager);
        this.secureStorageClient = Objects.requireNonNull(secureStorageClient);
        this.actionExecutionContextFactory = actionExecutionContextFactory;
    }

    @Override
    public void executeAction(
            @Nonnull final ExecuteActionRequest request,
            @Nonnull final StreamObserver<ExecuteActionResponse> responseObserver) {
        try {
            final ActionExecutionContext context = createActionExecutionContext(request);
            final ActionOperationRequest operationRequest = new ActionOperationRequest(
                    context.buildActionExecutionDto(),
                    context.getControlAffectedEntities());

            logger.info("Start execution of action {} after conversion to SDK actions ",
                    request.getActionId());

            operationManager.requestActions(operationRequest, request.getTargetId(),
                    context.getSecondaryTargetId());

            logger.info("ExecuteActionRequest completed for action: {}", request.getActionId());

            responseObserver.onNext(ExecuteActionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (ProbeException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage()).asException());
        } catch (InterruptedException e) {
            responseObserver.onError(Status.ABORTED
                    .withDescription(e.getMessage()).asException());
        } catch (ActionExecutionException | TargetNotFoundException | ContextCreationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (CommunicationException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void executeActionList(
            @Nonnull final ExecuteActionListRequest request,
            @Nonnull final StreamObserver<ExecuteActionResponse> responseObserver) {
        try {
            final int size = request.getActionRequestCount();
            if (size == 0) {
                throw new ActionExecutionException(
                        "Received ExecuteActionListRequest with empty action list");
            }

            final Set<Long> targetIds = request.getActionRequestList().stream()
                    .map(ExecuteActionRequest::getTargetId)
                    .collect(Collectors.toSet());
            if (targetIds.size() != 1) {
                throw new ActionExecutionException(
                        "Received different targets in ExecuteActionListRequest: " + targetIds);
            }

            final List<ActionOperationRequest> operationRequestList = new ArrayList<>(size);
            Long secondaryTargetId = null;
            boolean secondaryTargetIdAssigned = false;
            for (final ExecuteActionRequest actionRequest : request.getActionRequestList()) {
                final ActionExecutionContext context = createActionExecutionContext(actionRequest);
                operationRequestList.add(new ActionOperationRequest(
                        context.buildActionExecutionDto(),
                        context.getControlAffectedEntities()));
                final Long secondaryTargetIdTmp = context.getSecondaryTargetId();
                if (secondaryTargetIdAssigned) {
                    if (!Objects.equals(secondaryTargetId, secondaryTargetIdTmp)) {
                        throw new ActionExecutionException(String.format(
                                "Received different secondary targets in ExecuteActionListRequest: %s, %s",
                                secondaryTargetId, secondaryTargetIdTmp));
                    }
                } else {
                    secondaryTargetId = secondaryTargetIdTmp;
                    secondaryTargetIdAssigned = true;
                }
            }

            final List<Long> actionIds = request.getActionRequestList().stream()
                    .map(ExecuteActionRequest::getActionId)
                    .collect(Collectors.toList());
            logger.info("Start execution of actions {} after conversion to SDK actions ", actionIds);

            final long targetId = targetIds.iterator().next();
            operationManager.requestActions(operationRequestList, targetId, secondaryTargetId);

            logger.info("ExecuteActionListRequest completed for actions: {}", actionIds);

            responseObserver.onNext(ExecuteActionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (ProbeException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage()).asException());
        } catch (InterruptedException e) {
            responseObserver.onError(Status.ABORTED
                    .withDescription(e.getMessage()).asException());
        } catch (ActionExecutionException | TargetNotFoundException | ContextCreationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (CommunicationException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }

    private ActionExecutionContext createActionExecutionContext(
            @Nonnull final ExecuteActionRequest request)
            throws ContextCreationException, ActionExecutionException {
        final long actionId = request.getActionId();
        final long recommendationId = request.getActionSpec().getRecommendationId();

        logger.info("Action instance ID = {}; Stable Action ID = {}", actionId, recommendationId);

        // Construct a context to pull in additional data for action execution
        final ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Get the list of action items to execute
        logger.info("Convert actionId: {} to SDK actions", actionId);
        final List<ActionItemDTO> sdkActions = actionExecutionContext.getActionItems();

        // Ensure we have a sensible sdkActions result
        if (CollectionUtils.isEmpty(sdkActions)) {
            throw new ActionExecutionException(
                    "Cannot execute actionId: " + actionId + " with no action items!");
        }

        logger.info("Start execution of action {} after conversion to SDK actions ", actionId);

        return actionExecutionContext;
    }

    @Override
    public void executeWorkflow(final ExecuteWorkflowRequest request,
            final StreamObserver<ExecuteWorkflowResponse> responseObserver) {
        try {
            final Workflow workflow =
                    AbstractActionExecutionContext.buildWorkflow(request.getWorkflow(), secureStorageClient);

            ActionExecutionDTO actionExecutionDTO = ActionExecutionDTO.newBuilder()
                            .setActionType(ActionItemDTO.ActionType.NONE)
                            .addActionItem(ActionItemDTO.newBuilder()
                                    .setUuid("")
                                    .setActionType(ActionItemDTO.ActionType.NONE)
                                    .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                                            .setId("")
                                            .setEntityType(CommonDTO.EntityDTO.EntityType.UNKNOWN)
                                    )
                            )
                            .setWorkflow(workflow)
                            .build();

            logger.info("Start execution of workflow with {} ID", workflow.getId());
            WorkflowExecutionResult result = operationManager.requestWorkflow(actionExecutionDTO,
                    request.getTargetId());
            logger.info("Execute workflow request completed for ID: {}", workflow.getId());

            ExecuteWorkflowResponse response = ExecuteWorkflowResponse.newBuilder()
                    .setSucceeded(result.getSucceeded())
                    .setExecutionDetails(result.getExecutionDetails())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (ProbeException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage()).asException());
        } catch (InterruptedException e) {
            responseObserver.onError(Status.ABORTED
                    .withDescription(e.getMessage()).asException());
        } catch (TargetNotFoundException | ContextCreationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (CommunicationException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }

}
