package com.vmturbo.topology.processor.actions;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionResponse;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceImplBase;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContext;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.context.ContextCreationException;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

public class ActionExecutionRpcService extends ActionExecutionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Used to initiate actions
     */
    private final IOperationManager operationManager;

    /**
     * Constructs instances of ActionExecutionContext, an interface for collecting data needed for
     * action execution.
     */
    private final ActionExecutionContextFactory actionExecutionContextFactory;

    /**
     * Construct an ActionExecutionRpcService to respond to execute action requests
     *
     * @param operationManager used to initiate actions
     * @param actionExecutionContextFactory builds an ActionExecutionContext, providing additional
     *                                      data required for action execution
     */
    public ActionExecutionRpcService(@Nonnull final IOperationManager operationManager,
                                     @Nonnull final ActionExecutionContextFactory actionExecutionContextFactory) {
        this.operationManager = Objects.requireNonNull(operationManager);
        this.actionExecutionContextFactory = actionExecutionContextFactory;
    }

    @Override
    public void executeAction(ExecuteActionRequest request,
                    StreamObserver<ExecuteActionResponse> responseObserver) {
        final long actionId = request.getActionId();
        logger.info("ExecuteActionRequest received. ActionId: {}", actionId);
        try {
            // Construct a context to pull in additional data for action execution
            ActionExecutionContext actionExecutionContext =
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
            operationManager.requestActions(actionExecutionContext.buildActionExecutionDto(),
                    request.getTargetId(),
                    actionExecutionContext.getSecondaryTargetId(),
                    actionExecutionContext.getControlAffectedEntities());

            logger.info("ExecuteActionRequest completed for action: {}", actionId);
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
}
