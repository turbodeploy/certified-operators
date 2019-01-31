package com.vmturbo.action.orchestrator.workflow.rpc;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;

/**
 * gRPC Service for fetching Workflow items from the persistent store.
 **/
public class WorkflowRpcService extends WorkflowServiceGrpc.WorkflowServiceImplBase  {

    public static final String WORKFLOW_NOT_FOUND_MSG = "Workflow with id: %d not found.";
    // the store for the workflows that are uploaded
    private final WorkflowStore workflowStore;

    private static final Logger logger = LogManager.getLogger();

    public WorkflowRpcService(@Nonnull WorkflowStore workflowStore) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Return a FetchWorkflowResponse with the 'workflow' field set to the corresponding Workflow.
     * If the workflow with the given id is not found, then the 'workflow' field will be empty.
     */
    @Override
    public void fetchWorkflow(WorkflowDTO.FetchWorkflowRequest workflowRequest,
                              StreamObserver<FetchWorkflowResponse> responseObserver) {
        try {
            final Optional<WorkflowDTO.Workflow> workflowResult =
                    workflowStore.fetchWorkflow(workflowRequest.getId());
            FetchWorkflowResponse.Builder responseBuilder = FetchWorkflowResponse.newBuilder();
            workflowResult.ifPresent(responseBuilder::setWorkflow);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("exception fetching workflow: " + workflowRequest.getId(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void fetchWorkflows(FetchWorkflowsRequest request,
                               StreamObserver<FetchWorkflowsResponse> responseObserver) {
        try {
            // filter on the OrchestratorType - currently: UCSD, ACTION_SCRIPT or 'null'
            WorkflowDTO.OrchestratorType orchestratorTypeFilter = request.hasOrchestratorType() ?
                    request.getOrchestratorType() :
                    null;
            FetchWorkflowsResponse.Builder response = FetchWorkflowsResponse.newBuilder();
            response.addAllWorkflows(workflowStore.fetchWorkflows(orchestratorTypeFilter));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("exception fetching workflows: ", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

    }
}
