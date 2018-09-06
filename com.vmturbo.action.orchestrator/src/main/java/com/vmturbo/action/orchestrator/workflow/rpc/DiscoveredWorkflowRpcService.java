package com.vmturbo.action.orchestrator.workflow.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc.DiscoveredWorkflowServiceImplBase;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsResponse;

/**
 * Implements the RPC calls supported by the action orchestrator for storing and retrieving workflows.
 */
public class DiscoveredWorkflowRpcService extends DiscoveredWorkflowServiceImplBase {

    // the store for the workflows that are uploaded; it may or may not implement persistence.
    private final WorkflowStore workflowStore;

    private static final Logger logger = LogManager.getLogger();

    public DiscoveredWorkflowRpcService(WorkflowStore workflowStore) {
        this.workflowStore = workflowStore;
    }

    @Override
    public void storeDiscoveredWorkflows(StoreDiscoveredWorkflowsRequest request,
                                         StreamObserver<StoreDiscoveredWorkflowsResponse> responseObserver) {
        logger.info("store discovered workflows for target {}, count: {}",
                request.getTargetId(),
                request.getDiscoveredWorkflowList().size());
        try {
            workflowStore.persistWorkflows(request.getTargetId(),request.getDiscoveredWorkflowList());
            responseObserver.onNext(WorkflowDTO.StoreDiscoveredWorkflowsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("exception persisting workflows: ", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }
}
