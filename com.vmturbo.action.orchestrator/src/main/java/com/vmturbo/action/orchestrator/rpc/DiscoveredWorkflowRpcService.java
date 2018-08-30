package com.vmturbo.action.orchestrator.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc.DiscoveredWorkflowServiceImplBase;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsResponse;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving and executing actions.
 */
public class DiscoveredWorkflowRpcService extends DiscoveredWorkflowServiceImplBase {


    private static final Logger logger = LogManager.getLogger();

    @Override
    public void storeDiscoveredWorkflows(StoreDiscoveredWorkflowsRequest request,
                                         StreamObserver<StoreDiscoveredWorkflowsResponse> responseObserver) {
        // TODO: remove the log message from this code; this is a stubExplanationComposer to be replaced in OM-37222
        logger.info("store discovered workflows, count: {}", request.getDiscoveredWorkflowList().size());
        responseObserver.onNext(WorkflowDTO.StoreDiscoveredWorkflowsResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
