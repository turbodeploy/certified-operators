package com.vmturbo.topology.processor.workflow;

import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.EXPECTED_WORKFLOW_DTOS;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.NME_WITH_WORKFLOWS;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.TARGET_ID;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc.DiscoveredWorkflowServiceImplBase;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsResponse;
import com.vmturbo.components.api.test.GrpcTestServer;

public class DiscoveredWorkflowUploaderTest {

    private DiscoveredWorkflowUploader recorderSpy;

    private final TestDiscoveredWorkflowService uploadServiceSpy =
            spy(new TestDiscoveredWorkflowService());

    public DiscoveredWorkflowInterpreter converter;

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(uploadServiceSpy);

    @Before
    public void setup() {
        converter = new DiscoveredWorkflowInterpreter();
        recorderSpy = spy(new DiscoveredWorkflowUploader(server.getChannel(), converter));
    }

    @Test
    public void setTargetWorkflows() {
    }

    @Test
    public void targetRemoved() {
        // arrange
        recorderSpy.setTargetWorkflows(TARGET_ID, NME_WITH_WORKFLOWS);
        // After removing a target, an empty workflow list should be uploaded for that target
        StoreDiscoveredWorkflowsRequest expectedRequest = StoreDiscoveredWorkflowsRequest
            .newBuilder()
            .setTargetId(TARGET_ID)
            .addAllDiscoveredWorkflow(Collections.emptyList())
            .build();
        // act
        recorderSpy.targetRemoved(TARGET_ID);
        // assert
        verify(uploadServiceSpy).storeDiscoveredWorkflows(eq(expectedRequest), any());
    }

    @Test
    public void uploadDiscoveredWorkflows() {
        // arrange
        recorderSpy.setTargetWorkflows(TARGET_ID, NME_WITH_WORKFLOWS);
        StoreDiscoveredWorkflowsRequest expectedRequest = StoreDiscoveredWorkflowsRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .addAllDiscoveredWorkflow(EXPECTED_WORKFLOW_DTOS)
                .build();
        // act
        recorderSpy.uploadDiscoveredWorkflows();
        // assert
        verify(uploadServiceSpy).storeDiscoveredWorkflows(eq(expectedRequest), any());
    }


    public static class TestDiscoveredWorkflowService extends DiscoveredWorkflowServiceImplBase {

        private boolean error = false;

        public void enableError() {
            error = true;
        }

        @Override
        public void storeDiscoveredWorkflows(StoreDiscoveredWorkflowsRequest request,
                                          StreamObserver<StoreDiscoveredWorkflowsResponse> responseObserver) {
            if (error) {
                responseObserver.onError(Status.INTERNAL.asException());
            } else {
                responseObserver.onNext(StoreDiscoveredWorkflowsResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }
    }
}