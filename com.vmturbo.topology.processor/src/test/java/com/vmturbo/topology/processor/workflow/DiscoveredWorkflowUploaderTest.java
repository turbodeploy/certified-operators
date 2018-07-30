package com.vmturbo.topology.processor.workflow;

import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.EXPECTED_WORKFLOW_DTOS;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.NME_WITH_WORKFLOWS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;


import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc.DiscoveredWorkflowServiceImplBase;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsResponse;
import com.vmturbo.components.api.test.GrpcTestServer;

public class DiscoveredWorkflowUploaderTest {

    private static final long TARGET_ID = 1234L;

    private DiscoveredWorkflowUploader recorderSpy;

    private final TestDiscoveredWorkflowService uploadServiceSpy =
            spy(new TestDiscoveredWorkflowService());

    private final DiscoveredWorkflowInterpreter converter = mock(DiscoveredWorkflowInterpreter.class);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(uploadServiceSpy);

    @Before
    public void setup() {
        recorderSpy = spy(new DiscoveredWorkflowUploader(server.getChannel(), converter));
    }

    @Test
    public void setTargetWorkflows() {
    }

    @Test
    public void targetRemoved() {
    }

    @Test
    public void uploadDiscoveredWorkflows() {
        // arrange
        recorderSpy.setTargetWorkflows(TARGET_ID, NME_WITH_WORKFLOWS);
        StoreDiscoveredWorkflowsRequest expectedRequest = StoreDiscoveredWorkflowsRequest.newBuilder()
                .build();
        when(converter.interpretWorkflowList(any(), anyLong())).thenReturn(EXPECTED_WORKFLOW_DTOS);
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