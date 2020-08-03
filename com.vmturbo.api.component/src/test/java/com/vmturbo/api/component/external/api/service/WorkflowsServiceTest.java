package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.component.external.api.mapper.WorkflowMapper;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTOMoles.WorkflowServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Test service for fetching workflows.
 */
@RunWith(MockitoJUnitRunner.class)
public class WorkflowsServiceTest {

    // The class under test
    private WorkflowsService workflowsService;

    // Mock the dependencies
    private WorkflowServiceMole workflowServiceMole = spy(new WorkflowServiceMole());

    /**
     * The GRPC test server, to channel messages to the workflowServiceMole.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(workflowServiceMole);

    private WorkflowServiceBlockingStub workflowServiceRpc;

    private final TargetsService targetsService = mock(TargetsService.class);

    // No need to mock this one. It could easily be a static utility library.
    private final WorkflowMapper workflowMapper = new WorkflowMapper();

    /**
     * Expect no exceptions, by default.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Initial test setup.
     *
     * <p>Set up the GRPC test server to channel messages to the workflowServiceMole.
     * Instantiate the {@link WorkflowsService}, the class under test.</p>
     */
    @Before
    public void setup() {
        workflowServiceRpc = WorkflowServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        workflowsService = new WorkflowsService(workflowServiceRpc, targetsService, workflowMapper);
    }

    /**
     * Test the basic case of retrieving all workflows.
     */
    @Test
    public void testGetWorkflows() {
        // Arrange
        WorkflowDTO.FetchWorkflowsResponse workflowsResponse = FetchWorkflowsResponse.newBuilder()
            .addWorkflows(Workflow.newBuilder()
                .setId(101)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow1")
                    .setTargetId(1)))
            .addWorkflows(Workflow.newBuilder()
                .setId(102)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow2")
                    .setTargetId(1)))
            .addWorkflows(Workflow.newBuilder()
                .setId(103)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow3")
                    .setTargetId(1)))
            .build();
        doReturn(workflowsResponse).when(workflowServiceMole).fetchWorkflows(Matchers.any());

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid("1");
        targetApiDTO.setType(SDKProbeType.ACTIONSCRIPT.name());
        doReturn(Collections.singletonList(targetApiDTO)).when(targetsService).getTargets(null);

        // Act
        final List<WorkflowApiDTO> workflows = workflowsService.getWorkflows(null);

        // Assert
        assertEquals(3, workflows.size());
        assertTrue(workflows.stream()
            .map(WorkflowApiDTO::getDiscoveredBy)
            .allMatch(discoveredByTargetApiDTO -> targetApiDTO.equals(discoveredByTargetApiDTO)));
    }

    /**
     * Test the behavior when retrieving all workflows, but some of them refer to a target id
     * that no longer exists.
     */
    @Test
    public void testGetWorkflowsMissingTarget() {
        // Arrange
        WorkflowDTO.FetchWorkflowsResponse workflowsResponse = FetchWorkflowsResponse.newBuilder()
            .addWorkflows(Workflow.newBuilder()
                .setId(101)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow1")
                    .setTargetId(1)))
            .addWorkflows(Workflow.newBuilder()
                .setId(102)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow2")
                    .setTargetId(1)))
            .addWorkflows(Workflow.newBuilder()
                .setId(103)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow3")
                    .setTargetId(2)))
            .build();
        doReturn(workflowsResponse).when(workflowServiceMole).fetchWorkflows(Matchers.any());

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid("1");
        targetApiDTO.setType(SDKProbeType.ACTIONSCRIPT.name());
        doReturn(Collections.singletonList(targetApiDTO)).when(targetsService).getTargets(null);

        // Act
        final List<WorkflowApiDTO> workflows = workflowsService.getWorkflows(null);

        // Assert
        // The workflow with an invalid target should be omitted from the response.
        assertEquals(2, workflows.size());
        assertTrue(workflows.stream()
            .map(WorkflowApiDTO::getDiscoveredBy)
            .allMatch(discoveredByTargetApiDTO -> targetApiDTO.equals(discoveredByTargetApiDTO)));
    }

    /**
     * Test getting a single workflow.
     *
     * @throws Exception when an exception is thrown by the class under test
     */
    @Test
    public void testGetWorkflowByUuid() throws Exception {
        // Arrange
        final String requestedWorkflowId = "101";
        final String targetId = "1";
        final FetchWorkflowRequest fetchWorkflowRequest = FetchWorkflowRequest.newBuilder()
            .setId(Long.valueOf(requestedWorkflowId))
            .build();

        WorkflowDTO.FetchWorkflowResponse workflowResponse = FetchWorkflowResponse.newBuilder()
            .setWorkflow(Workflow.newBuilder()
                .setId(101)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow1")
                    .setTargetId(Long.valueOf(targetId))))
            .build();
        doReturn(workflowResponse).when(workflowServiceMole).fetchWorkflow(fetchWorkflowRequest);

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(targetId);
        targetApiDTO.setType(SDKProbeType.ACTIONSCRIPT.name());
        doReturn(targetApiDTO).when(targetsService).getTarget(targetId);

        // Act
        final WorkflowApiDTO workflowResult = workflowsService.getWorkflowByUuid(requestedWorkflowId);

        // Assert
        // The workflow with an invalid target should be omitted from the response.
        assertNotNull(workflowResult);
        assertEquals(requestedWorkflowId, workflowResult.getUuid());
        assertEquals(targetApiDTO, workflowResult.getDiscoveredBy());
    }

    /**
     * Test getting a single workflow, where the target associated with the workflow is missing.
     *
     * @throws Exception when an exception is thrown by the class under test
     */
    @Test
    public void testGetWorkflowByUuidMissingTarget() throws Exception {
        // Arrange
        final String requestedWorkflowId = "101";
        final String targetId = "1";
        final FetchWorkflowRequest fetchWorkflowRequest = FetchWorkflowRequest.newBuilder()
            .setId(Long.valueOf(requestedWorkflowId))
            .build();

        WorkflowDTO.FetchWorkflowResponse workflowResponse = FetchWorkflowResponse.newBuilder()
            .setWorkflow(Workflow.newBuilder()
                .setId(101)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setName("Workflow1")
                    .setTargetId(Long.valueOf(targetId))))
            .build();
        doReturn(workflowResponse).when(workflowServiceMole).fetchWorkflow(fetchWorkflowRequest);

        // Simulate a target not being found
        doThrow(new UnknownObjectException("Target not found.")).when(targetsService).getTarget(targetId);

        // Act
        thrown.expect(UnknownObjectException.class);
        workflowsService.getWorkflowByUuid(requestedWorkflowId);

        // Assert
        fail("Unknown object exception should have been thrown.");
    }
}
