package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.component.external.api.mapper.WorkflowMapper;
import com.vmturbo.api.component.external.api.mapper.WorkflowMapperTest;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.target.TargetDetailLevel;
import com.vmturbo.api.dto.workflow.AuthenticationMethod;
import com.vmturbo.api.dto.workflow.OAuthDataApiDTO;
import com.vmturbo.api.dto.workflow.OAuthGrantType;
import com.vmturbo.api.dto.workflow.RequestHeader;
import com.vmturbo.api.dto.workflow.WebhookApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowOperation;
import com.vmturbo.api.dto.workflow.WorkflowOperationRequestApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowOperationResponseApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTOMoles.WorkflowServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Test service for fetching workflows.
 */
@RunWith(MockitoJUnitRunner.class)
public class WorkflowsServiceTest {
    private static final long ACTION_ID = 222L;
    private static final String TEST_EXECUTION_DETAILS = "TEST_EXECUTION_DETAILS";

    // The class under test
    private WorkflowsService workflowsService;

    // Mock the dependencies
    private WorkflowServiceMole workflowServiceMole = spy(new WorkflowServiceMole());

    private SettingPolicyServiceMole settingPolicyServiceMole = spy(new SettingPolicyServiceMole());

    /**
     * The GRPC test server, to channel messages to the workflowServiceMole.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(workflowServiceMole,
        settingPolicyServiceMole);

    private WorkflowServiceBlockingStub workflowServiceRpc;

    private SettingPolicyServiceBlockingStub settingPolicyServiceRpc;

    private final TargetsService targetsService = mock(TargetsService.class);

    // No need to mock this one. It could easily be a static utility library.
    private final WorkflowMapper workflowMapper = new WorkflowMapper();

    private SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);

    private ActionsService actionsService = mock(ActionsService.class);

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
        settingPolicyServiceRpc = SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        workflowsService = new WorkflowsService(workflowServiceRpc, targetsService, workflowMapper,
            settingPolicyServiceRpc, secureStorageClient, actionsService);
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
        doReturn(workflowsResponse).when(workflowServiceMole).fetchWorkflows(any());

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid("1");
        targetApiDTO.setType(SDKProbeType.ACTIONSCRIPT.name());
        doReturn(Collections.singletonList(targetApiDTO)).when(targetsService).getTargets();

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
        doReturn(workflowsResponse).when(workflowServiceMole).fetchWorkflows(any());

        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid("1");
        targetApiDTO.setType(SDKProbeType.ACTIONSCRIPT.name());
        doReturn(Collections.singletonList(targetApiDTO)).when(targetsService).getTargets();

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
        doReturn(targetApiDTO).when(targetsService)
                .getTarget(targetId, TargetDetailLevel.BASIC);

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
        doThrow(new UnknownObjectException("Target not found.")).when(targetsService)
                .getTarget(targetId, TargetDetailLevel.BASIC);

        // Act
        thrown.expect(UnknownObjectException.class);
        workflowsService.getWorkflowByUuid(requestedWorkflowId);

        // Assert
        fail("Unknown object exception should have been thrown.");
    }

    /**
     * Tests the call for creation of webhook workflow.
     *
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test
    public void testCreatingWebhookWorkflow() throws InvalidOperationException {
        // ARRANGE
        final WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        ArgumentCaptor<WorkflowDTO.CreateWorkflowRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(WorkflowDTO.CreateWorkflowRequest.class);
        when(workflowServiceMole.createWorkflow(requestArgumentCaptor.capture()))
            .thenReturn(WorkflowDTO.CreateWorkflowResponse.newBuilder()
                .setWorkflow(WorkflowMapperTest.createWebhookWorkflow())
                .build());

        // ACT
        final WorkflowApiDTO createdWorkflow = workflowsService.addWorkflow(workflowApiDTO);

        // ASSERT
        WorkflowMapperTest.verifyWebhookWorkflowEquality(createdWorkflow, workflowApiDTO);
        // the name is random, so let's get rid of the name in the object
        Workflow requestWorkflow = requestArgumentCaptor.getValue().getWorkflow()
                .toBuilder()
                .setWorkflowInfo(requestArgumentCaptor.getValue().getWorkflow().getWorkflowInfo().toBuilder().clearName())
                .build();
        assertThat(requestWorkflow,
            equalTo(WorkflowMapperTest.createWebhookWorkflow(false)));
    }

    /**
     * Tests creating actionscript workflow. This should fail as creating actionscript
     * workflow should not be possible from API.
     */
    @Test
    public void testCreatingActionscriptWorkflow() {
        // ARRANGE
        WorkflowApiDTO actionscriptWorkflowApiDto = WorkflowMapperTest.createActionscriptApiWorkflow();

        try {
            // ACT
            workflowsService.addWorkflow(actionscriptWorkflowApiDto);
            fail("The creation of actionscript workflow must not be possible");
        } catch (InvalidOperationException exception) {
            // ASSERT
            verify(workflowServiceMole, times(0)).createWorkflow(any());
        }
    }

    /**
     * Test editing of workflow.
     *
     * @throws UnknownObjectException if something goes wrong.
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test
    public void testUpdateWebhookWorkflow() throws UnknownObjectException, InvalidOperationException {
        // ARRANGE
        final String updatedName = "Updated display name";
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
                .setId(WorkflowMapperTest.WORKFLOW_OID)
                .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
              .setWorkflow(WorkflowMapperTest.createWebhookWorkflow())
              .build());

        Workflow updatedWorkflow = WorkflowMapperTest.createWebhookWorkflow();
        updatedWorkflow = updatedWorkflow.toBuilder()
            .setWorkflowInfo(updatedWorkflow.getWorkflowInfo().toBuilder().setDisplayName(updatedName))
            .build();
        ArgumentCaptor<WorkflowDTO.UpdateWorkflowRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(WorkflowDTO.UpdateWorkflowRequest.class);
        when(workflowServiceMole.updateWorkflow(requestArgumentCaptor.capture()))
            .thenReturn(WorkflowDTO.UpdateWorkflowResponse.newBuilder()
                .setWorkflow(updatedWorkflow)
                .build());

        WorkflowApiDTO updatedApiWorkflow = WorkflowMapperTest.createWebhookWorkflowApiDto();
        updatedApiWorkflow.setDisplayName(updatedName);

        // ACT
        WorkflowApiDTO returnedApiWorkflow = workflowsService
            .editWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), updatedApiWorkflow);

        // ASSERT
        assertThat(returnedApiWorkflow.getDisplayName(), equalTo(updatedName));
        WorkflowDTO.UpdateWorkflowRequest request = requestArgumentCaptor.getValue();
        assertThat(request.getId(), equalTo(WorkflowMapperTest.WORKFLOW_OID));
        assertThat(request.getWorkflow().getWorkflowInfo().getDisplayName(), equalTo(updatedName));
    }

    /**
     * Test the case that a actionscript workflow being edited. That should not be possible.
     *
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testUpdatingActionscriptWorkflow() throws UnknownObjectException {
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .setWorkflow(WorkflowMapperTest.createActionscriptWorkflow())
                .build());

        WorkflowApiDTO updatedApiWorkflow = WorkflowMapperTest.createActionscriptApiWorkflow();

        try {
            // ACT
            workflowsService
                .editWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), updatedApiWorkflow);
            fail("The editing of actionscript workflow must not be possible");
        } catch (InvalidOperationException exception) {
            // ASSERT
            verify(workflowServiceMole, times(0)).updateWorkflow(any());
        }
    }

    /**
     * Test the case the user tries to change the type of workflow through API.
     *
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testUpdatingWebhookWorkflowToActionscript() throws UnknownObjectException {
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .setWorkflow(WorkflowMapperTest.createWebhookWorkflow())
                .build());

        WorkflowApiDTO updatedApiWorkflow = WorkflowMapperTest.createActionscriptApiWorkflow();

        try {
            // ACT
            workflowsService
                .editWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), updatedApiWorkflow);
            fail("The change of type of workflow must not be possible");
        } catch (InvalidOperationException exception) {
            // ASSERT
            verify(workflowServiceMole, times(0)).updateWorkflow(any());
        }
    }

    /**
     * Tests the case that we are editing a workflow that does not exist.
     *
     * @throws UnknownObjectException when there is no such workflow (expected to happen).
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test(expected = UnknownObjectException.class)
    public void testUpdatingNonExistentWorkflow() throws UnknownObjectException, InvalidOperationException {
        // ARRANGE
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .build());
        WorkflowApiDTO updatedApiWorkflow = WorkflowMapperTest.createWebhookWorkflowApiDto();

        // ACT
        workflowsService
            .editWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), updatedApiWorkflow);
    }

    /**
     * Tests the case that deleting workflow executes successfully.
     *
     * @throws UnknownObjectException if something goes wrong.
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test
    public void testDeletingWebhookWorkflow() throws UnknownObjectException, InvalidOperationException {
        // ARRANGE
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .setWorkflow(WorkflowMapperTest.createWebhookWorkflow())
                .build());

        when(settingPolicyServiceMole.listSettingPolicies(SettingProto.ListSettingPoliciesRequest
            .newBuilder()
            .addWorkflowId(WorkflowMapperTest.WORKFLOW_OID)
            .build())).thenReturn(Collections.emptyList());

        ArgumentCaptor<WorkflowDTO.DeleteWorkflowRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(WorkflowDTO.DeleteWorkflowRequest.class);
        when(workflowServiceMole.deleteWorkflow(requestArgumentCaptor.capture()))
            .thenReturn(WorkflowDTO.DeleteWorkflowResponse.getDefaultInstance());

        // ACT
        workflowsService.deleteWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID));

        // ASSERT
        assertThat(requestArgumentCaptor.getValue().getId(), equalTo(WorkflowMapperTest.WORKFLOW_OID));
    }

    /**
     * Tests the case that we are deleting a workflow that does not exist.
     *
     * @throws UnknownObjectException when there is no such workflow (expected to happen).
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test(expected = UnknownObjectException.class)
    public void testDeletingNonExistentWorkflow() throws UnknownObjectException, InvalidOperationException {
        // ARRANGE
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .build());

        when(settingPolicyServiceMole.listSettingPolicies(SettingProto.ListSettingPoliciesRequest
            .newBuilder()
            .addWorkflowId(WorkflowMapperTest.WORKFLOW_OID)
            .build())).thenReturn(Collections.emptyList());

        // ACT
        workflowsService
            .deleteWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID));
    }

    /**
     * Test the case that a actionscript workflow being deleted. That should not be possible.
     *
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testDeletingActionscriptWorkflow() throws UnknownObjectException {
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .setWorkflow(WorkflowMapperTest.createActionscriptWorkflow())
                .build());

        when(settingPolicyServiceMole.listSettingPolicies(SettingProto.ListSettingPoliciesRequest
            .newBuilder()
            .addWorkflowId(WorkflowMapperTest.WORKFLOW_OID)
            .build())).thenReturn(Collections.emptyList());

        try {
            // ACT
            workflowsService
                .deleteWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID));
            fail("The deleting of actionscript workflow must not be possible");
        } catch (InvalidOperationException exception) {
            // ASSERT
            verify(workflowServiceMole, times(0)).deleteWorkflow(any());
        }
    }

    /**
     * Test the case for deleting workflow with associated policies.
     *
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testDeletingActionscriptUsedInPolicies() throws UnknownObjectException {
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
            .setId(WorkflowMapperTest.WORKFLOW_OID)
            .build()))
            .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                .setWorkflow(WorkflowMapperTest.createWebhookWorkflow())
                .build());

        when(settingPolicyServiceMole.listSettingPolicies(SettingProto.ListSettingPoliciesRequest
            .newBuilder()
            .addWorkflowId(WorkflowMapperTest.WORKFLOW_OID)
            .build())).thenReturn(Collections.singletonList(SettingProto.SettingPolicy.newBuilder()
              .setId(123L)
              .setInfo(SettingProto.SettingPolicyInfo.newBuilder().setDisplayName("XYZA")
              .build()).build()));

        try {
            // ACT
            workflowsService
                .deleteWorkflow(String.valueOf(WorkflowMapperTest.WORKFLOW_OID));
            fail("The deleting of actionscript workflow must not be possible");
        } catch (InvalidOperationException exception) {
            // ASSERT
            assertThat(exception.getMessage(), containsString("XYZA"));
            verify(workflowServiceMole, times(0)).deleteWorkflow(any());
        }
    }

    /**
     * Test successful and unsuccessful validation of webhook workflow.
     */
    @Test
    public void testValidateWorkflow() {
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();

        // successful validation
        workflowsService.validateInput(workflowApiDTO);

        workflowApiDTO.setDisplayName("");
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // Test script path is empty for webhook workflow
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        workflowApiDTO.setScriptPath("test");
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // Test parameter is empty for webhook workflow
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        workflowApiDTO.setParameters(Collections.singletonList(new InputFieldApiDTO()));
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // checks if webhook details has been set
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        workflowApiDTO.setTypeSpecificDetails(null);
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test validating url
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        ((WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails()).setUrl("test");
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test validating username for BASIC auth (cannot be empty)
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setUsername("");
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test validating username for BASIC auth (cannot contain colon)
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setUsername("abc:123");
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test 'addWorkFlow' password validation (cannot be empty)
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setPassword("");
        try {
            workflowsService.addWorkflow(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException | InvalidOperationException e) {
        }

        // test header name are not in ASCII.
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setHeaders(
                Collections.singletonList(new RequestHeader("⊗ invalid header ⊗", "header_value")));
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test header value are not in ASCII.
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setHeaders(
                Collections.singletonList(new RequestHeader("header_name", "⊗ header_value ⊗")));
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test header name is empty (or has only whitespaces)
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setHeaders(
                Collections.singletonList(new RequestHeader("", "header_value")));
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test header name wasn't provided or was null
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setHeaders(
                Collections.singletonList(new RequestHeader(null, "header_value")));
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }

        // test header value wasn't provided or was null
        workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setHeaders(
                Collections.singletonList(new RequestHeader("header_name", null)));
        try {
            workflowsService.validateInput(workflowApiDTO);
            fail();
        } catch (IllegalArgumentException ex) {
        }
    }

    /**
     * Test correct args are passed to secureStorageClient when adding workflow.
     *
     * @throws CommunicationException if something goes wrong.
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test
    public void testAddingWorkflowWithBasicAuthUsingSecureStorage()
            throws CommunicationException, InvalidOperationException {
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setUsername("abc");
        webhookApiDTO.setPassword("123");
        workflowsService.addWorkflow(workflowApiDTO);
        ArgumentCaptor<String> subject = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> password = ArgumentCaptor.forClass(String.class);
        verify(secureStorageClient).updateValue(subject.capture(), Mockito.any(),
                password.capture());
        assertEquals(StringConstants.WEBHOOK_PASSWORD_SUBJECT, subject.getValue());
        assertEquals(webhookApiDTO.getPassword(), password.getValue());
    }

    /**
     * Test correct args are passed to secureStorageClient when deleting workflow.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws CommunicationException if something goes wrong.
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testDeletingWorkflowWithBasicAuthUsingSecureStorage()
            throws InvalidOperationException, CommunicationException, UnknownObjectException {
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setUsername("abc");
        webhookApiDTO.setPassword("123");
        long id = 0L;
        FetchWorkflowResponse fetchWorkflowResponse =
                FetchWorkflowResponse.newBuilder().setWorkflow(
                        workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO, "test")).build();
        when(workflowServiceMole.fetchWorkflow(any())).thenReturn(fetchWorkflowResponse);
        workflowsService.addWorkflow(workflowApiDTO);
        workflowsService.deleteWorkflow(Long.toString(id));
        ArgumentCaptor<String> subject = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> idToDelete = ArgumentCaptor.forClass(String.class);
        verify(secureStorageClient).deleteValue(subject.capture(), idToDelete.capture());
        assertEquals(StringConstants.WEBHOOK_PASSWORD_SUBJECT, subject.getValue());
        assertEquals(idToDelete.getValue(), Long.toString(id));
    }

    /**
     * Test correct args are passed to secureStorageClient when editing workflow.
     *
     * @throws UnknownObjectException if something goes wrong.
     * @throws InvalidOperationException if something goes wrong.
     * @throws CommunicationException if something goes wrong.
     */
    @Test
    public void testEditingWorkflowWithBasicAuthUsingSecureStorage()
            throws UnknownObjectException, InvalidOperationException, CommunicationException {
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setUsername("abc");
        webhookApiDTO.setPassword("123");
        FetchWorkflowResponse fetchWorkflowResponse =
                FetchWorkflowResponse.newBuilder().setWorkflow(
                        workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO, "test")).build();
        when(workflowServiceMole.fetchWorkflow(any())).thenReturn(fetchWorkflowResponse);
        workflowsService.editWorkflow(Long.toString(0L), workflowApiDTO);
        ArgumentCaptor<String> subject = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> password = ArgumentCaptor.forClass(String.class);
        verify(secureStorageClient).updateValue(subject.capture(), Mockito.any(),
                password.capture());
        assertEquals(StringConstants.WEBHOOK_PASSWORD_SUBJECT, subject.getValue());
        assertEquals(webhookApiDTO.getPassword(), password.getValue());
    }

    /**
     * Test reverting workflow upon failed password update using secure storage.
     *
     * @throws UnknownObjectException if something goes wrong.
     * @throws InvalidOperationException if something goes wrong.
     * @throws CommunicationException if something goes wrong.
     */
    @Test
    public void testRevertWorkflowOnFailedPasswordUpdateUsingSecureStorage()
            throws UnknownObjectException, InvalidOperationException, CommunicationException {
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.BASIC);
        webhookApiDTO.setUsername("abc");
        webhookApiDTO.setPassword("123");
        FetchWorkflowResponse fetchWorkflowResponse =
                FetchWorkflowResponse.newBuilder().setWorkflow(
                        workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO, "test")).build();
        when(workflowServiceMole.fetchWorkflow(any())).thenReturn(fetchWorkflowResponse);
        doThrow(new CommunicationException("")).when(secureStorageClient).updateValue(Mockito.any(),
                Mockito.any(), Mockito.any());
        try {
            workflowsService.editWorkflow(Long.toString(0L), workflowApiDTO);
        } catch (IllegalStateException e) {
            // first time to update work flow, second time to revert it.
            verify(workflowServiceMole, times(2)).updateWorkflow(any());
        }
    }

    /**
     * Tests the call for creation of webhook workflow using oAuth.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws CommunicationException if something goes wrong.
     */
    @Test
    public void testCreatingWebhookUsingOAuth()
            throws InvalidOperationException, CommunicationException {
        // ARRANGE
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.OAUTH);
        OAuthDataApiDTO oAuthData = new OAuthDataApiDTO("123", "secretPassword", "testURL", null,
                OAuthGrantType.CLIENT_CREDENTIALS);
        webhookApiDTO.setOAuthData(oAuthData);
        // ACT
        workflowsService.addWorkflow(workflowApiDTO);
        // ASSERT
        ArgumentCaptor<String> subject = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> password = ArgumentCaptor.forClass(String.class);
        verify(secureStorageClient).updateValue(subject.capture(), Mockito.any(),
                password.capture());
        assertEquals(StringConstants.WEBHOOK_OAUTH_CLIENT_SECRET_SUBJECT, subject.getValue());
        assertEquals(webhookApiDTO.getOAuthData().getClientSecret(), password.getValue());
    }

    /**
     * Tests the call for creation of webhook workflow and assert that it fails when oAuth is
     * selected and the client secret is missing.
     *
     * @throws InvalidOperationException if something goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingWebhookUsingOAuthMissingClientSecret() throws InvalidOperationException {
        // ARRANGE
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.OAUTH);
        OAuthDataApiDTO oAuthData = new OAuthDataApiDTO("123", null, "testURL", null,
                OAuthGrantType.CLIENT_CREDENTIALS);
        webhookApiDTO.setOAuthData(oAuthData);
        // ACT
        workflowsService.addWorkflow(workflowApiDTO);
    }

    /**
     * Tests the call for editing a webhook workflow using oAuth.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws CommunicationException if something goes wrong.
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testEditWebhookUsingOAuth()
            throws InvalidOperationException, CommunicationException, UnknownObjectException {
        // ARRANGE
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.OAUTH);
        OAuthDataApiDTO oAuthData = new OAuthDataApiDTO("123", "secretPassword", "testURL", null,
                OAuthGrantType.CLIENT_CREDENTIALS);
        webhookApiDTO.setOAuthData(oAuthData);
        // ACT
        FetchWorkflowResponse fetchWorkflowResponse =
                FetchWorkflowResponse.newBuilder().setWorkflow(
                        workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO, "test")).build();
        when(workflowServiceMole.fetchWorkflow(any())).thenReturn(fetchWorkflowResponse);
        workflowsService.editWorkflow(Long.toString(0L), workflowApiDTO);
        // ASSERT
        ArgumentCaptor<String> subject = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> password = ArgumentCaptor.forClass(String.class);
        verify(secureStorageClient).updateValue(subject.capture(), Mockito.any(),
                password.capture());
        assertEquals(StringConstants.WEBHOOK_OAUTH_CLIENT_SECRET_SUBJECT, subject.getValue());
        assertEquals(webhookApiDTO.getOAuthData().getClientSecret(), password.getValue());
    }

    /**
     * Tests the call for creation of webhook workflow and check to ensure that the existing
     * secret is not overwritten when oAuth is selected and a new secret is not provided as part
     * of an edit.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws UnknownObjectException if something goes wrong.
     */
    @Test
    public void testEditWebhookUsingOAuthWithoutClientSecret()
            throws InvalidOperationException, UnknownObjectException {
        // ARRANGE
        WorkflowApiDTO workflowApiDTO = WorkflowMapperTest.createWebhookWorkflowApiDto();
        WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
        webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.OAUTH);
        OAuthDataApiDTO oAuthData = new OAuthDataApiDTO("123", null, "testURL", null,
                OAuthGrantType.CLIENT_CREDENTIALS);
        webhookApiDTO.setOAuthData(oAuthData);
        // ACT
        FetchWorkflowResponse fetchWorkflowResponse =
                FetchWorkflowResponse.newBuilder().setWorkflow(
                        workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO, "test")).build();
        when(workflowServiceMole.fetchWorkflow(any())).thenReturn(fetchWorkflowResponse);
        workflowsService.editWorkflow(Long.toString(0L), workflowApiDTO);
        // VERIFY
        // The secure storage client must not be called when the client secret is unchanged!
        verifyNoMoreInteractions(secureStorageClient);
    }

    /**
     * Test successful call to execute workflow service.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testSuccessfulWorkflowExecution() throws Exception {
        // ARRANGE
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
                .setId(WorkflowMapperTest.WORKFLOW_OID)
                .build()))
                .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                        .setWorkflow(WorkflowMapperTest.createWebhookWorkflow())
                        .build());

        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        actionApiDTO.setUuid(String.valueOf(ACTION_ID));

        when(actionsService.getActionByUuid(String.valueOf(ACTION_ID), null))
                .thenReturn(actionApiDTO);

        ArgumentCaptor<WorkflowDTO.ExecuteWorkflowRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(WorkflowDTO.ExecuteWorkflowRequest.class);
        when(workflowServiceMole.executeWorkflow(requestArgumentCaptor.capture()))
                .thenReturn(WorkflowDTO.ExecuteWorkflowResponse.newBuilder()
                        .setSucceeded(true)
                        .setExecutionDetails(TEST_EXECUTION_DETAILS)
                        .build());

        // ACT
        WorkflowOperationRequestApiDTO tryOutWorkflowApiDTO = new WorkflowOperationRequestApiDTO();
        tryOutWorkflowApiDTO.setActionId(ACTION_ID);
        tryOutWorkflowApiDTO.setOperation(WorkflowOperation.TEST);
        final WorkflowOperationResponseApiDTO executionResult =
                workflowsService.performOperation(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), tryOutWorkflowApiDTO);

        // ASSERT
        assertTrue(executionResult.getSucceeded());
        assertEquals(TEST_EXECUTION_DETAILS, executionResult.getDetails());
        assertEquals("{\"uuid\":\"222\"}", requestArgumentCaptor.getValue().getActionApiDTO());
        assertEquals(WorkflowMapperTest.WORKFLOW_OID, requestArgumentCaptor.getValue().getWorkflowId());
    }

    /**
     * Test call to execute workflow service when the workflow does not exist.
     *
     * @throws Exception if something goes wrong.
     */
    @Test(expected = UnknownObjectException.class)
    public void testWorkflowExecutionWorkflowNotFound() throws Exception {
        // ARRANGE
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
                .setId(WorkflowMapperTest.WORKFLOW_OID)
                .build()))
                .thenReturn(WorkflowDTO.FetchWorkflowResponse.getDefaultInstance());

        // ACT
        WorkflowOperationRequestApiDTO tryOutWorkflowApiDTO = new WorkflowOperationRequestApiDTO();
        tryOutWorkflowApiDTO.setActionId(ACTION_ID);
        tryOutWorkflowApiDTO.setOperation(WorkflowOperation.TEST);
        workflowsService.performOperation(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), tryOutWorkflowApiDTO);
    }

    /**
     * test call to execute workflow service when the workflow is not webhook.
     *
     * @throws Exception if something goes wrong.
     */
    @Test(expected = InvalidOperationException.class)
    public void testWorkflowExecutionNonWebhook() throws Exception {
        // ARRANGE
        when(workflowServiceMole.fetchWorkflow(WorkflowDTO.FetchWorkflowRequest.newBuilder()
                .setId(WorkflowMapperTest.WORKFLOW_OID)
                .build()))
                .thenReturn(WorkflowDTO.FetchWorkflowResponse.newBuilder()
                        .setWorkflow(WorkflowMapperTest.createActionscriptWorkflow())
                        .build());


        // ACT
        WorkflowOperationRequestApiDTO tryOutWorkflowApiDTO = new WorkflowOperationRequestApiDTO();
        tryOutWorkflowApiDTO.setActionId(ACTION_ID);
        tryOutWorkflowApiDTO.setOperation(WorkflowOperation.TEST);
        workflowsService.performOperation(String.valueOf(WorkflowMapperTest.WORKFLOW_OID), tryOutWorkflowApiDTO);
    }
}
