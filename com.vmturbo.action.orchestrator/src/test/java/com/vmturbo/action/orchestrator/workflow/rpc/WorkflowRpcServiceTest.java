package com.vmturbo.action.orchestrator.workflow.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.action.orchestrator.workflow.webhook.ActionTemplateApplicator;
import com.vmturbo.common.protobuf.api.ApiMessageMoles.ApiMessageServiceMole;
import com.vmturbo.common.protobuf.api.ApiMessageServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecution;
import com.vmturbo.common.protobuf.topology.ActionExecutionMoles.ActionExecutionServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.CreateWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.CreateWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.DeleteWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.DeleteWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.ExecuteWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.ExecuteWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.UpdateWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.UpdateWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowProperty;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Verifies WorkflowRpcService validates inputs and passes the correct information to the
 * WorkflowStore.
 */
public class WorkflowRpcServiceTest {

    private static final long WEBHOOK_TARGET_ID = 100L;
    private static final long WEBHOOK_ID = 123L;
    private static final String EXECUTION_RESULT = "Response code: 200, Body: test";

    private static final ThinTargetInfo WEBHOOK_TARGET = ImmutableThinTargetInfo.builder()
        .displayName("Webhook target")
        .oid(WEBHOOK_TARGET_ID)
        .isHidden(true)
        .probeInfo(ImmutableThinProbeInfo.builder()
            .oid(2L)
            .type(SDKProbeType.WEBHOOK.getProbeType())
            .uiCategory("Orchestrator")
            .category("Orchestrator")
            .build())
        .build();

    private static final Workflow CREATE_WORKFLOW = Workflow.newBuilder()
        .setWorkflowInfo(WorkflowInfo.newBuilder()
            .setType(OrchestratorType.WEBHOOK)
            .setDisplayName("A unique name for the webhook")
            .buildPartial())
        .buildPartial();

    private static final Workflow EXISTING_WORKFLOW = Workflow.newBuilder()
        .setId(123L)
        .setWorkflowInfo(WorkflowInfo.newBuilder()
            .setTargetId(WEBHOOK_TARGET_ID)
            .setType(OrchestratorType.WEBHOOK)
            .setDisplayName("A unique name for the webhook")
            .setDescription("An old description")
            .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                    .setTemplate("{\"id\": \"$action.uuid\", \"type\": \"$action.actionType\", \"commodity\":"
                            + " \"$action.risk.reasonCommodities.toArray()[0]\", \"to\": \"$action.newValue\"}")
                    .build())
            .buildPartial())
        .buildPartial();

    private static final Workflow UPDATE_WORKFLOW = Workflow.newBuilder()
        .setId(123L)
        .setWorkflowInfo(WorkflowInfo.newBuilder()
            .setType(OrchestratorType.WEBHOOK)
            .setDisplayName("A unique name for the webhook")
            .setDescription("A new description")
            .buildPartial())
        .buildPartial();

    @Mock
    private ThinTargetCache thinTargetCache;
    @Mock
    private WorkflowStore workflowStore;
    @Mock
    private ActionExecutionServiceMole actionExecutionServiceMole;
    @Mock
    private ApiMessageServiceMole apiMessageServiceMole;

    private GrpcTestServer grpcTestServer;

    private WorkflowRpcService workflowRpcService;

    private ActionTemplateApplicator actionTemplateApplicator;

    /**
     * Set up the object to test with mock implementations.
     *
     * @throws IOException if something goes wrong starting the mock server.
     */
    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);
        grpcTestServer = GrpcTestServer.newServer(actionExecutionServiceMole, apiMessageServiceMole);
        grpcTestServer.start();
        actionTemplateApplicator = new ActionTemplateApplicator(ApiMessageServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
        workflowRpcService = new WorkflowRpcService(workflowStore, thinTargetCache,
                grpcTestServer.getChannel(), actionTemplateApplicator);

        when(thinTargetCache.getAllTargets()).thenReturn(Collections.singletonList(WEBHOOK_TARGET));
    }

    /**
     * Cleanups after test.
     */
    @After
    public void after() {
        grpcTestServer.close();
    }

    /**
     * A workflow without a orchestrator type, should be filled in using the workflow name.
     *
     * @throws WorkflowStoreException exception should not be thrown.
     */
    @Test
    public void testFillsInMissingOrchestratorType() throws WorkflowStoreException {
        makeWorkflow(1L, "ServiceNowAppAuditOnGeneration");
        makeWorkflow(2L, "ActionStreamKafkaOnGen");
        Workflow workflow = Workflow.newBuilder()
            .setId(3L)
            .setWorkflowInfo(WorkflowInfo.newBuilder()
                .setName("This is an action script")
                .setScriptPath("/home/user/some_script.sh")
                .buildPartial())
            .buildPartial();
        when(workflowStore.fetchWorkflow(3L))
            .thenReturn(Optional.of(workflow));
        makeWorkflow(4L, "This is something else");

        assertEquals(OrchestratorType.SERVICENOW,
            fetchWorkflow(1L).getWorkflow().getWorkflowInfo().getType());
        assertEquals(OrchestratorType.ACTIONSTREAM_KAFKA,
            fetchWorkflow(2L).getWorkflow().getWorkflowInfo().getType());
        assertEquals(OrchestratorType.ACTION_SCRIPT,
            fetchWorkflow(3L).getWorkflow().getWorkflowInfo().getType());
        assertEquals(OrchestratorType.UCSD,
            fetchWorkflow(4L).getWorkflow().getWorkflowInfo().getType());
    }

    private FetchWorkflowResponse fetchWorkflow(long oid) {
        StreamObserver<FetchWorkflowResponse> obs = mock(StreamObserver.class);
        ArgumentCaptor<FetchWorkflowResponse> obsCaptor =
            ArgumentCaptor.forClass(FetchWorkflowResponse.class);
        doNothing().when(obs).onNext(obsCaptor.capture());

        workflowRpcService.fetchWorkflow(FetchWorkflowRequest.newBuilder()
            .setId(oid)
            .build(),
            obs);
        verify(obs, times(1)).onNext(any());
        verify(obs, times(1)).onCompleted();

        return obsCaptor.getValue();
    }

    private void makeWorkflow(long oid, String name) throws WorkflowStoreException {
        makeWorkflow(oid, name, null);
    }

    private void makeWorkflow(long oid, String name, OrchestratorType type) throws WorkflowStoreException {
        WorkflowInfo.Builder workflowInfo = WorkflowInfo.newBuilder()
            .setName(name);
        if (type != null) {
            workflowInfo.setType(type);
        }
        Workflow workflow = Workflow.newBuilder()
            .setId(oid)
            .setWorkflowInfo(workflowInfo.buildPartial())
            .buildPartial();
        when(workflowStore.fetchWorkflow(oid))
            .thenReturn(Optional.of(workflow));
    }

    /**
     * Should not touch the OrchestratorType if it's already there.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testAlreadyHasOrchestratorType() throws WorkflowStoreException {
        makeWorkflow(1L, "ServiceNowUnrelated", OrchestratorType.ACTION_SCRIPT);
        assertEquals(OrchestratorType.ACTION_SCRIPT,
            fetchWorkflow(1L).getWorkflow().getWorkflowInfo().getType());
    }

    /**
     * Create, update, and delete on non user workflows should fail.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testCRUDNonUserWorkflow() throws WorkflowStoreException {
        makeWorkflow(1L, "ServiceNowAppAuditOnGeneration");

        // create
        StreamObserver<CreateWorkflowResponse> createObs = mock(StreamObserver.class);
        workflowRpcService.createWorkflow(CreateWorkflowRequest.newBuilder()
            .setWorkflow(Workflow.newBuilder()
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setType(OrchestratorType.ACTION_SCRIPT)
                    .buildPartial())
                .buildPartial())
            .buildPartial(), createObs);
        verify(createObs, times(0)).onNext(any());
        verify(createObs, times(0)).onCompleted();
        verify(createObs, times(1)).onError(any());

        // update
        StreamObserver<UpdateWorkflowResponse> updateObs = mock(StreamObserver.class);
        workflowRpcService.updateWorkflow(UpdateWorkflowRequest.newBuilder()
            .setWorkflow(Workflow.newBuilder()
                .setId(1L) // this id already exists
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                    .setType(OrchestratorType.ACTION_SCRIPT)
                    .buildPartial())
                .buildPartial())
            .buildPartial(), updateObs);
        verify(updateObs, times(0)).onNext(any());
        verify(updateObs, times(0)).onCompleted();
        verify(updateObs, times(1)).onError(any());

        // delete
        StreamObserver<DeleteWorkflowResponse> deleteObs = mock(StreamObserver.class);
        workflowRpcService.deleteWorkflow(DeleteWorkflowRequest.newBuilder()
            .setId(1L)
            .buildPartial(), deleteObs);
        verify(deleteObs, times(0)).onNext(any());
        verify(deleteObs, times(0)).onCompleted();
        verify(deleteObs, times(1)).onError(any());
    }

    /**
     * When there is no webhook target, the error on the StreamObserver should be set. We require
     * the webhook target, so we can set the targetId on the WorkflowInfo.
     */
    @Test
    public void testUserWorkflowNoTarget() {
        when(thinTargetCache.getAllTargets()).thenReturn(Collections.emptyList());
        StreamObserver<CreateWorkflowResponse> createObs = mock(StreamObserver.class);
        workflowRpcService.createWorkflow(CreateWorkflowRequest.newBuilder()
            .setWorkflow(CREATE_WORKFLOW)
            .buildPartial(), createObs);
        verify(createObs, times(0)).onNext(any());
        verify(createObs, times(0)).onCompleted();
        verify(createObs, times(1)).onError(any());
    }

    /**
     * The workflow info should be passed to the workflow store with the targetId replaced with the
     * WEBHOOK's targetId. The response should contain the oid picked by the workflowStore.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void createPassesWorkflowWithTargetId() throws WorkflowStoreException {
        ArgumentCaptor<WorkflowInfo> workflowInfoCaptor =
            ArgumentCaptor.forClass(WorkflowInfo.class);
        when(workflowStore.insertWorkflow(workflowInfoCaptor.capture())).thenReturn(123L);
        when(workflowStore.getWorkflowByDisplayName(any())).thenReturn(Optional.empty());

        StreamObserver<CreateWorkflowResponse> createObs = mock(StreamObserver.class);
        workflowRpcService.createWorkflow(CreateWorkflowRequest.newBuilder()
            .setWorkflow(CREATE_WORKFLOW)
            .buildPartial(), createObs);

        verify(createObs, times(1)).onNext(eq(CreateWorkflowResponse.newBuilder()
            .setWorkflow(CREATE_WORKFLOW.toBuilder()
                .setId(123L)
                .setWorkflowInfo(CREATE_WORKFLOW.getWorkflowInfo().toBuilder()
                    .setTargetId(WEBHOOK_TARGET_ID)))
            .build()));
        verify(createObs, times(1)).onCompleted();
        verify(createObs, times(0)).onError(any());

        WorkflowInfo actual = workflowInfoCaptor.getValue();
        assertEquals(CREATE_WORKFLOW.getWorkflowInfo().toBuilder()
            .setTargetId(WEBHOOK_TARGET_ID)
            .buildPartial(), actual);
    }

    /**
     * Update should fail if no oid is specified on the workflow. Without it, we have no idea which
     * workflow to update.
     */
    @Test
    public void testUpdateWithoutId() {
        StreamObserver<UpdateWorkflowResponse> updateObs = mock(StreamObserver.class);
        workflowRpcService.updateWorkflow(UpdateWorkflowRequest.newBuilder()
            .setWorkflow(CREATE_WORKFLOW) // create_workflow does not have an oid
            .buildPartial(), updateObs);
        verify(updateObs, times(0)).onNext(any());
        verify(updateObs, times(0)).onCompleted();
        verify(updateObs, times(1)).onError(any());
    }

    /**
     * When the workflow does not exist, there is nothing to update so the service should return
     * an error using the StreamObserver.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testUpdateExistingDoesNotExist() throws WorkflowStoreException {
        when(workflowStore.fetchWorkflow(anyLong())).thenReturn(Optional.empty());
        when(workflowStore.getWorkflowByDisplayName(any())).thenReturn(Optional.empty());
        StreamObserver<UpdateWorkflowResponse> updateObs = mock(StreamObserver.class);
        workflowRpcService.updateWorkflow(UpdateWorkflowRequest.newBuilder()
            .setWorkflow(UPDATE_WORKFLOW)
            .setId(UPDATE_WORKFLOW.getId())
            .buildPartial(), updateObs);
        verify(updateObs, times(0)).onNext(any());
        verify(updateObs, times(0)).onCompleted();
        verify(updateObs, times(1)).onError(any());
    }

    /**
     * Update should pass on the workflow info to the workflowStore.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testUpdate() throws WorkflowStoreException {
        when(workflowStore.fetchWorkflow(anyLong()))
            .thenReturn(Optional.of(EXISTING_WORKFLOW));
        when(workflowStore.getWorkflowByDisplayName(any())).thenReturn(Optional.empty());
        StreamObserver<UpdateWorkflowResponse> updateObs = mock(StreamObserver.class);
        workflowRpcService.updateWorkflow(UpdateWorkflowRequest.newBuilder()
            .setWorkflow(UPDATE_WORKFLOW)
            .setId(UPDATE_WORKFLOW.getId())
            .buildPartial(), updateObs);
        verify(workflowStore, times(1))
            .updateWorkflow(
                eq(UPDATE_WORKFLOW.getId()),
                eq(UPDATE_WORKFLOW.getWorkflowInfo().toBuilder()
                    // The target id should be extracted from the existing workflow and applied to
                    // the updated workflow.
                    .setTargetId(WEBHOOK_TARGET_ID)
                    .build()));
        verify(updateObs, times(1)).onNext(any());
        verify(updateObs, times(1)).onCompleted();
        verify(updateObs, times(0)).onError(any());
    }

    /**
     * Update should fail when workflow id provided in the body request doesn't match with
     * workflow id from path parameters.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testFailedUpdate() throws WorkflowStoreException {
        long mismatchedWorkflowId = 1242L;
        final ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
        when(workflowStore.fetchWorkflow(anyLong())).thenReturn(Optional.of(EXISTING_WORKFLOW));
        when(workflowStore.getWorkflowByDisplayName(any())).thenReturn(Optional.empty());
        StreamObserver<UpdateWorkflowResponse> updateObs = mock(StreamObserver.class);
        workflowRpcService.updateWorkflow(UpdateWorkflowRequest.newBuilder()
                .setWorkflow(UPDATE_WORKFLOW)
                .setId(mismatchedWorkflowId)
                .setId(mismatchedWorkflowId)
                .buildPartial(), updateObs);
        verify(updateObs, times(0)).onNext(any());
        verify(updateObs, times(0)).onCompleted();
        verify(updateObs, times(1)).onError(exceptionCaptor.capture());

        assertThat(exceptionCaptor.getValue().getMessage(), CoreMatchers.containsString(
                "The workflow id provided in the request body must be equal to the id provided as path parameter"));
    }

    /**
     * When the workflow is not found, it cannot be deleted so an error should be returned.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testDeleteDoesNotExist() throws WorkflowStoreException {
        when(workflowStore.fetchWorkflow(anyLong())).thenReturn(Optional.empty());
        StreamObserver<DeleteWorkflowResponse> deleteObs = mock(StreamObserver.class);
        workflowRpcService.deleteWorkflow(DeleteWorkflowRequest.newBuilder()
            .setId(1000L)
            .buildPartial(), deleteObs);
        verify(deleteObs, times(0)).onNext(any());
        verify(deleteObs, times(0)).onCompleted();
        verify(deleteObs, times(1)).onError(any());
    }

    /**
     * The service should pass on the valid delete request to the workflowStore.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testDelete() throws WorkflowStoreException {
        when(workflowStore.fetchWorkflow(anyLong()))
            .thenReturn(Optional.of(EXISTING_WORKFLOW));
        StreamObserver<DeleteWorkflowResponse> deleteObs = mock(StreamObserver.class);
        workflowRpcService.deleteWorkflow(DeleteWorkflowRequest.newBuilder()
            .setId(123L)
            .buildPartial(), deleteObs);
        verify(deleteObs, times(1)).onNext(any());
        verify(deleteObs, times(1)).onCompleted();
        verify(deleteObs, times(0)).onError(any());
        verify(workflowStore, times(1)).deleteWorkflow(eq(123L));
    }

    /**
     * An exception should not be thrown when we cannot reach the workflow store. Instead an error
     * should be set on the stream observer.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testWorkflowStoreFailure() throws WorkflowStoreException {
        when(workflowStore.fetchWorkflow(anyLong())).thenThrow(new WorkflowStoreException("500 Not Available"));
        when(workflowStore.getWorkflowByDisplayName(any())).thenReturn(Optional.empty());
        when(workflowStore.insertWorkflow(any())).thenThrow(new WorkflowStoreException("500 Not Available"));

        StreamObserver<CreateWorkflowResponse> createObs = mock(StreamObserver.class);
        workflowRpcService.createWorkflow(CreateWorkflowRequest.newBuilder()
            .setWorkflow(CREATE_WORKFLOW)
            .buildPartial(), createObs);
        verify(createObs, times(0)).onNext(any());
        verify(createObs, times(0)).onCompleted();
        verify(createObs, times(1)).onError(any());

        StreamObserver<UpdateWorkflowResponse> updateObs = mock(StreamObserver.class);
        workflowRpcService.updateWorkflow(UpdateWorkflowRequest.newBuilder()
            .setWorkflow(UPDATE_WORKFLOW)
            .buildPartial(), updateObs);
        verify(updateObs, times(0)).onNext(any());
        verify(updateObs, times(0)).onCompleted();
        verify(updateObs, times(1)).onError(any());

        StreamObserver<DeleteWorkflowResponse> deleteObs = mock(StreamObserver.class);
        workflowRpcService.deleteWorkflow(DeleteWorkflowRequest.newBuilder()
            .setId(123L)
            .buildPartial(), deleteObs);
        verify(deleteObs, times(0)).onNext(any());
        verify(deleteObs, times(0)).onCompleted();
        verify(deleteObs, times(1)).onError(any());

    }

    /**
     * Tests successful execution of workflow.
     *
     * @throws WorkflowStoreException should not happen.
     */
    @Test
    public void testSuccessfulWorkflowExecution() throws WorkflowStoreException {
        // ARRANGE
        final String actionApiDto = ActionOrchestratorTestUtils.readSampleActionApiDto();
        when(workflowStore.fetchWorkflow(eq(WEBHOOK_ID))).thenReturn(Optional.of(EXISTING_WORKFLOW));
        ArgumentCaptor<ActionExecution.ExecuteWorkflowRequest> captor =
                ArgumentCaptor.forClass(ActionExecution.ExecuteWorkflowRequest.class);
        doAnswer(invocation -> {
            StreamObserver<ActionExecution.ExecuteWorkflowResponse> observer = invocation
                    .getArgumentAt(1, StreamObserver.class);
            observer.onNext(ActionExecution.ExecuteWorkflowResponse.newBuilder()
                    .setSucceeded(true)
                    .setExecutionDetails(EXECUTION_RESULT)
                    .build());
            observer.onCompleted();
            return null;
        }).when(actionExecutionServiceMole).executeWorkflow(captor.capture(), any());
        when(actionExecutionServiceMole.executeWorkflow(captor.capture()))
                .thenReturn(ActionExecution.ExecuteWorkflowResponse.newBuilder()
                        .setSucceeded(true)
                        .setExecutionDetails(EXECUTION_RESULT)
                        .build()
                );

        // ACT
        StreamObserver<ExecuteWorkflowResponse> executeObs = mock(StreamObserver.class);
        workflowRpcService.executeWorkflow(ExecuteWorkflowRequest.newBuilder()
                .setActionApiDTO(actionApiDto)
                .setWorkflowId(WEBHOOK_ID)
                .build(), executeObs);

        // ASSERT
        final List<WorkflowProperty> webhookProperties =
                captor.getValue().getWorkflow().getWorkflowInfo().getWorkflowPropertyList();
        final Optional<WorkflowProperty> templatedActionBody = ActionOrchestratorTestUtils.getWorkflowProperty(
                webhookProperties, WebhookConstants.TEMPLATED_ACTION_BODY);
        final Optional<WorkflowProperty> url = ActionOrchestratorTestUtils.getWorkflowProperty(webhookProperties,
                WebhookConstants.URL);
        assertEquals(2, webhookProperties.size());
        assertTrue(templatedActionBody.isPresent());
        assertTrue(url.isPresent());
        assertEquals(
                "{\"id\": \"637078747168364\", \"type\": \"RESIZE\", \"commodity\": \"VMem\", \"to\": \"59768832.0\"}",
                templatedActionBody.get().getValue());
        final Workflow webhookWithFilledInProperties = fillInWorkflowProperties(EXISTING_WORKFLOW,
                webhookProperties);
        assertEquals(webhookWithFilledInProperties, captor.getValue().getWorkflow());
        ArgumentCaptor<ExecuteWorkflowResponse> workflowResponse =
                ArgumentCaptor.forClass(ExecuteWorkflowResponse.class);
        verify(executeObs, times(1)).onNext(workflowResponse.capture());
        assertEquals(EXECUTION_RESULT, workflowResponse.getValue().getExecutionDetails());
        assertTrue(workflowResponse.getValue().getSucceeded());
        verify(executeObs, times(1)).onCompleted();
        verify(executeObs, times(0)).onError(any());
    }

    /**
     * Tests execution of workflow when the deserialization ActionApiDTO fails.
     *
     * @throws WorkflowStoreException if failed to communicate with the workflow store
     */
    @Test
    public void testWorkflowExecutionFailedDeserialization() throws WorkflowStoreException {
        // ARRANGE
        doAnswer(invocation -> {
            fail("This method should not be called");
            return null;
        }).when(actionExecutionServiceMole).executeWorkflow(any(), any());
        when(workflowStore.fetchWorkflow(eq(WEBHOOK_ID))).thenReturn(
                Optional.of(EXISTING_WORKFLOW));

        // ACT
        StreamObserver<ExecuteWorkflowResponse> executeObs = mock(StreamObserver.class);
        workflowRpcService.executeWorkflow(ExecuteWorkflowRequest.newBuilder()
                .setActionApiDTO("{")
                .setWorkflowId(WEBHOOK_ID)
                .build(), executeObs);

        // ASSERT
        ArgumentCaptor<Throwable> workflowResponse =
                ArgumentCaptor.forClass(Throwable.class);
        verify(executeObs, times(0)).onNext(any());
        verify(executeObs, times(0)).onCompleted();
        verify(executeObs, times(1)).onError(workflowResponse.capture());
        assertEquals("INVALID_ARGUMENT: Failed to de-serialize ActionApiDTO.", workflowResponse.getValue().getMessage());
    }

    /**
     * Tests execution of workflow when the workflow cannot be looked up.
     *
     * @throws WorkflowStoreException should not happen.
     */
    @Test
    public void testWorkflowExecutionWorkflowNotFound() throws WorkflowStoreException {
        // ARRANGE
        final String actionApiDto = ActionOrchestratorTestUtils.readSampleActionApiDto();
        when(workflowStore.fetchWorkflow(eq(WEBHOOK_ID))).thenReturn(Optional.empty());
        doAnswer(invocation -> {
            fail("This method should not be called");
            return null;
        }).when(actionExecutionServiceMole).executeWorkflow(any(), any());

        // ACT
        StreamObserver<ExecuteWorkflowResponse> executeObs = mock(StreamObserver.class);
        workflowRpcService.executeWorkflow(ExecuteWorkflowRequest.newBuilder()
                .setActionApiDTO(actionApiDto)
                .setWorkflowId(WEBHOOK_ID)
                .build(), executeObs);

        // ASSERT
        ArgumentCaptor<Throwable> workflowResponse =
                ArgumentCaptor.forClass(Throwable.class);
        verify(executeObs, times(0)).onNext(any());
        verify(executeObs, times(0)).onCompleted();
        verify(executeObs, times(1)).onError(workflowResponse.capture());
        assertEquals("INVALID_ARGUMENT: Workflow with ID: 123 has not been found", workflowResponse.getValue().getMessage());
    }

    /**
     * Tests execution of workflow when the type of workflow is not webhook.
     *
     * @throws WorkflowStoreException should not happen.
     */
    @Test
    public void testWorkflowExecutionNotWebhook() throws WorkflowStoreException {
        // ARRANGE
        final String actionApiDto = ActionOrchestratorTestUtils.readSampleActionApiDto();
        when(workflowStore.fetchWorkflow(eq(WEBHOOK_ID))).thenReturn(Optional.of(Workflow.newBuilder()
                .setId(123L)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setType(OrchestratorType.ACTION_SCRIPT)
                        .setDisplayName("action script")
                        .build()).build()));
        doAnswer(invocation -> {
            fail("This method should not be called");
            return null;
        }).when(actionExecutionServiceMole).executeWorkflow(any(), any());

        // ACT
        StreamObserver<ExecuteWorkflowResponse> executeObs = mock(StreamObserver.class);
        workflowRpcService.executeWorkflow(ExecuteWorkflowRequest.newBuilder()
                .setActionApiDTO(actionApiDto)
                .setWorkflowId(WEBHOOK_ID)
                .build(), executeObs);

        // ASSERT
        ArgumentCaptor<Throwable> workflowResponse =
                ArgumentCaptor.forClass(Throwable.class);
        verify(executeObs, times(0)).onNext(any());
        verify(executeObs, times(0)).onCompleted();
        verify(executeObs, times(1)).onError(workflowResponse.capture());
        assertEquals("INVALID_ARGUMENT: Trying out workflow of type ACTION_SCRIPT is not supported.",
                workflowResponse.getValue().getMessage());
    }

    /**
     * Tests failed execution of workflow due to exception while applying template.
     *
     * @throws WorkflowStoreException should not happen.
     */
    @Test
    public void testWorkflowExecutionFailedTemplateApplication() throws WorkflowStoreException {
        // ARRANGE
        final String actionApiDto = ActionOrchestratorTestUtils.readSampleActionApiDto();
        when(workflowStore.fetchWorkflow(eq(WEBHOOK_ID))).thenReturn(Optional.of(Workflow.newBuilder()
                .setId(123L)
                .setWorkflowInfo(WorkflowInfo.newBuilder()
                        .setType(OrchestratorType.WEBHOOK)
                        .setDisplayName("A unique name for the webhook")
                        .setWebhookInfo(WorkflowInfo.WebhookInfo.newBuilder()
                            .setTemplate("{\"id\": \"$action.uuid1\", \"type\": \"$action.actionType\", \"commodity\":"
                                    + " \"$action.risk.reasonCommodities.toArray()[0]\", \"to\": \"$action.newValue\"}")
                            ))
                .build()));
        doAnswer(invocation -> {
            fail("This method should not be called");
            return null;
        }).when(actionExecutionServiceMole).executeWorkflow(any(), any());

        // ACT
        StreamObserver<ExecuteWorkflowResponse> executeObs = mock(StreamObserver.class);
        workflowRpcService.executeWorkflow(ExecuteWorkflowRequest.newBuilder()
                .setActionApiDTO(actionApiDto)
                .setWorkflowId(WEBHOOK_ID)
                .build(), executeObs);

        // ASSERT
        ArgumentCaptor<Throwable> workflowResponse =
                ArgumentCaptor.forClass(Throwable.class);
        verify(executeObs, times(0)).onNext(any());
        verify(executeObs, times(0)).onCompleted();
        verify(executeObs, times(1)).onError(workflowResponse.capture());
        assertThat(workflowResponse.getValue().getMessage(), CoreMatchers.containsString(
                "INVALID_ARGUMENT: Exception while applying template:"));
    }

    private Workflow fillInWorkflowProperties(final Workflow workflow,
            final List<WorkflowProperty> workflowProperties) {
        final WorkflowInfo.Builder workflowInfoBuilder = WorkflowInfo.newBuilder(
                workflow.getWorkflowInfo());
        workflowInfoBuilder.addAllWorkflowProperty(workflowProperties);
        return Workflow.newBuilder(workflow).setWorkflowInfo(workflowInfoBuilder.build()).build();
    }

}