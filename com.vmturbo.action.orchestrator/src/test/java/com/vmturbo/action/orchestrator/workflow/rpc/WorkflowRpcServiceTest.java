package com.vmturbo.action.orchestrator.workflow.rpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.CreateWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.CreateWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.DeleteWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.DeleteWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.UpdateWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.UpdateWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
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

    private WorkflowRpcService workflowRpcService;

    /**
     * Set up the object to test with mock implementations.
     */
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        workflowRpcService = new WorkflowRpcService(workflowStore, thinTargetCache);

        when(thinTargetCache.getAllTargets()).thenReturn(Arrays.asList(
            WEBHOOK_TARGET));
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

        Assert.assertThat(exceptionCaptor.getValue().getMessage(), CoreMatchers.containsString(
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
}