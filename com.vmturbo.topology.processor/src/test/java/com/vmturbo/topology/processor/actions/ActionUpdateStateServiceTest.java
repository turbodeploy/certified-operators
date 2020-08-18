package com.vmturbo.topology.processor.actions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.TriConsumer;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalFeature;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit test for {@link ActionUpdateStateService}.
 */
public class ActionUpdateStateServiceTest {

    private static final long TGT_ID = 1001L;
    private static final long PROBE_ID = 2001L;

    private static final long ACTION1 = 1001L;
    private static final long ACTION2 = 1002L;
    private static final long ACTION3 = 1003L;

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private TargetStore targetStore;
    @Mock
    private IOperationManager operationManager;
    @Mock
    private IMessageReceiver<ActionResponse> msgReceiver;
    @Mock
    private SpanContext spanContext;
    @Captor
    private ArgumentCaptor<TriConsumer<ActionResponse, Runnable, SpanContext>> receiverCaptor;
    @Captor
    private ArgumentCaptor<OperationCallback<ActionErrorsResponse>> callbackCaptor;
    @Captor
    private ArgumentCaptor<Collection<ActionResponse>> probeRequestCaptor;
    private MockScheduledService scheduledService;

    /**
     * Initializes all the tests.
     *
     * @throws Exception on exceptions occurred.
     */
    @Before
    public void init() throws Exception {
        IdentityGenerator.initPrefix(0);
        MockitoAnnotations.initMocks(this);
        this.scheduledService = new MockScheduledService();
        new ActionUpdateStateService(targetStore, operationManager, msgReceiver, scheduledService,
                30, 2, 3);
        Mockito.verify(msgReceiver).addListener(receiverCaptor.capture());
        final TargetSpec targetSpec = TargetSpec.newBuilder().setProbeId(PROBE_ID).addAccountValue(
                AccountValue.newBuilder().setKey("id").setStringValue("target").build()).build();
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeType("test probe")
                .setProbeCategory("generic")
                .setActionApproval(ActionApprovalFeature.newBuilder().build())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName("id")
                                .setDescription("id")
                                .setDisplayName("id")
                                .build())
                        .build())
                .addTargetIdentifierField("id")
                .build();
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        final Target target = new Target(TGT_ID, probeStore, targetSpec, false);
        Mockito.when(targetStore.getTarget(TGT_ID)).thenReturn(Optional.of(target));
        Mockito.when(targetStore.getAll()).thenReturn(Collections.singletonList(target));

        final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateOperationId()).thenAnswer(
                invocation -> IdentityGenerator.next());

        Mockito.when(operationManager.updateExternalAction(Mockito.anyLong(), Mockito.any(),
                Mockito.any())).thenAnswer(
                invocation -> new ActionUpdateState(PROBE_ID, TGT_ID, identityProvider));
    }

    /**
     * Tests successful flow of action state updates.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSuccessFlow() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION1), commit1, spanContext);
        final Runnable commit2 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION2), commit2, spanContext);
        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);
        Mockito.verifyZeroInteractions(operationManager);
        scheduledService.executeScheduledTasks();
        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);
        Mockito.verify(operationManager).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(
                Sets.newHashSet(createActionResponse(ACTION1), createActionResponse(ACTION2)),
                new HashSet<>(probeRequestCaptor.getValue()));
        callbackCaptor.getValue().onSuccess(ActionErrorsResponse.newBuilder()
                .addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION2)
                        .setMessage("Failed to report")
                        .build())
                .build());
        Mockito.verify(commit1).run();
        Mockito.verify(commit2).run();
    }

    /**
     * Test following flow when the number of maximum elements is exceeded in queue contains
     * state updates. When queue is full and we receive new updates we remove oldest elements
     * from queue in order to add latest one.
     * MaxElementsInQueue - 3, batchSize - 2.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testTheNumberOfMaximumElementsInQueueIsExceeded() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        final Runnable commit2 = Mockito.mock(Runnable.class);
        final Runnable commit3 = Mockito.mock(Runnable.class);
        final Runnable commit4 = Mockito.mock(Runnable.class);

        final ActionResponse actionResponse1 =
                createActionResponse(ACTION1, 30, ActionResponseState.IN_PROGRESS);
        final ActionResponse actionResponse2 =
                createActionResponse(ACTION1, 50, ActionResponseState.IN_PROGRESS);
        final ActionResponse actionResponse3 =
                createActionResponse(ACTION1, 80, ActionResponseState.IN_PROGRESS);
        final ActionResponse actionResponse4 =
                createActionResponse(ACTION1, 100, ActionResponseState.SUCCEEDED);

        receiverCaptor.getValue().accept(actionResponse1, commit1, spanContext);
        receiverCaptor.getValue().accept(actionResponse2, commit2, spanContext);
        receiverCaptor.getValue().accept(actionResponse3, commit3, spanContext);
        receiverCaptor.getValue().accept(actionResponse4, commit4, spanContext);

        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);
        Mockito.verifyZeroInteractions(commit3);
        Mockito.verifyZeroInteractions(commit4);
        Mockito.verifyZeroInteractions(operationManager);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager)
                .updateExternalAction(Mockito.eq(TGT_ID), probeRequestCaptor.capture(),
                        callbackCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(actionResponse2, actionResponse3),
                new HashSet<>(probeRequestCaptor.getValue()));
        callbackCaptor.getValue().onSuccess(ActionErrorsResponse.getDefaultInstance());
        // sent only #2 and #3 state updates because #1 was removed from queue when it was full and
        // we received #4 update
        Mockito.verifyZeroInteractions(commit1);
        Mockito.verify(commit2).run();
        Mockito.verify(commit3).run();
        Mockito.verifyZeroInteractions(commit4);
        // Sending another chunk after previous is finished
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2)).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(actionResponse4),
                new HashSet<>(probeRequestCaptor.getAllValues().get(2)));
        callbackCaptor.getValue().onSuccess(ActionErrorsResponse.getDefaultInstance());
        // send last remaining #4 state update
        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);
        Mockito.verifyZeroInteractions(commit3);
        Mockito.verify(commit4).run();
    }

    /**
     * Tests sending intermediate state updates.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSendingIntermediateStateUpdates() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        final Runnable commit2 = Mockito.mock(Runnable.class);

        final ActionResponse actionResponse1 =
                createActionResponse(ACTION1, 80, ActionResponseState.IN_PROGRESS);
        final ActionResponse actionResponse2 =
                createActionResponse(ACTION1, 100, ActionResponseState.SUCCEEDED);

        receiverCaptor.getValue().accept(actionResponse1, commit1, spanContext);
        receiverCaptor.getValue().accept(actionResponse2, commit2, spanContext);

        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);
        Mockito.verifyZeroInteractions(operationManager);
        scheduledService.executeScheduledTasks();
        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);
        Mockito.verify(operationManager)
                .updateExternalAction(Mockito.eq(TGT_ID), probeRequestCaptor.capture(),
                        callbackCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(actionResponse1, actionResponse2),
                new HashSet<>(probeRequestCaptor.getValue()));
        callbackCaptor.getValue().onSuccess(ActionErrorsResponse.getDefaultInstance());
        Mockito.verify(commit1).run();
        Mockito.verify(commit2).run();
    }

    /**
     * Tests failure while sending values to remove container. Action updates are expected to
     * still be resent within a next execution
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFailureSendingUpdates() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION1), commit1, spanContext);
        Mockito.verifyZeroInteractions(operationManager);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(createActionResponse(ACTION1)),
                new HashSet<>(probeRequestCaptor.getValue()));
        callbackCaptor.getValue().onFailure("Some error");
        Mockito.verifyZeroInteractions(commit1);

        final Runnable commit2 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION2), commit2, spanContext);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2)).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(
                Sets.newHashSet(createActionResponse(ACTION1), createActionResponse(ACTION2)),
                new HashSet<>(probeRequestCaptor.getAllValues().get(2)));
        callbackCaptor.getAllValues().get(2).onSuccess(ActionErrorsResponse.newBuilder()
                .addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION2)
                        .setMessage("Failed")
                        .build())
                .build());
        Mockito.verify(commit1).run();
        Mockito.verify(commit2).run();
    }

    /**
     * If current operation is in progress, we do not commit a newly accepted state updates, but
     * instead await for the operation to complete.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSdkOperationAlreadyRunning() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION1), commit1, spanContext);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(createActionResponse(ACTION1)),
                new HashSet<>(probeRequestCaptor.getValue()));
        final Runnable commit2 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION2), commit2, spanContext);
        scheduledService.executeScheduledTasks();
        callbackCaptor.getValue().onSuccess(ActionErrorsResponse.newBuilder().build());
        Mockito.verify(commit1).run();
        Mockito.verifyZeroInteractions(commit2);
    }

    /**
     * If there is not action approval target available, we do not expect any operations to be sent
     * to operation manager.
     */
    @Test
    public void testNoApprovalTarget() {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION1), commit1, spanContext);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());
        scheduledService.executeScheduledTasks();
        Mockito.verifyZeroInteractions(operationManager);
        Mockito.verifyZeroInteractions(commit1);
    }

    /**
     * Tests if error occurred while sending request to a probe.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFailureSendingRequestToProbe() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION1), commit1, spanContext);
        Mockito.doThrow(new CommunicationException("Avada Kedavra"))
                .when(operationManager)
                .updateExternalAction(Mockito.eq(TGT_ID), Mockito.any(), Mockito.any());
        scheduledService.executeScheduledTasks();
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2)).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), Mockito.any());
        Mockito.verifyZeroInteractions(commit1);
    }

    /**
     * Tests when there is too large amount of action updates, so they should be splitted into
     * different chunks.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testBatchChunking() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION1), commit1, spanContext);
        final Runnable commit2 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION2), commit2, spanContext);
        final Runnable commit3 = Mockito.mock(Runnable.class);
        receiverCaptor.getValue().accept(createActionResponse(ACTION3), commit3, spanContext);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(
                Sets.newHashSet(createActionResponse(ACTION1), createActionResponse(ACTION2)),
                new HashSet<>(probeRequestCaptor.getValue()));
        callbackCaptor.getValue().onSuccess(ActionErrorsResponse.newBuilder().build());
        Mockito.verify(commit1).run();
        Mockito.verify(commit2).run();
        Mockito.verifyZeroInteractions(commit3);
        // Sending another chunk after previous is finished
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2)).updateExternalAction(Mockito.eq(TGT_ID),
                probeRequestCaptor.capture(), callbackCaptor.capture());
        Assert.assertEquals(Sets.newHashSet(createActionResponse(ACTION3)),
                new HashSet<>(probeRequestCaptor.getAllValues().get(2)));
        callbackCaptor.getAllValues().get(2).onSuccess(ActionErrorsResponse.newBuilder().build());
        Mockito.verify(commit3).run();
    }

    /**
     * Tests incorrect batch size specified in the constructor parameters.
     */
    @Test
    public void testInvalidBatchSize() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("updateBatchSize");
        new ActionUpdateStateService(targetStore, operationManager, msgReceiver, scheduledService,
                10, -1, 2);
    }

    @Nonnull
    private static ActionResponse createActionResponse(long oid) {
        return createActionResponse(oid, 20, ActionResponseState.IN_PROGRESS);
    }

    @Nonnull
    private static ActionResponse createActionResponse(long oid, int progressValue,
            @Nonnull ActionResponseState actionState) {
        return ActionResponse.newBuilder()
                .setActionOid(oid)
                .setProgress(progressValue)
                .setResponseDescription("state description")
                .setActionResponseState(actionState)
                .build();
    }
}
