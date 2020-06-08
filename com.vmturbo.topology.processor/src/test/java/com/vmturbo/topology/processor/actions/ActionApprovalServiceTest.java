package com.vmturbo.topology.processor.actions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionApprovalRequests;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.ExternalActionInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalFeature;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit tests for {@link ActionApprovalService}.
 */
public class ActionApprovalServiceTest {

    private static final long TGT_ID = 1001L;
    private static final long PROBE_ID = 2001L;

    private static final long ACTION1 = 3001L;
    private static final long ACTION2 = 3002L;

    private static final long ENTITY_ID = 4001L;

    @Mock
    private IMessageReceiver<ActionApprovalRequests> actionApprovalRequests;
    @Mock
    private IMessageSender<GetActionStateResponse> actionStateSender;
    @Mock
    private IMessageSender<ActionApprovalResponse> approvalResponseSender;
    @Captor
    private ArgumentCaptor<BiConsumer<ActionApprovalRequests, Runnable>> receiverCaptor;
    @Captor
    private ArgumentCaptor<OperationCallback<ActionApprovalResponse>> approvalCaptor;
    @Captor
    private ArgumentCaptor<OperationCallback<GetActionStateResponse>> getExternalStateCaptor;
    @Captor
    private ArgumentCaptor<Collection<Long>> getExternalStateRequestCaptor;

    private MockScheduledService scheduledService;
    private TargetStore targetStore;
    private IOperationManager operationManager;
    private BiConsumer<ActionApprovalRequests, Runnable> receiver;
    private Runnable commitOperation;
    private IdentityProvider identityProvider;
    private ActionExecutionContextFactory contextFactory;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exception occurred
     */
    @Before
    public void init() throws Exception {
        IdentityGenerator.initPrefix(0);
        MockitoAnnotations.initMocks(this);
        final ActionDataManager actionDataManagerMock = Mockito.mock(ActionDataManager.class);
        final EntityStore entityStoreMock = Mockito.mock(EntityStore.class);
        final EntityRetriever entityRetrieverMock = Mockito.mock(EntityRetriever.class);
        final EntityDTO vm = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setDisplayName("vm-1")
                .setId("vm-1")
                .build();
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(ENTITY_ID))
                .thenReturn(vm);
        final Entity vmEntity = new Entity(ENTITY_ID, EntityType.VIRTUAL_MACHINE);
        vmEntity.addTargetInfo(TGT_ID, vm);
        Mockito.when(entityStoreMock.getEntity(ENTITY_ID)).thenReturn(Optional.of(vmEntity));
        final ProbeStore probeStoreMock = Mockito.mock(ProbeStore.class);

        this.scheduledService = new MockScheduledService();
        this.targetStore = Mockito.mock(TargetStore.class);
        this.operationManager = Mockito.mock(IOperationManager.class);
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
        identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateOperationId()).thenAnswer(
                invocation -> IdentityGenerator.next());

        Mockito.when(
                operationManager.approveActions(Mockito.anyLong(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> new ActionApproval(PROBE_ID, TGT_ID, identityProvider));
        Mockito.when(operationManager.getExternalActionState(Mockito.anyLong(), Mockito.any(),
                Mockito.any())).thenAnswer(
                invocation -> new GetActionState(PROBE_ID, TGT_ID, identityProvider));

        this.contextFactory = new ActionExecutionContextFactory(actionDataManagerMock,
                entityStoreMock, entityRetrieverMock, targetStore, probeStoreMock);
        final ActionApprovalService svc = new ActionApprovalService(actionApprovalRequests,
                actionStateSender, approvalResponseSender, operationManager, contextFactory,
                targetStore, scheduledService, 10);
        Mockito.verify(actionApprovalRequests).addListener(receiverCaptor.capture());
        receiver = receiverCaptor.getValue();
        this.commitOperation = Mockito.mock(Runnable.class);
    }

    /**
     * Tests successful flow. Actions got approved and successfully returned their external states.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSuccessfullFlow() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).addActions(createAction(ACTION2)).build();

        receiver.accept(request, commitOperation);
        Mockito.verify(commitOperation).run();
        Mockito.verify(operationManager).approveActions(Mockito.eq(TGT_ID),
                Mockito.argThat(new ActionCollectionMatcher(ACTION1, ACTION2)),
                approvalCaptor.capture());
        Mockito.verify(actionStateSender, Mockito.never()).sendMessage(Mockito.any());
        Mockito.verify(approvalResponseSender, Mockito.never()).sendMessage(Mockito.any());

        final ActionApprovalResponse response = ActionApprovalResponse.newBuilder()
                .addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION2)
                        .setMessage("Some error")
                        .build())
                .putActionState(ACTION1, ExternalActionInfo.newBuilder()
                        .setShortName("some-action")
                        .setUrl("http://action")
                        .build())
                .build();
        approvalCaptor.getValue().onSuccess(response);
        Mockito.verify(approvalResponseSender).sendMessage(response);

        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager).getExternalActionState(Mockito.eq(TGT_ID),
                Mockito.eq(Sets.newHashSet(ACTION1, ACTION2)), getExternalStateCaptor.capture());
        final GetActionStateResponse stateResponse =
                GetActionStateResponse.newBuilder().putActionState(ACTION1,
                        ActionResponseState.ACCEPTED).putActionState(ACTION2,
                        ActionResponseState.PENDING_ACCEPT).addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION2)
                        .setMessage("some new error")
                        .build()).build();
        getExternalStateCaptor.getValue().onSuccess(stateResponse);
        Mockito.verify(actionStateSender).sendMessage(stateResponse);
    }

    /**
     * Tests when approval request is sent when another owe is active. It is expected, that the
     * 2nd one will be ignored.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testOneApprovalRequestsConflict() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).build();

        receiver.accept(request, commitOperation);
        Mockito.verify(commitOperation).run();
        Mockito.verify(operationManager).approveActions(Mockito.eq(TGT_ID),
                Mockito.argThat(new ActionCollectionMatcher(ACTION1)), Mockito.any());

        receiver.accept(request, commitOperation);
        Mockito.verify(commitOperation, Mockito.times(2)).run();
        Mockito.verify(operationManager).approveActions(Mockito.eq(TGT_ID),
                Mockito.argThat(new ActionCollectionMatcher(ACTION1)), Mockito.any());
    }

    /**
     * Tests subsequent approval calls. All are expected to work.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSubsequentApprovals() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).build();
        receiver.accept(request, commitOperation);
        Mockito.verify(commitOperation).run();
        Mockito.verify(operationManager).approveActions(Mockito.eq(TGT_ID),
                Mockito.argThat(new ActionCollectionMatcher(ACTION1)), approvalCaptor.capture());
        final ActionApprovalResponse response1 = createResponse(ACTION1);
        approvalCaptor.getValue().onSuccess(response1);
        Mockito.verify(approvalResponseSender).sendMessage(response1);

        final ActionApprovalRequests request2 = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).addActions(createAction(ACTION2)).build();
        receiver.accept(request2, commitOperation);
        Mockito.verify(commitOperation, Mockito.times(2)).run();
        Mockito.verify(operationManager)
                .approveActions(Mockito.eq(TGT_ID),
                        Mockito.argThat(new ActionCollectionMatcher(ACTION1)),
                        approvalCaptor.capture());
        final ActionApprovalResponse response2 = createResponse(ACTION1, ACTION2);
        approvalCaptor.getAllValues().get(1).onSuccess(response2);
        Mockito.verify(approvalResponseSender).sendMessage(response2);
    }

    /**
     * Tests approval after previous approval is failed.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testApprovalSuccessAfterFailure() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).build();
        receiver.accept(request, commitOperation);
        Mockito.verify(commitOperation).run();
        Mockito.verify(operationManager).approveActions(Mockito.eq(TGT_ID),
                Mockito.argThat(new ActionCollectionMatcher(ACTION1)), approvalCaptor.capture());
        final ActionApprovalResponse response1 = createResponse(ACTION1);
        approvalCaptor.getValue().onFailure("Avada Kedavra");
        Mockito.verify(approvalResponseSender, Mockito.never()).sendMessage(response1);

        final ActionApprovalRequests request2 = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).addActions(createAction(ACTION2)).build();
        receiver.accept(request2, commitOperation);
        Mockito.verify(commitOperation, Mockito.times(2)).run();
        Mockito.verify(operationManager)
                .approveActions(Mockito.eq(TGT_ID),
                        Mockito.argThat(new ActionCollectionMatcher(ACTION1, ACTION2)),
                        approvalCaptor.capture());
        final ActionApprovalResponse response2 = createResponse(ACTION1, ACTION2);
        approvalCaptor.getAllValues().get(1).onSuccess(response2);
        Mockito.verify(approvalResponseSender).sendMessage(response2);
    }

    /**
     * Tests failure while sending approval request to probe.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testApprovalSyncFailure() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).build();
        Mockito.doThrow(new ProbeException("Failed to connect"))
                .when(operationManager)
                .approveActions(Mockito.anyLong(), Mockito.any(), Mockito.any());

        receiver.accept(request, commitOperation);
        Mockito.verify(commitOperation).run();
        Mockito.verify(operationManager)
                .approveActions(Mockito.eq(TGT_ID),
                        Mockito.argThat(new ActionCollectionMatcher(ACTION1)),
                        approvalCaptor.capture());
        final ActionApprovalResponse response1 = createResponse(ACTION1);
        Mockito.verify(approvalResponseSender, Mockito.never()).sendMessage(response1);
    }

    /**
     * Tests when there is no action approval target in the store to operate with.
     */
    @Test
    public void testNoApprovalTarget() {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).build();
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());
        receiver.accept(request, commitOperation);
        scheduledService.executeScheduledTasks();
        Mockito.verifyZeroInteractions(operationManager);
    }

    /**
     * Tests when there is no action approval target after action approvals arrived.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testNoApprovalTargetAfterApprovals() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().addActions(
                createAction(ACTION1)).build();
        receiver.accept(request, commitOperation);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.never()).getExternalActionState(Mockito.anyLong(),
                Mockito.any(), Mockito.any());
    }

    /**
     * Tests when there is no action approval target after action approvals arrived.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetActionStatesAlreadyRunning() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder()
                .addActions(createAction(ACTION1))
                .build();
        receiver.accept(request, commitOperation);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager).getExternalActionState(Mockito.anyLong(),
                Mockito.any(), getExternalStateCaptor.capture());
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager).getExternalActionState(Mockito.anyLong(), Mockito.any(),
                Mockito.any());
    }

    /**
     * Tests that get action state is still executed after previous run failed asynchronously.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetActionStatesRunAfterAsyncFailure() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder()
                .addActions(createAction(ACTION1))
                .build();
        receiver.accept(request, commitOperation);
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager)
                .getExternalActionState(Mockito.anyLong(), Mockito.any(),
                        getExternalStateCaptor.capture());
        getExternalStateCaptor.getValue().onFailure("Something failed");
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2))
                .getExternalActionState(Mockito.anyLong(), Mockito.any(), Mockito.any());
    }

    /**
     * Tests when there is no action approval target after action approvals arrived.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetActionStatesFailureSending() throws Exception {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder()
                .addActions(createAction(ACTION1))
                .build();
        receiver.accept(request, commitOperation);
        Mockito.doThrow(new CommunicationException("Avada Kedavra"))
                .doAnswer(invocation -> new GetActionState(PROBE_ID, TGT_ID, identityProvider))
                .when(operationManager)
                .getExternalActionState(Mockito.anyLong(), Mockito.any(), Mockito.any());
        scheduledService.executeScheduledTasks();
        scheduledService.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2)).getExternalActionState(Mockito.anyLong(),
                getExternalStateRequestCaptor.capture(), Mockito.any());
        Assert.assertEquals(Collections.singleton(ACTION1),
                new HashSet<>(getExternalStateRequestCaptor.getAllValues().get(1)));
    }

    /**
     * Tests no actions sent by approval. It is not expected that anything will be sent to
     * external action approval probe
     */
    @Test
    public void testNoActionsSent() {
        final ActionApprovalRequests request = ActionApprovalRequests.newBuilder().build();
        receiver.accept(request, commitOperation);
        scheduledService.executeScheduledTasks();
        Mockito.verifyZeroInteractions(operationManager);
    }

    private static ActionApprovalResponse createResponse(long... oids) {
        final ActionApprovalResponse.Builder builder = ActionApprovalResponse.newBuilder();
        for (long oid : oids) {
            builder.putActionState(oid, ExternalActionInfo.newBuilder().setUrl(
                    "http://some-target/" + oid).setShortName("CR-" + oid).build());
        }
        return builder.build();
    }

    private static ExecuteActionRequest createAction(long oid) {
        return ExecuteActionRequest.newBuilder()
                .setActionId(oid)
                .setTargetId(TGT_ID)
                .setActionInfo(ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setType(1)
                                        .setId(ENTITY_ID)
                                        .build())))
                .build();
    }

    /**
     * Collection matcher.
     *
     * @param <S> source collection (which could be passed by a method call) type
     * @param <T> target collection (which to compare with) type
     */
    private abstract static class CollectionMatcher<S, T> extends BaseMatcher<Collection<S>> {
        private final Set<T> expected;

        CollectionMatcher(@Nonnull Collection<T> expected) {
            this.expected = new HashSet<>(expected);
        }

        protected abstract T convert(S source);

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof Collection)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            final Collection<S> collection = (Collection<S>)item;
            final Set<T> actual = collection.stream()
                    .map(this::convert)
                    .collect(Collectors.toSet());
            return actual.equals(expected);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(expected.toString());
        }
    }

    /**
     * Matcher for action execution collection - by action OIDs.
     */
    private static class ActionCollectionMatcher extends CollectionMatcher<ActionExecutionDTO, Long> {

        ActionCollectionMatcher(@Nonnull Long... expected) {
            super(Arrays.asList(expected));
        }

        @Override
        protected Long convert(ActionExecutionDTO source) {
            return source.getActionOid();
        }

    }
}
