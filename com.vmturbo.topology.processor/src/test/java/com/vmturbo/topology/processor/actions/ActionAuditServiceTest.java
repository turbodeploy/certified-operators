package com.vmturbo.topology.processor.actions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.After;
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

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionAuditFeature;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAudit;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit test for {@link ActionAuditService}.
 */
public class ActionAuditServiceTest {
    private static final long AUDIT_PROBE = 1003L;
    private static final long ACTION_TARGET = 1001L;
    private static final long AUDIT_TARGET = 1002L;

    private static final long ACTION1 = 2001L;
    private static final long ACTION2 = 2002L;
    private static final long ACTION3 = 2003L;

    private static final long ENTITY_ID = 4001L;

    private static final long TIMEOUT_SEC = 30;

    @Mock
    private IMessageReceiver<ActionEvent> eventsReceiver;
    @Captor
    private ArgumentCaptor<BiConsumer<ActionEvent, Runnable>> eventCaptor;
    @Captor
    private ArgumentCaptor<List<ActionEventDTO>> sdkEventsCaptor;
    @Captor
    private ArgumentCaptor<OperationCallback<ActionErrorsResponse>> operationCaptor;
    @Mock
    private TargetStore targetStore;
    @Mock
    private IOperationManager operationManager;
    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private ActionExecutionContextFactory contextFactory;
    private MockScheduledService threadPool;
    private BiConsumer<ActionEvent, Runnable> messageConsumer;
    private AtomicLong counter;
    private IdentityProvider identityProvider;
    private ActionAuditService actionAuditService;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exception occurred
     */
    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.threadPool = new MockScheduledService();
        counter = new AtomicLong(0);
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeType("test probe")
                .setProbeCategory("generic")
                .setActionAudit(ActionAuditFeature.newBuilder().build())
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
        final ActionDataManager actionDataManagerMock = Mockito.mock(ActionDataManager.class);
        final EntityStore entityStoreMock = Mockito.mock(EntityStore.class);
        final EntityRetriever entityRetriever = Mockito.mock(EntityRetriever.class);
        final TopologyEntityDTO vmTopology = TopologyEntityDTO
                .newBuilder()
                .setOid(ENTITY_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("vm-1")
                .build();
        final EntityDTO vm = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setDisplayName("vm-1")
                .setId("vm-1")
                .build();
        Mockito.when(entityRetriever.retrieveTopologyEntities(Collections.singletonList(ENTITY_ID)))
                .thenReturn(Collections.singletonList(vmTopology));
        Mockito.when(entityRetriever.fetchAndConvertToEntityDTO(ENTITY_ID)).thenReturn(vm);
        final Entity vmEntity = new Entity(ENTITY_ID, EntityType.VIRTUAL_MACHINE);
        vmEntity.addTargetInfo(ACTION_TARGET, vm);
        final TopologyToSdkEntityConverter topologyToSdkEntityConverter = Mockito.mock(
                TopologyToSdkEntityConverter.class);
        Mockito.when(topologyToSdkEntityConverter.convertToEntityDTO(vmTopology)).thenReturn(vm);
        Mockito.when(entityStoreMock.getEntity(ENTITY_ID)).thenReturn(Optional.of(vmEntity));
        this.contextFactory = new ActionExecutionContextFactory(actionDataManagerMock,
                entityStoreMock, entityRetriever, targetStore, probeStore);
        identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateOperationId()).thenAnswer(
                invocation -> counter.getAndIncrement());
        Mockito.when(operationManager.sendActionAuditEvents(Mockito.anyLong(), Mockito.any(),
                Mockito.any())).thenAnswer(
                invocation -> new ActionAudit(AUDIT_PROBE, AUDIT_TARGET, identityProvider));

        actionAuditService = new ActionAuditService(eventsReceiver, operationManager,
                contextFactory, threadPool, 10, 2, 0);
        Mockito.verify(eventsReceiver).addListener(eventCaptor.capture());
        messageConsumer = eventCaptor.getValue();
    }

    /**
     * Cleans up the resources.
     */
    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    /**
     * Tests sending audit messages triggered by both overflow and timer.
     *
     * @throws Exception on exception occurred.
     */
    @Test
    public void testOverflowAndTimer() throws Exception {
        actionAuditService.initialize();
        final Runnable commit1 = Mockito.mock(Runnable.class);
        final Runnable commit2 = Mockito.mock(Runnable.class);
        final Runnable commit3 = Mockito.mock(Runnable.class);
        final ActionEvent event1 = createAction(ACTION1);
        final ActionEvent event2 = createAction(ACTION2);
        final ActionEvent event3 = createAction(ACTION3);
        messageConsumer.accept(event1, commit1);
        Mockito.verifyZeroInteractions(operationManager);
        messageConsumer.accept(event2, commit2);
        messageConsumer.accept(event3, commit3);
        Mockito.verify(operationManager, Mockito.timeout(TIMEOUT_SEC * 1000)).sendActionAuditEvents(
                Mockito.eq(AUDIT_TARGET), sdkEventsCaptor.capture(), operationCaptor.capture());
        Assert.assertEquals(Arrays.asList(ACTION1, ACTION2), sdkEventsCaptor.getValue()
                .stream()
                .map(ActionEventDTO::getAction)
                .map(ActionExecutionDTO::getActionOid)
                .collect(Collectors.toList()));
        final OperationCallback<ActionErrorsResponse> callback = operationCaptor.getValue();
        Mockito.verifyZeroInteractions(commit1);
        Mockito.verifyZeroInteractions(commit2);

        callback.onSuccess(ActionErrorsResponse.newBuilder().build());
        Mockito.verify(commit1).run();
        Mockito.verify(commit2).run();
        Mockito.verifyZeroInteractions(commit3);

        threadPool.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.timeout(TIMEOUT_SEC * 1000).times(2))
                .sendActionAuditEvents(Mockito.eq(AUDIT_TARGET), sdkEventsCaptor.capture(),
                        operationCaptor.capture());
        final OperationCallback<ActionErrorsResponse> callback2 =
                operationCaptor.getAllValues().get(2);
        Assert.assertEquals(Collections.singletonList(ACTION3), sdkEventsCaptor.getAllValues()
                .get(2)
                .stream()
                .map(ActionEventDTO::getAction)
                .map(ActionExecutionDTO::getActionOid)
                .collect(Collectors.toList()));
        Mockito.verifyZeroInteractions(commit3);
        callback2.onSuccess(ActionErrorsResponse.newBuilder().build());
        Mockito.verify(commit3).run();
    }

    /**
     * Tests sending audit messages processing is not performed until service is initialized.
     *
     * @throws Exception on exception occurred.
     */
    @Test
    public void testNotActingUntillInitialized() throws Exception {
        final Runnable commit1 = Mockito.mock(Runnable.class);
        final Runnable commit2 = Mockito.mock(Runnable.class);
        final Runnable commit3 = Mockito.mock(Runnable.class);
        final ActionEvent event1 = createAction(ACTION1);
        final ActionEvent event2 = createAction(ACTION2);
        final ActionEvent event3 = createAction(ACTION3);
        messageConsumer.accept(event1, commit1);
        messageConsumer.accept(event2, commit2);
        messageConsumer.accept(event3, commit3);
        threadPool.executeScheduledTasks();
        Mockito.verifyZeroInteractions(operationManager);
        actionAuditService.initialize();
        threadPool.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.timeout(TIMEOUT_SEC * 1000))
                .sendActionAuditEvents(Mockito.eq(AUDIT_TARGET), sdkEventsCaptor.capture(),
                        operationCaptor.capture());
    }

    /**
     * Tests when errors occurred while sending request to SDK probe.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testErrorSendingRequest() throws Exception {
        actionAuditService.initialize();
        Mockito.doThrow(new CommunicationException("Avada Kedavra"))
                .doThrow(new InterruptedException())
                .doAnswer(
                        invocation -> new ActionAudit(AUDIT_PROBE, AUDIT_TARGET, identityProvider))
                .when(operationManager)
                .sendActionAuditEvents(Mockito.anyLong(), Mockito.any(), Mockito.any());
        final Runnable commit = Mockito.mock(Runnable.class);
        final ActionEvent event = createAction(ACTION1);
        messageConsumer.accept(event, commit);
        threadPool.executeScheduledTasks();
        threadPool.executeScheduledTasks();
        threadPool.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(3)).sendActionAuditEvents(
                Mockito.eq(AUDIT_TARGET), sdkEventsCaptor.capture(), operationCaptor.capture());
        Mockito.verifyZeroInteractions(commit);
        Assert.assertEquals(Collections.singletonList(ACTION1), sdkEventsCaptor.getAllValues()
                .get(0)
                .stream()
                .map(ActionEventDTO::getAction)
                .map(ActionExecutionDTO::getActionOid)
                .collect(Collectors.toList()));
        Assert.assertEquals(Collections.singletonList(ACTION1), sdkEventsCaptor.getAllValues()
                .get(1)
                .stream()
                .map(ActionEventDTO::getAction)
                .map(ActionExecutionDTO::getActionOid)
                .collect(Collectors.toList()));
        Assert.assertEquals(Collections.singletonList(ACTION1), sdkEventsCaptor.getAllValues()
                .get(2)
                .stream()
                .map(ActionEventDTO::getAction)
                .map(ActionExecutionDTO::getActionOid)
                .collect(Collectors.toList()));
        operationCaptor.getAllValues().get(2).onSuccess(ActionErrorsResponse.newBuilder()
                .addErrors(ActionErrorDTO.newBuilder()
                        .setActionOid(ACTION1)
                        .setMessage("Something failed"))
                .build());
        Mockito.verify(commit).run();
    }

    /**
     * Tests when operation is failed (for example timed out) by operation manager.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testOperationFailed() throws Exception {
        actionAuditService.initialize();
        final Runnable commit = Mockito.mock(Runnable.class);
        final ActionEvent event = createAction(ACTION1);
        messageConsumer.accept(event, commit);
        threadPool.executeScheduledTasks();
        Mockito.verify(operationManager).sendActionAuditEvents(Mockito.eq(AUDIT_TARGET),
                sdkEventsCaptor.capture(), operationCaptor.capture());
        operationCaptor.getValue().onFailure("Avada Kedavra");
        threadPool.executeScheduledTasks();
        Mockito.verify(operationManager, Mockito.times(2)).sendActionAuditEvents(
                Mockito.eq(AUDIT_TARGET), sdkEventsCaptor.capture(), operationCaptor.capture());
    }

    /**
     * Tests invalid batch size reported as a parameter to a service constructor.
     */
    @Test
    public void testInvalidBatchSize() {
        actionAuditService.initialize();
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("batchSize must be a positive value");
        new ActionAuditService(eventsReceiver, operationManager, contextFactory, threadPool, 10, 0,
                0);
    }

    @Nonnull
    private ActionEvent createAction(long oid) {
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(oid)
                .setTargetId(AUDIT_TARGET)
                .setActionInfo(ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder()
                                .setTarget(ActionEntity
                                        .newBuilder()
                                        .setType(1)
                                        .setId(ENTITY_ID))))
                .build();
        return ActionEvent.newBuilder()
                .setAcceptedBy("Albus Dumbldore")
                .setActionRequest(request)
                .setTimestamp(counter.getAndIncrement())
                .setOldState(ActionResponseState.ACCEPTED)
                .setNewState(ActionResponseState.REJECTED)
                .build();
    }
}
