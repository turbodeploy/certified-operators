package com.vmturbo.topology.processor.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionResponse;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.SdkActionPolicyBuilder;

public class ActionExecutionRpcServiceTest {
    private static final String ACTION_DESCRIPTION = "Action Description";
    private static final long RECOMMENDATION_TIME = 1609346497000L;
    private static final long UPDATE_TIME = 1609347497000L;
    private static final String ACCEPTING_USER = "admin1";
    private static final String IMPROVE_EFFICIENCY = "Improve overall performance";

    private EntityStore entityStore = Mockito.mock(EntityStore.class);

    private OperationManager operationManager = Mockito.mock(OperationManager.class);

    private ActionDataManager actionDataManager = Mockito.mock(ActionDataManager.class);

    private EntityRetriever entityRetriever = Mockito.mock(EntityRetriever.class);

    private GroupAndPolicyRetriever groupAndPolicyRetrieverMock = Mockito.mock(GroupAndPolicyRetriever.class);

    private SecureStorageClient secureStorageClient = Mockito.mock(SecureStorageClient.class);

    private final TargetStore targetStoreMock = Mockito.mock(TargetStore.class);

    private final ProbeStore probeStoreMock = Mockito.mock(ProbeStore.class);

    private ActionExecutionContextFactory actionExecutionContextFactory =
            new ActionExecutionContextFactory(actionDataManager, entityStore, entityRetriever,
                    targetStoreMock, probeStoreMock, groupAndPolicyRetrieverMock, secureStorageClient);

    private ActionExecutionRpcService actionExecutionBackend = new ActionExecutionRpcService(
            operationManager, actionExecutionContextFactory);

    private StreamObserver<ExecuteActionResponse> responseObserver = Mockito.mock(StreamObserver.class);

    @Captor
    private ArgumentCaptor<ActionExecutionDTO> actionExecutionCaptor;

    private AtomicLong targetIdCounter = new AtomicLong();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionExecutionServiceBlockingStub actionExecutionStub;

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(actionExecutionBackend);

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        // These stubs will be replaced in the initializeTopology method for all valid entities in
        // each test. The stubs defined here will be applied to entities that are not found in the
        // topology defined in the test, i.e. missing entities.
        Mockito.when(entityStore.getEntity(Mockito.anyLong())).thenReturn(Optional.empty());
        Mockito.doThrow(new EntityRetrievalException("No entity found "))
                .when(entityRetriever)
                .fetchAndConvertToEntityDTO(Mockito.anyLong());
        Target target = Mockito.mock(Target.class);
        Mockito.when(target.getProbeId()).thenReturn(555L);
        Mockito.when(targetStoreMock.getTarget(Mockito.anyLong())).thenReturn(Optional.of(target));
        Mockito.when(probeStoreMock.getProbe(555L))
            .thenReturn(Optional.of(ProbeInfo.getDefaultInstance()));
        actionExecutionStub = ActionExecutionServiceGrpc.newBlockingStub(server.getChannel());
    }

    /**
     * Test that properly formatted actions go to the {@link OperationManager}
     * with a properly formatted {@link ActionItemDTO}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testHostMove() throws Exception {
        final long targetId = createTarget();

        final ActionDTO.Move moveSpec = ActionDTO.Move.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).addChanges(
                ChangeProvider.newBuilder()
                        .setSource(ActionExecutionTestUtils.createActionEntity(2))
                        .setDestination(ActionExecutionTestUtils.createActionEntity(3))
                        .build()).addChanges(ChangeProvider.newBuilder()
                .setSource(ActionExecutionTestUtils.createActionEntity(4))
                .setDestination(ActionExecutionTestUtils.createActionEntity(5))
                .build()).build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setDescription(ACTION_DESCRIPTION)
                    .setRecommendationTime(RECOMMENDATION_TIME)
                    .setCategory(ActionDTO.ActionCategory.PERFORMANCE_ASSURANCE)
                    .setSeverity(ActionDTO.Severity.CRITICAL)
                    .setExplanation(IMPROVE_EFFICIENCY)
                    .setDecision(ActionDTO.ActionDecision.newBuilder()
                        .setDecisionTime(UPDATE_TIME)
                        .setExecutionDecision(ActionDTO.ActionDecision.ExecutionDecision
                            .newBuilder()
                            .setReason(ActionDTO.ActionDecision.ExecutionDecision.Reason.MANUALLY_ACCEPTED)
                            .setUserUuid(ACCEPTING_USER)
                            .build())
                        .build())
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setDisruptive(true)
                        .setReversible(true)
                        .setInfo(ActionInfo.newBuilder().setMove(moveSpec)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        List<ChangeProvider> changes = moveSpec.getChangesList();
        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(moveSpec.getTarget().getId(), changes.get(0)
                        .getSource()
                        .getId()), NewEntityRequest.physicalMachine(changes.get(0)
                        .getSource()
                        .getId()), NewEntityRequest.physicalMachine(changes.get(0)
                        .getDestination()
                        .getId()), NewEntityRequest.storage(changes.get(1).getSource().getId()),
                NewEntityRequest.storage(changes.get(1).getDestination().getId()));
        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(),
                Mockito.eq(Stream.of(1L, 2L, 3L, 4L, 5L).collect(Collectors.toSet())));

        final ActionExecutionDTO executionDto = actionExecutionCaptor.getValue();

        // verify execution dto fields
        assertEquals(request.getActionId(), executionDto.getActionOid());
        assertEquals(RECOMMENDATION_TIME, executionDto.getCreateTime());
        assertEquals(UPDATE_TIME, executionDto.getUpdateTime());
        assertEquals(ACCEPTING_USER, executionDto.getAcceptedBy());

        final List<ActionItemDTO> dtos = executionDto.getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        // verify primary action fields
        ActionItemDTO primaryAction = dtos.get(0);
        assertEquals(ACTION_DESCRIPTION, primaryAction.getDescription());

        final ActionItemDTO.Risk risk = primaryAction.getRisk();
        assertEquals(IMPROVE_EFFICIENCY, risk.getDescription());
        assertEquals(ActionItemDTO.Risk.Category.PERFORMANCE_ASSURANCE, risk.getCategory());
        assertEquals(ActionItemDTO.Risk.Severity.CRITICAL, risk.getSeverity());
        assertTrue(risk.getAffectedCommodityList().isEmpty());

        ActionItemDTO.ExecutionCharacteristics characteristics = primaryAction.getCharacteristics();
        assertTrue(characteristics.getDisruptive());
        assertTrue(characteristics.getReversible());

        assertEquals(entities.get(moveSpec.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
        assertEquals(entities.get(moveSpec.getChanges(0).getSource().getId()).getTargetInfo(
                targetId).get().getEntityInfo(), dtos.get(0).getHostedBySE());
        assertEquals(entities.get(moveSpec.getChanges(0).getSource().getId()).getTargetInfo(
                targetId).get().getEntityInfo(), dtos.get(0).getCurrentSE());
        assertEquals(entities.get(moveSpec.getChanges(0).getDestination().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getNewSE());
    }

    /**
     * Check that there is no call to the {@link OperationManager} if one of
     * the entities is missing.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMoveMissingDestinationEntity() throws Exception {
        final long targetId = createTarget();
        final ActionDTO.Move move = ActionDTO.Move.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).addChanges(
                ChangeProvider.newBuilder()
                        .setSource(ActionExecutionTestUtils.createActionEntity(2))
                        .setDestination(ActionExecutionTestUtils.createActionEntity(3))
                        .build()).build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setTargetId(targetId)
                .setActionId(0L)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setMove(move)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // Entity with ID 3 (destination) is missing.
        initializeTopology(targetId,
                NewEntityRequest.virtualMachine(move.getTarget().getId(), move.getChanges(0)
                        .getSource()
                        .getId()), NewEntityRequest.physicalMachine(move.getChanges(0)
                        .getSource()
                        .getId()));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 3 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testStorageMove() throws Exception {
        final long targetId = createTarget();

        final ActionDTO.Move moveSpec = ActionDTO.Move.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).addChanges(
                ChangeProvider.newBuilder()
                        .setSource(
                                ActionExecutionTestUtils.createActionEntity(2, EntityType.STORAGE))
                        .setDestination(
                                ActionExecutionTestUtils.createActionEntity(3, EntityType.STORAGE))
                        .build()).build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setDisruptive(false)
                        .setReversible(false)
                        .setInfo(ActionInfo.newBuilder().setMove(moveSpec)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .setActionType(ActionDTO.ActionType.MOVE)
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(moveSpec.getTarget().getId(), moveSpec.getChanges(0)
                        .getSource()
                        .getId()), NewEntityRequest.storage(moveSpec.getChanges(0)
                        .getSource()
                        .getId()), NewEntityRequest.storage(moveSpec.getChanges(0)
                        .getDestination()
                        .getId()));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(),
                // Storage move is a CHANGE in the SDK
                Mockito.eq(Stream.of(1L, 2L, 3L).collect(Collectors.toSet())));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(entities.get(moveSpec.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
        assertEquals(entities.get(moveSpec.getChanges(0).getSource().getId()).getTargetInfo(
                targetId).get().getEntityInfo(), dtos.get(0).getHostedBySE());
        assertEquals(entities.get(moveSpec.getChanges(0).getSource().getId()).getTargetInfo(
                targetId).get().getEntityInfo(), dtos.get(0).getCurrentSE());
        assertEquals(entities.get(moveSpec.getChanges(0).getDestination().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getNewSE());
    }

    @Test
    public void testDatastoreMove() throws Exception {
        final long targetId = createTarget();
        final ActionDTO.Move moveSpec = ActionDTO.Move.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).addChanges(
                ChangeProvider.newBuilder()
                        .setSource(ActionExecutionTestUtils.createActionEntity(2))
                        .setDestination(ActionExecutionTestUtils.createActionEntity(3))
                        .build()).build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setDisruptive(false)
                        .setReversible(true)
                        .setInfo(ActionInfo.newBuilder().setMove(moveSpec)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.storage(moveSpec.getTarget().getId()),
                NewEntityRequest.diskArray(moveSpec.getChanges(0).getSource().getId()),
                NewEntityRequest.diskArray(moveSpec.getChanges(0).getDestination().getId()));
        Mockito.when(targetStoreMock.getProbeTypeForTarget(targetId)).thenReturn(
                Optional.of(SDKProbeType.VCENTER));
        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(),
                Mockito.eq(Stream.of(1L, 2L, 3L).collect(Collectors.toSet())));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(entities.get(moveSpec.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
        Assert.assertFalse(dtos.get(0).hasHostedBySE());
        assertEquals(entities.get(moveSpec.getChanges(0).getSource().getId()).getTargetInfo(
                targetId).get().getEntityInfo(), dtos.get(0).getCurrentSE());
        assertEquals(entities.get(moveSpec.getChanges(0).getDestination().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getNewSE());
    }

    @Test
    public void testMoveIncompatibleSourceAndDestination() throws Exception {
        final long targetId1 = createTarget();

        final ActionDTO.Move move = ActionDTO.Move.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).addChanges(
                ChangeProvider.newBuilder()
                        .setSource(ActionExecutionTestUtils.createActionEntity(2))
                        .setDestination(ActionExecutionTestUtils.createActionEntity(3))
                        .build()).build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setTargetId(targetId1)
                .setActionId(0L)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setMove(move)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        initializeTopology(targetId1, NewEntityRequest.virtualMachine(move.getTarget().getId(), move
                        .getChanges(0)
                        .getSource()
                        .getId()), NewEntityRequest.physicalMachine(move.getChanges(0).getSource().getId()),
                // Destination is a storage, but source is a PM.
                NewEntityRequest.storage(move.getChanges(0).getDestination().getId()));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Mismatched source and destination"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testResize() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Resize resizeSpec = ActionDTO.Resize.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).setCommodityType(
                CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE)
                        .setKey("key")).setOldCapacity(10).setNewCapacity(20).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setResize(resizeSpec)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .setActionType(ActionDTO.ActionType.RESIZE)
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(resizeSpec.getTarget().getId(), 7),
                NewEntityRequest.physicalMachine(7));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(), Mockito.eq(Collections.singleton(1L)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        //        ActionItemDTOValidator.validateRequest(dto);

        assertEquals(entities.get(resizeSpec.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
        assertEquals(CommodityAttribute.Capacity, dtos.get(0).getCommodityAttribute());
        assertEquals(CommodityDTO.CommodityType.MEM, dtos.get(0)
                .getCurrentComm()
                .getCommodityType());
        assertEquals(10, dtos.get(0).getCurrentComm().getCapacity(), 0);
        assertEquals(CommodityDTO.CommodityType.MEM, dtos.get(0)
                .getNewComm()
                .getCommodityType());
        assertEquals(20, dtos.get(0).getNewComm().getCapacity(), 0);
    }

    @Test
    public void testResizeMissingEntity() throws Exception {

        final long targetId = targetIdCounter.getAndIncrement();
        final ActionDTO.Resize resizeSpec = ActionDTO.Resize.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).setCommodityType(
                CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE)
                        .setKey("key")).setOldCapacity(10).setNewCapacity(20).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setResize(resizeSpec)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // No entities in topology; target entity is missing.
        initializeTopology(targetId);

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 1 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testResizeVMMssingHost() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Resize resizeSpec = ActionDTO.Resize.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(1)).setCommodityType(
                CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE)
                        .setKey("key")).setOldCapacity(10).setNewCapacity(20).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setResize(resizeSpec)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // Include a virtual machine, but no host that matches the host ID.
        initializeTopology(targetId,
                NewEntityRequest.virtualMachine(resizeSpec.getTarget().getId(), 7));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 7 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testActivateVM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setActivate(activate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(activate.getTarget().getId(), 7),
                NewEntityRequest.physicalMachine(7));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(), Mockito.eq(Collections.singleton(1L)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        //        ActionItemDTOValidator.validateRequest(dto);

        assertEquals(Long.toString(0), dtos.get(0).getUuid());
        assertEquals(entities.get(activate.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
        assertEquals(entities.get(7L).getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getHostedBySE());
    }

    @Test
    public void testActivatePM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(entityId,
                        EntityType.PHYSICAL_MACHINE))
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setActivate(activate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.physicalMachine(entityId));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(), Mockito.eq(Collections.singleton(1L)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(Long.toString(0), dtos.get(0).getUuid());
        assertEquals(entities.get(activate.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
    }

    @Test
    public void testActivateStorage() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId, EntityType.STORAGE)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setActivate(activate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .setActionType(ActionDTO.ActionType.ACTIVATE)
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.storage(entityId));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(), Mockito.eq(Collections.singleton(1L)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(Long.toString(0), dtos.get(0).getUuid());
        assertEquals(entities.get(activate.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
    }

    @Test
    public void testActivateMissingEntity() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setActivate(activate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // Empty topology.
        initializeTopology(targetId);

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 1 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testActivateVMMissingHost() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setActivate(activate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // No host for VM topology.
        initializeTopology(targetId, NewEntityRequest.virtualMachine(entityId, entityId + 1));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 2 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testDeactivateVM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setDeactivate(deactivate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(deactivate.getTarget().getId(), 7),
                NewEntityRequest.physicalMachine(7));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(),
                Mockito.eq(Collections.singleton(entityId)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(Long.toString(0), dtos.get(0).getUuid());
        assertEquals(entities.get(deactivate.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
        assertEquals(entities.get(7L).getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getHostedBySE());
    }

    @Test
    public void testDeactivatePM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(entityId,
                        EntityType.PHYSICAL_MACHINE))
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setDeactivate(deactivate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.physicalMachine(entityId));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(),
                Mockito.eq(Collections.singleton(entityId)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(Long.toString(0), dtos.get(0).getUuid());
        assertEquals(entities.get(deactivate.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
    }

    @Test
    public void testDeactivateStorage() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId, EntityType.STORAGE)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setDeactivate(deactivate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .setActionType(ActionDTO.ActionType.DEACTIVATE)
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.storage(entityId));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(actionExecutionCaptor.capture(),
                Mockito.eq(targetId), Mockito.anyLong(),
                Mockito.eq(Collections.singleton(entityId)));

        assertEquals(request.getActionId(), actionExecutionCaptor.getValue().getActionOid());
        final List<ActionItemDTO> dtos = actionExecutionCaptor.getValue().getActionItemList();

        ActionItemDTOValidator.validateRequest(dtos);

        assertEquals(Long.toString(0), dtos.get(0).getUuid());
        assertEquals(entities.get(deactivate.getTarget().getId())
                .getTargetInfo(targetId)
                .get()
                .getEntityInfo(), dtos.get(0).getTargetSE());
    }

    @Test
    public void testDeactivateMissingEntity() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setDeprecatedImportance(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setInfo(ActionInfo.newBuilder().setDeactivate(deactivate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // Empty topology.
        initializeTopology(targetId);

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 1 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testDeactivateVMMissingHost() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder().setTarget(
                ActionExecutionTestUtils.createActionEntity(entityId)).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionSpec(ActionDTO.ActionSpec.newBuilder()
                    .setRecommendation(ActionDTO.Action.newBuilder().setId(0)
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setInfo(ActionInfo.newBuilder().setDeactivate(deactivate)))
                    .setActionState(ActionState.IN_PROGRESS).build())
                .build();

        // No host for VM topology.
        initializeTopology(targetId, NewEntityRequest.virtualMachine(entityId, entityId + 1));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("entitydata for entity 2 could not be retrieved"));
        actionExecutionStub.executeAction(request);
    }

    private Map<Long, Entity> initializeTopology(final long targetId, NewEntityRequest... entities)
            throws Exception {
        return Arrays.stream(entities).map(entityRequest -> {
            final Entity entity = new Entity(entityRequest.id, entityRequest.entityType);
            final EntityDTO entityDTO = EntityDTO.newBuilder().setEntityType(
                    entityRequest.entityType).setId(Long.toString(entityRequest.id)).build();
            entity.addTargetInfo(targetId, entityDTO);
            if (entityRequest.entityType == EntityType.VIRTUAL_MACHINE) {
                entityRequest.hostPm.ifPresent(hostId -> entity.setHostedBy(targetId, hostId));
            }
            Mockito.when(entityStore.getEntity(entityRequest.id)).thenReturn(Optional.of(entity));
            try {
                // This is expressed as a doReturn instead of a when...thenReturn so that
                // it does not invoke the default behavior that has already been stubbed for
                // this method (which is to trigger an exception).
                Mockito.doReturn(entityDTO).when(entityRetriever).fetchAndConvertToEntityDTO(
                        entityRequest.id);
            } catch (EntityRetrievalException e) {
                throw new RuntimeException(e);
            }
            return entity;
        }).collect(Collectors.toMap(Entity::getId, Function.identity()));
    }

    /**
     * Request for an entity in the mock topology.
     *
     * @return OID of a target mocked.
     */
    private long createTarget() {
        final long targetId = targetIdCounter.getAndIncrement();

        final Target target = Mockito.mock(Target.class);
        Mockito.when(targetStoreMock.getTarget(targetId)).thenReturn(Optional.of(target));
        final ActionPolicyDTO moveActionPolicy = SdkActionPolicyBuilder.build(
                ActionCapability.SUPPORTED, EntityType.VIRTUAL_MACHINE,
                ActionType.CROSS_TARGET_MOVE);

        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.HYPERVISOR.toString())
                .setProbeType(SDKProbeType.VCENTER.toString())
                .setUiProbeCategory(ProbeCategory.HYPERVISOR.toString())
                .addActionPolicy(moveActionPolicy)
                .build();
        Mockito.when(probeStoreMock.getProbe(targetStoreMock.getTarget(targetId)
                .get()
                .getProbeId())).thenReturn(Optional.of(probeInfo));
        return targetId;
    }

    private static class NewEntityRequest {
        final long id;
        final Optional<Long> hostPm;
        final EntityType entityType;

        private NewEntityRequest(final long entityId, final Optional<Long> hostPm,
                final EntityType entityType) {
            this.id = entityId;
            this.hostPm = hostPm;
            this.entityType = entityType;
        }

        static NewEntityRequest virtualMachine(final long entityId, final long hostPm) {
            return new NewEntityRequest(entityId, Optional.of(hostPm), EntityType.VIRTUAL_MACHINE);
        }

        static NewEntityRequest physicalMachine(final long entityId) {
            return new NewEntityRequest(entityId, Optional.empty(), EntityType.PHYSICAL_MACHINE);
        }

        static NewEntityRequest storage(final long entityId) {
            return new NewEntityRequest(entityId, Optional.empty(), EntityType.STORAGE);
        }

        static NewEntityRequest diskArray(final long entityId) {
            return new NewEntityRequest(entityId, Optional.empty(), EntityType.DISK_ARRAY);
        }
    }
}
