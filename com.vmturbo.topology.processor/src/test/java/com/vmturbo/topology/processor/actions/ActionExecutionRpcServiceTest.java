package com.vmturbo.topology.processor.actions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import io.grpc.Status.Code;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.operation.OperationManager;

public class ActionExecutionRpcServiceTest {
    private EntityStore entityStore = Mockito.mock(EntityStore.class);

    private OperationManager operationManager = Mockito.mock(OperationManager.class);

    private ActionExecutionRpcService actionExecutionBackend =
            new ActionExecutionRpcService(entityStore, operationManager);

    @Captor
    private ArgumentCaptor<List<ActionItemDTO>> actionItemDTOCaptor;

    private AtomicLong targetIdCounter = new AtomicLong();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionExecutionServiceBlockingStub actionExecutionStub;

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(actionExecutionBackend);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        Mockito.when(entityStore.getEntity(Mockito.anyLong())).thenReturn(Optional.empty());

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
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Move moveSpec = ActionDTO.Move.newBuilder()
                .setTargetId(1)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(2)
                    .setDestinationId(3)
                    .build())
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(4)
                    .setDestinationId(5)
                    .build())
                .build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                    .setMove(moveSpec))
                .build();

        List<ChangeProvider> changes = moveSpec.getChangesList();
        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(moveSpec.getTargetId(), changes.get(0).getSourceId()),
                NewEntityRequest.physicalMachine(changes.get(0).getSourceId()),
                NewEntityRequest.physicalMachine(changes.get(0).getDestinationId()),
                NewEntityRequest.storage(changes.get(1).getSourceId()),
                NewEntityRequest.storage(changes.get(1).getDestinationId()));


        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

        ActionItemDTOValidator.validateRequest(dtos);

        Assert.assertEquals(
                entities.get(moveSpec.getTargetId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getSourceId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getHostedBySE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getSourceId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getCurrentSE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getDestinationId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getNewSE());
    }

    /**
     * Check that there is no call to the {@link OperationManager} if one of
     * the entities is missing.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMoveMissingDestinationEntity() throws Exception {

        final long targetId = targetIdCounter.getAndIncrement();
        final ActionDTO.Move move = ActionDTO.Move.newBuilder()
                .setTargetId(1)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(2)
                    .setDestinationId(3)
                    .build())
                .build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setTargetId(targetId)
                .setActionId(0L)
                .setActionInfo(ActionInfo.newBuilder()
                    .setMove(move))
                .build();

        // Entity with ID 3 (destination) is missing.
        initializeTopology(targetId,
                NewEntityRequest.virtualMachine(move.getTargetId(), move.getChanges(0).getSourceId()),
                NewEntityRequest.physicalMachine(move.getChanges(0).getSourceId()));

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing destination"));
        actionExecutionStub.executeAction(request);
    }

    /**
     * Check that there is no call to the {@link OperationManager} if all three entities
     * are not on the same target.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMoveIncompatibleTargets() throws Exception {
        final long targetId1 = targetIdCounter.getAndIncrement();

        final ActionDTO.Move move = ActionDTO.Move.newBuilder()
                .setTargetId(1)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(2)
                    .setDestinationId(3)
                    .build())
                .build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setTargetId(targetId1)
                .setActionId(0L)
                .setActionInfo(ActionInfo.newBuilder()
                        .setMove(move))
                .build();

        // Target and source are on the same target
        initializeTopology(targetId1,
                NewEntityRequest.virtualMachine(move.getTargetId(), move.getChanges(0).getSourceId()),
                NewEntityRequest.physicalMachine(move.getChanges(0).getSourceId()));

        // Destination is on a different target
        initializeTopology(
                targetIdCounter.getAndIncrement(),
                NewEntityRequest.physicalMachine(move.getChanges(0).getDestinationId()));

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing target info"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testStorageMove() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Move moveSpec = ActionDTO.Move.newBuilder()
                .setTargetId(1)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(2)
                    .setDestinationId(3)
                    .build())
                .build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setMove(moveSpec))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(moveSpec.getTargetId(), moveSpec.getChanges(0).getSourceId()),
                NewEntityRequest.storage(moveSpec.getChanges(0).getSourceId()),
                NewEntityRequest.storage(moveSpec.getChanges(0).getDestinationId()));


        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

        ActionItemDTOValidator.validateRequest(dtos);

        Assert.assertEquals(
                entities.get(moveSpec.getTargetId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getSourceId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getHostedBySE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getSourceId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getCurrentSE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getDestinationId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getNewSE());
    }

    @Test
    public void testDatastoreMove() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Move moveSpec = ActionDTO.Move.newBuilder()
                .setTargetId(1)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(2)
                    .setDestinationId(3)
                    .build())
                .build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setMove(moveSpec))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.storage(moveSpec.getTargetId()),
                NewEntityRequest.diskArray(moveSpec.getChanges(0).getSourceId()),
                NewEntityRequest.diskArray(moveSpec.getChanges(0).getDestinationId()));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

        ActionItemDTOValidator.validateRequest(dtos);

        Assert.assertEquals(
                entities.get(moveSpec.getTargetId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
        Assert.assertFalse(dtos.get(0).hasHostedBySE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getSourceId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getCurrentSE());
        Assert.assertEquals(
                entities.get(moveSpec.getChanges(0).getDestinationId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getNewSE());
    }

    @Test
    public void testMoveIncompatibleSourceAndDestination() throws Exception {
        final long targetId1 = targetIdCounter.getAndIncrement();

        final ActionDTO.Move move = ActionDTO.Move.newBuilder()
                .setTargetId(1)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(2)
                    .setDestinationId(3)
                    .build())
                .build();
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setTargetId(targetId1)
                .setActionId(0L)
                .setActionInfo(ActionInfo.newBuilder()
                        .setMove(move))
                .build();

        initializeTopology(targetId1,
                NewEntityRequest.virtualMachine(move.getTargetId(), move.getChanges(0).getSourceId()),
                NewEntityRequest.physicalMachine(move.getChanges(0).getSourceId()),
                // Destination is a storage, but source is a PM.
                NewEntityRequest.storage(move.getChanges(0).getDestinationId()));

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Mismatched source and destination"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testResize() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Resize resizeSpec = ActionDTO.Resize.newBuilder()
                .setTargetId(1)
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE)
                    .setKey("key"))
                .setOldCapacity(10)
                .setNewCapacity(20)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setResize(resizeSpec))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                        NewEntityRequest.virtualMachine(resizeSpec.getTargetId(), 7),
                        NewEntityRequest.physicalMachine(7));


        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

//        ActionItemDTOValidator.validateRequest(dto);

        Assert.assertEquals(
                entities.get(resizeSpec.getTargetId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
        Assert.assertEquals(
                CommodityAttribute.Capacity,
                dtos.get(0).getCommodityAttribute());
        Assert.assertEquals(CommodityDTO.CommodityType.MEM, dtos.get(0).getCurrentComm().getCommodityType());
        Assert.assertEquals(
                10,
                dtos.get(0).getCurrentComm().getCapacity(), 0);
        Assert.assertEquals(CommodityDTO.CommodityType.MEM, dtos.get(0).getNewComm().getCommodityType());
        Assert.assertEquals(
                20,
                dtos.get(0).getNewComm().getCapacity(), 0);
    }

    @Test
    public void testResizeMissingEntity() throws Exception {

        final long targetId = targetIdCounter.getAndIncrement();
        final ActionDTO.Resize resizeSpec = ActionDTO.Resize.newBuilder()
                .setTargetId(1)
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE)
                        .setKey("key"))
                .setOldCapacity(10)
                .setNewCapacity(20)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setResize(resizeSpec))
                .build();

        // No entities in topology; target entity is missing.
        initializeTopology(targetId);

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing target"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testResizeVMMssingHost() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();

        final ActionDTO.Resize resizeSpec = ActionDTO.Resize.newBuilder()
                .setTargetId(1)
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.MEM_VALUE)
                        .setKey("key"))
                .setOldCapacity(10)
                .setNewCapacity(20)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setResize(resizeSpec))
                .build();

        // Include a virtual machine, but no host that matches the host ID.
        initializeTopology(targetId, NewEntityRequest.virtualMachine(resizeSpec.getTargetId(), 7));

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing host of target"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testActivateVM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setActivate(activate))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(activate.getTargetId(), 7),
                NewEntityRequest.physicalMachine(7));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

//        ActionItemDTOValidator.validateRequest(dto);

        Assert.assertEquals(Long.toString(0), dtos.get(0).getUuid());
        Assert.assertEquals(
                entities.get(activate.getTargetId()).getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
        Assert.assertEquals(
                entities.get(7L).getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getHostedBySE());
    }

    @Test
    public void testActivatePM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setActivate(activate))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.physicalMachine(entityId));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

        ActionItemDTOValidator.validateRequest(dtos);

        Assert.assertEquals(Long.toString(0), dtos.get(0).getUuid());
        Assert.assertEquals(
                entities.get(activate.getTargetId()).getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
    }

    @Test
    public void testActivateMissingEntity() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setActivate(activate))
                .build();

        // Empty topology.
        initializeTopology(targetId);

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing target"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testActivateVMMissingHost() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Activate activate = ActionDTO.Activate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setActivate(activate))
                .build();

        // No host for VM topology.
         initializeTopology(targetId,
                NewEntityRequest.virtualMachine(entityId, entityId + 1));

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing host of target"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testDeactivateVM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setDeactivate(deactivate))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.virtualMachine(deactivate.getTargetId(), 7),
                NewEntityRequest.physicalMachine(7));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

        ActionItemDTOValidator.validateRequest(dtos);

        Assert.assertEquals(Long.toString(0), dtos.get(0).getUuid());
        Assert.assertEquals(
                entities.get(deactivate.getTargetId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());
        Assert.assertEquals(
                entities.get(7L).getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getHostedBySE());
    }

    @Test
    public void testDeactivatePM() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setDeactivate(deactivate))
                .build();

        final Map<Long, Entity> entities = initializeTopology(targetId,
                NewEntityRequest.physicalMachine(entityId));

        actionExecutionStub.executeAction(request);

        Mockito.verify(operationManager).requestActions(Mockito.eq(request.getActionId()),
                Mockito.eq(targetId), actionItemDTOCaptor.capture());

        final List<ActionItemDTO> dtos = actionItemDTOCaptor.getValue();

        ActionItemDTOValidator.validateRequest(dtos);

        Assert.assertEquals(Long.toString(0), dtos.get(0).getUuid());
        Assert.assertEquals(
                entities.get(deactivate.getTargetId())
                        .getTargetInfo(targetId).get().getEntityInfo(),
                dtos.get(0).getTargetSE());

    }

    @Test
    public void testDeactivateMissingEntity() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setDeactivate(deactivate))
                .build();

        // Empty topology.
        initializeTopology(targetId);

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing target"));
        actionExecutionStub.executeAction(request);
    }

    @Test
    public void testDeactivateVMMissingHost() throws Exception {
        final long targetId = targetIdCounter.getAndIncrement();
        final long entityId = 1;

        final ActionDTO.Deactivate deactivate = ActionDTO.Deactivate.newBuilder()
                .setTargetId(entityId)
                .build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0)
                .setTargetId(targetId)
                .setActionInfo(ActionInfo.newBuilder()
                        .setDeactivate(deactivate))
                .build();

        // No host for VM topology.
        initializeTopology(targetId,
                NewEntityRequest.virtualMachine(entityId, entityId + 1));

        expectedException.expect(GrpcRuntimeExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing host of target"));
        actionExecutionStub.executeAction(request);
    }

    private Map<Long, Entity> initializeTopology(final long targetId,
                                                 NewEntityRequest... entities)
            throws Exception {
        return Arrays.stream(entities)
                .map(entityRequest -> {
                    final Entity entity = new Entity(entityRequest.id,
                        entityRequest.entityType);
                    entity.addTargetInfo(targetId, EntityDTO.newBuilder()
                            .setEntityType(entityRequest.entityType)
                            .setId(Long.toString(entityRequest.id))
                            .build());
                    if (entityRequest.entityType == EntityType.VIRTUAL_MACHINE) {
                        entityRequest.hostPm.ifPresent(hostId ->
                                entity.setHostedBy(targetId, hostId));
                    }
                    Mockito.when(entityStore.getEntity(entityRequest.id))
                           .thenReturn(Optional.of(entity));
                    return entity;
                })
                .collect(Collectors.toMap(Entity::getId, Function.identity()));
    }

    /**
     * Request for an entity in the mock topology.
     */
    private static class NewEntityRequest {
        final long id;
        final Optional<Long> hostPm;
        final EntityType entityType;

        private NewEntityRequest(final long entityId,
                                 final Optional<Long> hostPm,
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
