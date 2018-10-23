package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collections;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.matchers.GreaterThan;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.ActionExecutionTestUtils;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;

public class ActionExecutionContextTest {

    private ActionDataManager actionDataManagerMock = Mockito.mock(ActionDataManager.class);

    private EntityStore entityStoreMock = Mockito.mock(EntityStore.class);

    @Test
    public void testActivateContext() throws Exception {
        // Construct an activate action request
        final long entityId = 6;
        final ActionDTO.ActionInfo activate = ActionInfo.newBuilder()
                .setActivate(ActionDTO.Activate.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId)))
                .build();
        final int targetId = 11;
        final int actionId = 32;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(activate)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(activate))
                .thenReturn(Collections.emptyList());

        // We need entity info for both the primary entity and its host
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = new Entity(entityId, entityType);
        entity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build());
        final int hostEntityId = 82;
        entity.setHostedBy(targetId, hostEntityId);

        final EntityType hostEntityType = EntityType.PHYSICAL_MACHINE;
        final Entity hostEntity = new Entity(hostEntityId, hostEntityType);
        hostEntity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(hostEntityType)
                .setId(Long.toString(hostEntityId))
                .build());

        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        Mockito.when(entityStoreMock.getEntity(hostEntityId)).thenReturn(Optional.of(hostEntity));

        // Construct an activate action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext = ActionExecutionContextFactory
                .getActionExecutionContext(request, actionDataManagerMock, entityStoreMock);

        // Activate actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.ACTIVATE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.START, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was retrieved
        Mockito.verify(entityStoreMock).getEntity(entityId);
        Mockito.verify(entityStoreMock).getEntity(hostEntityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(activate);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

    @Test
    public void testDeactivateContext() throws Exception {
        // Construct an deactivate action request
        final long entityId = 26;
        final ActionDTO.ActionInfo deactivate = ActionInfo.newBuilder()
                .setDeactivate(ActionDTO.Deactivate.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId)))
                .build();
        final int targetId = 13;
        final int actionId = 40;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(deactivate)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(deactivate))
                .thenReturn(Collections.emptyList());

        // We need entity info for just the primary entity -- physical machines don't have hosts
        final EntityType entityType = EntityType.PHYSICAL_MACHINE;
        final Entity entity = new Entity(entityId, entityType);
        entity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build());

        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));

        // Construct a deactivate action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext = ActionExecutionContextFactory
                .getActionExecutionContext(request, actionDataManagerMock, entityStoreMock);

        // Deactivate actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.DEACTIVATE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.SUSPEND, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was retrieved
        Mockito.verify(entityStoreMock).getEntity(entityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(deactivate);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

    @Test
    public void testMoveContext() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final int sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.PHYSICAL_MACHINE;
        final int destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.PHYSICAL_MACHINE;
        final ActionDTO.ActionInfo move = ActionInfo.newBuilder()
                .setMove(ActionDTO.Move.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(ActionExecutionTestUtils
                                        .createActionEntity(sourceEntityId, sourceEntityType))
                                .setDestination(ActionExecutionTestUtils
                                        .createActionEntity(destinationEntityId, destinationEntityType))
                                .build()))
                .build();
        final int targetId = 2;
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(move)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
                .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = new Entity(entityId, entityType);
        entity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build());
        // Build the source provider entity
        entity.setHostedBy(targetId, sourceEntityId);
        final Entity sourceEntity = new Entity(sourceEntityId, sourceEntityType);
        sourceEntity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(sourceEntityType)
                .setId(Long.toString(sourceEntityId))
                .build());
        // Build the destination provider entity
        entity.setHostedBy(targetId, sourceEntityId);
        final Entity destinationEntity = new Entity(destinationEntityId, destinationEntityType);
        destinationEntity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(destinationEntityType)
                .setId(Long.toString(destinationEntityId))
                .build());

        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        Mockito.when(entityStoreMock.getEntity(sourceEntityId))
                .thenReturn(Optional.of(sourceEntity));
        Mockito.when(entityStoreMock.getEntity(destinationEntityId))
                .thenReturn(Optional.of(destinationEntity));

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext = ActionExecutionContextFactory
                .getActionExecutionContext(request, actionDataManagerMock, entityStoreMock);

        // Move actions should have at least one actionItem
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.MOVE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.MOVE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was retrieved
        Mockito.verify(entityStoreMock).getEntity(entityId);
        // Source entity info will be retrieved once while building the actionItem and again for
        // setting the hostedBy flag
        Mockito.verify(entityStoreMock, Mockito.times(2)).getEntity(sourceEntityId);
        // Destination entity info will be retrieved once while building the actionItem and again
        // to determine if it is a cross target move.
        Mockito.verify(entityStoreMock, Mockito.times(2)).getEntity(destinationEntityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(move);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

    @Test
    public void testResizeContext() throws Exception {
        // Construct an resize action request
        final long entityId = 35;
        final ActionDTO.ActionInfo resize = ActionInfo.newBuilder()
                .setResize(ActionDTO.Resize.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId)))
                .build();
        final int targetId = 13;
        final int actionId = 5;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(resize)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(resize))
                .thenReturn(Collections.emptyList());

        // We need entity info for both the primary entity and its host
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = new Entity(entityId, entityType);
        entity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build());
        final int hostEntityId = 42;
        entity.setHostedBy(targetId, hostEntityId);

        final EntityType hostEntityType = EntityType.PHYSICAL_MACHINE;
        final Entity hostEntity = new Entity(hostEntityId, hostEntityType);
        hostEntity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(hostEntityType)
                .setId(Long.toString(hostEntityId))
                .build());

        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        Mockito.when(entityStoreMock.getEntity(hostEntityId)).thenReturn(Optional.of(hostEntity));

        // Construct a resize action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext = ActionExecutionContextFactory
                .getActionExecutionContext(request, actionDataManagerMock, entityStoreMock);

        // Resize actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.RESIZE, actionExecutionContext.getActionType());
        // TODO Update this after addressing the TODO in ResizeContext relating to the discrepancy
        //      between RESIZE vs RIGHT_SIZE. Some probes expect one, while others expect the other.
        Assert.assertEquals(ActionType.RIGHT_SIZE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was retrieved
        Mockito.verify(entityStoreMock).getEntity(entityId);
        Mockito.verify(entityStoreMock).getEntity(hostEntityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(resize);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

    @Test
    public void testProvisionContext() throws Exception {
        // Construct a provision action request
        final long entityId = 22;
        final ActionDTO.ActionInfo provision = ActionInfo.newBuilder()
                .setProvision(ActionDTO.Provision.newBuilder()
                        .setEntityToClone(ActionExecutionTestUtils.createActionEntity(entityId)))
                .build();
        final int targetId = 14;
        final int actionId = 41;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(provision)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(provision))
                .thenReturn(Collections.emptyList());

        // We need entity info for just the primary entity -- physical machines don't have hosts
        final EntityType entityType = EntityType.PHYSICAL_MACHINE;
        final Entity entity = new Entity(entityId, entityType);
        entity.addTargetInfo(targetId, EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build());

        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));

        // Construct a provision action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext = ActionExecutionContextFactory
                .getActionExecutionContext(request, actionDataManagerMock, entityStoreMock);

        // Provision actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.PROVISION, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.PROVISION, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was retrieved
        Mockito.verify(entityStoreMock).getEntity(entityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(provision);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

}
