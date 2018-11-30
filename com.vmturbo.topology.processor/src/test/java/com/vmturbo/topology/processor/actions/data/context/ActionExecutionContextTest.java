package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.ActionExecutionTestUtils;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetStore;

public class ActionExecutionContextTest {

    private final ActionDataManager actionDataManagerMock = Mockito.mock(ActionDataManager.class);

    private final EntityStore entityStoreMock = Mockito.mock(EntityStore.class);

    private final EntityRetriever entityRetrieverMock = Mockito.mock(EntityRetriever.class);

    private final TargetStore targetStoreMock = Mockito.mock(TargetStore.class);

    // Builds the class under test
    private ActionExecutionContextFactory actionExecutionContextFactory;

    @Before
    public void setup() {
        actionExecutionContextFactory = new ActionExecutionContextFactory(
                actionDataManagerMock,
                entityStoreMock,
                entityRetrieverMock,
                targetStoreMock);
    }

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
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(targetId, entityDTO);
        final int hostEntityId = 82;
        entity.setHostedBy(targetId, hostEntityId);

        final EntityType hostEntityType = EntityType.PHYSICAL_MACHINE;
        final EntityDTO hostEntityDTO = EntityDTO.newBuilder()
                .setEntityType(hostEntityType)
                .setId(Long.toString(hostEntityId))
                .build();

        // Retrieve the raw entity info (used only for setting the host field)
        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        // Retrieve the full entity info
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(hostEntityId))
                .thenReturn(hostEntityDTO);

        // Construct an activate action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Activate actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.ACTIVATE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.START, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the raw entityInfo was retrieved (used only for setting the host field)
        Mockito.verify(entityStoreMock).getEntity(entityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(entityId);
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(hostEntityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

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
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(targetId, entityDTO);

        // Retrieve the full entity info
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);

        // Construct a deactivate action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Deactivate actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.DEACTIVATE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.SUSPEND, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was not retrieved (used only for setting the host field)
        // The reason this has zero interactions is that physical machines don't have the host field set
        Mockito.verifyZeroInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(entityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

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
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(targetId, entityDTO);
        // Build the source provider entity
        entity.setHostedBy(targetId, sourceEntityId);
        final Entity sourceEntity = new Entity(sourceEntityId, sourceEntityType);
        final EntityDTO sourceEntityDTO = EntityDTO.newBuilder()
                .setEntityType(sourceEntityType)
                .setId(Long.toString(sourceEntityId))
                .build();
        sourceEntity.addTargetInfo(targetId, sourceEntityDTO);
        // Build the destination provider entity
        entity.setHostedBy(targetId, sourceEntityId);
        final Entity destinationEntity = new Entity(destinationEntityId, destinationEntityType);
        final EntityDTO destinationEntityDTO = EntityDTO.newBuilder()
                .setEntityType(destinationEntityType)
                .setId(Long.toString(destinationEntityId))
                .build();
        destinationEntity.addTargetInfo(targetId, destinationEntityDTO);

        // Retrieve the raw entity info (used only for setting the host field)
        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        // Raw destination entity is retrieved to check for cross target move
        Mockito.when(entityStoreMock.getEntity(destinationEntityId))
                .thenReturn(Optional.of(destinationEntity));
        // Retrieve the full entity info
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(sourceEntityId))
                .thenReturn(sourceEntityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(destinationEntityId))
                .thenReturn(destinationEntityDTO);

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have at least one actionItem
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.MOVE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.MOVE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the raw entityInfo was retrieved (used only for setting the host field)
        Mockito.verify(entityStoreMock).getEntity(entityId);
        // Check that the destination entityInfo was retrieved (used for detecting cross target move)
        Mockito.verify(entityStoreMock).getEntity(destinationEntityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(entityId);
        // Source entity info will be retrieved once while building the actionItem and again for
        // setting the hostedBy flag
        Mockito.verify(entityRetrieverMock, Mockito.times(2)).fetchAndConvertToEntityDTO(sourceEntityId);
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(destinationEntityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(move);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

    @Test
    public void testCrossTargetMoveContext() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final int sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.PHYSICAL_MACHINE;
        final int destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.PHYSICAL_MACHINE;
        final int storageEntityId = 14;
        final EntityType storageEntityType = EntityType.STORAGE;
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
        final int primaryTargetId = 2;
        // In a cross-target move, the destination entity was discovered by a second target
        final int secondaryTargetId = 3;
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(primaryTargetId)
                .setActionInfo(move)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
                .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = new Entity(entityId, entityType);
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(primaryTargetId, entityDTO);
        // Build the source provider entity
        entity.setHostedBy(primaryTargetId, sourceEntityId);
        final Entity sourceEntity = new Entity(sourceEntityId, sourceEntityType);
        final EntityDTO sourceEntityDTO = EntityDTO.newBuilder()
                .setEntityType(sourceEntityType)
                .setId(Long.toString(sourceEntityId))
                .build();
        sourceEntity.addTargetInfo(primaryTargetId, sourceEntityDTO);
        // Build the destination provider entity
        entity.setHostedBy(primaryTargetId, sourceEntityId);
        final Entity destinationEntity = new Entity(destinationEntityId, destinationEntityType);
        final EntityDTO destinationEntityDTO = EntityDTO.newBuilder()
                .setEntityType(destinationEntityType)
                .setId(Long.toString(destinationEntityId))
                .build();
        // In a cross-target move, the destination entity was discovered by a second target
        destinationEntity.addTargetInfo(secondaryTargetId, destinationEntityDTO);

        // For a cross-target move, we also need entity info for the storage
        final Entity storageEntity = new Entity(storageEntityId, storageEntityType);
        final EntityDTO storageEntityDTO = EntityDTO.newBuilder()
                .setEntityType(storageEntityType)
                .setId(Long.toString(storageEntityId))
                .build();
        storageEntity.addTargetInfo(primaryTargetId, storageEntityDTO);
        // We need to provide a mocked TopologyEntityDTO for the primary entity because that is how
        // the cross-target logic will find the storage entity to retrieve
        TopologyEntityDTO primaryTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(entityId)
                .setEntityType(entityType.getNumber())
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(storageEntityId)
                                .setProviderEntityType(storageEntityType.getNumber()))
                .build();
        Mockito.when(entityRetrieverMock.retrieveTopologyEntity(entityId))
                .thenReturn(Optional.of(primaryTopologyEntityDTO));

        // Configure the mock for the targetStore, which gets called when determing secondary target ID
        // This is something that currently only happens during a cross-target move
        // The key is seeing whether the primary and secondary target are the same type
        Mockito.when(targetStoreMock.getProbeTypeForTarget(primaryTargetId))
                .thenReturn(Optional.of(SDKProbeType.VCENTER));
        Mockito.when(targetStoreMock.getProbeTypeForTarget(secondaryTargetId))
                .thenReturn(Optional.of(SDKProbeType.VCENTER));

        // Retrieve the raw entity info (used only for setting the host field)
        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        // Raw destination entity is retrieved to check for cross target move
        Mockito.when(entityStoreMock.getEntity(destinationEntityId))
                .thenReturn(Optional.of(destinationEntity));
        // Retrieve the full entity info
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(sourceEntityId))
                .thenReturn(sourceEntityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(destinationEntityId))
                .thenReturn(destinationEntityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(storageEntityId))
                .thenReturn(storageEntityDTO);

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have at least one actionItem
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        // The XL ActionType does not have a separate entry for CROSS_TARGET_MOVE like the SDK one does
        Assert.assertEquals(ActionDTO.ActionType.MOVE, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.CROSS_TARGET_MOVE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(primaryTargetId, actionExecutionContext.getTargetId());
        Assert.assertEquals(secondaryTargetId,
                actionExecutionContext.getSecondaryTargetId().longValue());

        // Check that the raw entityInfo was retrieved (used only for setting the host field)
        Mockito.verify(entityStoreMock, Mockito.atLeastOnce()).getEntity(entityId);
        // Check that the destination entityInfo was retrieved (used for detecting cross target move)
        Mockito.verify(entityStoreMock, Mockito.atLeastOnce()).getEntity(destinationEntityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock, Mockito.atLeastOnce()).fetchAndConvertToEntityDTO(entityId);
        // Source entity info will be retrieved once while building the actionItem and again for
        // setting the hostedBy flag
        Mockito.verify(entityRetrieverMock, Mockito.atLeastOnce()).fetchAndConvertToEntityDTO(sourceEntityId);
        Mockito.verify(entityRetrieverMock, Mockito.atLeastOnce()).fetchAndConvertToEntityDTO(destinationEntityId);
        // Cross target moves require additional lookup to detemine the storage entities
        Mockito.verify(entityRetrieverMock, Mockito.atLeastOnce()).fetchAndConvertToEntityDTO(storageEntityId);
        Mockito.verify(entityRetrieverMock, Mockito.atLeastOnce()).retrieveTopologyEntity(entityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock, Mockito.atLeastOnce()).getContextData(move);
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
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(targetId, entityDTO);
        final int hostEntityId = 42;
        entity.setHostedBy(targetId, hostEntityId);

        final EntityType hostEntityType = EntityType.PHYSICAL_MACHINE;
        final Entity hostEntity = new Entity(hostEntityId, hostEntityType);
        final EntityDTO hostEntityDTO = EntityDTO.newBuilder()
                .setEntityType(hostEntityType)
                .setId(Long.toString(hostEntityId))
                .build();
        hostEntity.addTargetInfo(targetId, hostEntityDTO);

        // Retrieve the raw entity info (used only for setting the host field)
        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        // Retrieve the full entity info
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(hostEntityId))
                .thenReturn(hostEntityDTO);

        // Construct a resize action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

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

        // Check that the raw entityInfo was retrieved (used only for setting the host field)
        Mockito.verify(entityStoreMock).getEntity(entityId);
        Mockito.verifyNoMoreInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(entityId);
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(hostEntityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

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
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(targetId, entityDTO);

        // Retrieve the full entity info
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);

        // Construct a provision action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Provision actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionDTO.ActionType.PROVISION, actionExecutionContext.getActionType());
        Assert.assertEquals(ActionType.PROVISION, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was not retrieved (used only for setting the host field)
        // The reason this has zero interactions is that physical machines don't have the host field set
        Mockito.verifyZeroInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(entityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(provision);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

}
