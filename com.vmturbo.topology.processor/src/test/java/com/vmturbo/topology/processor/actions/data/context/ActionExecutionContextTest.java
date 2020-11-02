package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.topology.processor.actions.ActionExecutionTestUtils;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class ActionExecutionContextTest {

    private final ActionDataManager actionDataManagerMock = Mockito.mock(ActionDataManager.class);

    private final EntityStore entityStoreMock = Mockito.mock(EntityStore.class);

    private final EntityRetriever entityRetrieverMock = Mockito.mock(EntityRetriever.class);

    private final TargetStore targetStoreMock = Mockito.mock(TargetStore.class);

    private final ProbeStore probeStoreMock = Mockito.mock(ProbeStore.class);


    // Builds the class under test
    private ActionExecutionContextFactory actionExecutionContextFactory;

    @Before
    public void setup() {
        Target target = Mockito.mock(Target.class);
        Mockito.when(target.getProbeId()).thenReturn(555L);
        Mockito.when(targetStoreMock.getTarget(Mockito.anyLong())).thenReturn(Optional.of(target));
        Mockito.when(probeStoreMock.getProbe(555L))
            .thenReturn(Optional.of(MediationMessage.ProbeInfo.getDefaultInstance()));
        actionExecutionContextFactory = new ActionExecutionContextFactory(
                actionDataManagerMock,
                entityStoreMock,
                entityRetrieverMock,
                targetStoreMock,
                probeStoreMock);
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
                .setActionType(ActionDTO.ActionType.ACTIVATE)
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
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

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
                .setActionType(ActionDTO.ActionType.DEACTIVATE)
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
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

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
    public void testResizeContext() throws Exception {
        // Construct an resize action request
        final long entityId = 35;
        final float oldCapacity = 2000;
        final float newCapacity = 3000;
        final ActionDTO.ActionInfo resize = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.VMEM_VALUE)
                        .build())
                    .setHotAddSupported(true)
                    .setNewCapacity(newCapacity)
                    .setOldCapacity(oldCapacity)
                    .setCommodityAttribute(CommodityAttribute.CAPACITY))
                .build();
        final int targetId = 13;
        final int actionId = 5;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(resize)
                .setActionType(ActionDTO.ActionType.RESIZE)
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
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        // TODO Update this after addressing the TODO in ResizeContext relating to the discrepancy
        //      between RESIZE vs RIGHT_SIZE. Some probes expect one, while others expect the other.
        Assert.assertEquals(ActionType.RIGHT_SIZE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // verify resize commodity value and attributes like: hotAddSupported
        ActionItemDTO actionItemDTO = actionExecutionContext.getActionItems().get(0);
        Assert.assertEquals(ActionItemDTO.CommodityAttribute.Capacity, actionItemDTO.getCommodityAttribute());
        Assert.assertEquals(CommodityDTO.CommodityType.VMEM, actionItemDTO.getCurrentComm().getCommodityType());
        Assert.assertEquals(oldCapacity, actionItemDTO.getCurrentComm().getCapacity(), 0);
        Assert.assertEquals(newCapacity, actionItemDTO.getNewComm().getCapacity(), 0);
        Assert.assertTrue(actionItemDTO.getCurrentComm().getVmemData().getHotAddSupported());
        Assert.assertTrue(actionItemDTO.getNewComm().getVmemData().getHotAddSupported());

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
                .setActionType(ActionDTO.ActionType.PROVISION)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(provision))
                .thenReturn(Collections.emptyList());

        // The XL-domain TopologyEntityDTO will be retrieved, before later being converted into
        // an EntityDTO.
        final EntityType entityType = EntityType.PHYSICAL_MACHINE;
        TopologyEntityDTO primaryTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(entityId)
                .setEntityType(entityType.getNumber())
                .build();

        // We need entity info for just the primary entity -- physical machines don't have hosts
        final Entity entity = new Entity(entityId, entityType);
        final EntityDTO entityDTO = EntityDTO.newBuilder()
                .setEntityType(entityType)
                .setId(Long.toString(entityId))
                .build();
        entity.addTargetInfo(targetId, entityDTO);

        // The XL-domain TopologyEntityDTO will be retrieved, before later being converted into
        // an EntityDTO.
        Mockito.when(entityRetrieverMock.retrieveTopologyEntity(entityId))
                .thenReturn(Optional.of(primaryTopologyEntityDTO));
        Mockito.when(entityRetrieverMock.convertToEntityDTO(primaryTopologyEntityDTO))
                .thenReturn(entityDTO);

        // Construct a provision action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Provision actions should have exactly one actionItem
        Assert.assertEquals(1, actionExecutionContext.getActionItems().size());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionType.PROVISION, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the entityInfo was not retrieved (used only for setting the host field)
        // The reason this has zero interactions is that physical machines don't have the host field set
        Mockito.verifyZeroInteractions(entityStoreMock);

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).retrieveTopologyEntity(entityId);
        Mockito.verify(entityRetrieverMock).convertToEntityDTO(primaryTopologyEntityDTO);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(provision);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

}
