package com.vmturbo.topology.processor.actions.data.context;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.actions.ActionExecutionTestUtils;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.SdkActionPolicyBuilder;

/**
 * Unit tests for {@link MoveContext} and {@link ScaleContext} classes.
 */
public class ChangeProviderContextTest {

    private final ActionDataManager actionDataManagerMock = Mockito.mock(ActionDataManager.class);

    private final EntityStore entityStoreMock = Mockito.mock(EntityStore.class);

    private final EntityRetriever entityRetrieverMock = Mockito.mock(EntityRetriever.class);

    private final TargetStore targetStoreMock = Mockito.mock(TargetStore.class);

    private final ProbeStore probeStoreMock = Mockito.mock(ProbeStore.class);

    private final int targetId = 2;

    private final int primaryTargetId = 2;
    // In a cross-target move, the destination entity was discovered by a second target
    private final int secondaryTargetId = 3;

    // Builds the class under test
    private ActionExecutionContextFactory actionExecutionContextFactory;

    @Before
    public void setup() {
        actionExecutionContextFactory = new ActionExecutionContextFactory(
                actionDataManagerMock,
                entityStoreMock,
                entityRetrieverMock,
                targetStoreMock,
                probeStoreMock);
        Mockito.when(targetStoreMock.getProbeTypeForTarget(targetId))
            .thenReturn(Optional.of(SDKProbeType.VCENTER));
        final Target target = Mockito.mock(Target.class);
        Mockito.when(targetStoreMock.getTarget(targetId))
            .thenReturn(Optional.of(target));
        final ActionPolicyDTO moveActionPolicy =
            SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.VIRTUAL_MACHINE,
                ActionType.CROSS_TARGET_MOVE);

        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.toString()).setProbeType(SDKProbeType.VCENTER.toString())
                .setUiProbeCategory(ProbeCategory.HYPERVISOR.toString())
            .addActionPolicy(moveActionPolicy)
            .build();
        Mockito.when(probeStoreMock.getProbe(targetStoreMock.getTarget(targetId).get().getProbeId())).thenReturn(Optional.of(probeInfo));
    }

    @Test
    public void testMoveContext_ChangeOnlyHost() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final int sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.PHYSICAL_MACHINE;
        final int destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.PHYSICAL_MACHINE;
        final ActionInfo move = ActionInfo.newBuilder()
                .setMove(ActionDTO.Move.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(ActionExecutionTestUtils
                                        .createActionEntity(sourceEntityId, sourceEntityType))
                                .setDestination(ActionExecutionTestUtils
                                        .createActionEntity(destinationEntityId, destinationEntityType))
                                .build()))
                .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(move)
                .setActionType(ActionDTO.ActionType.MOVE)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
                .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = mockEntity(entityId, entityType, targetId);
        entity.setHostedBy(targetId, sourceEntityId);
        // Build the source provider entity
        mockEntity(sourceEntityId, sourceEntityType, targetId);
        // Build the destination provider entity
        mockEntity(destinationEntityId, destinationEntityType, targetId);

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have at least one actionItem
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionType.MOVE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

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
    public void testMoveContext_ChangeOnlyStorage() throws Exception {
        // Construct an storage move (VM change) action request
        final long entityId = 22;
        final int sourceEntityId = 146;
        final EntityType sourceEntityType = EntityType.STORAGE;
        final int destinationEntityId = 153;
        final EntityType destinationEntityType = EntityType.STORAGE;
        final ActionInfo move = ActionInfo.newBuilder()
                .setMove(ActionDTO.Move.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(ActionExecutionTestUtils
                                        .createActionEntity(sourceEntityId, sourceEntityType))
                                .setDestination(ActionExecutionTestUtils
                                        .createActionEntity(destinationEntityId, destinationEntityType))
                                .build()))
                .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(move)
                .setActionType(ActionDTO.ActionType.MOVE)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
                .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = mockEntity(entityId, entityType, targetId);
        entity.setHostedBy(targetId, sourceEntityId);
        // Build the source provider entity
        mockEntity(sourceEntityId, sourceEntityType, targetId);
        // Build the destination provider entity
        mockEntity(destinationEntityId, destinationEntityType, targetId);

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have at least one actionItem
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        // Probes expect a storage move action to be reported as a CHANGE.
        // This is the key difference between this test and the normal move context test above
        Assert.assertEquals(ActionType.CHANGE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

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
    public void testCrossTargetMoveContext_ChangeOnlyHost() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final long sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.PHYSICAL_MACHINE;
        final long destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.PHYSICAL_MACHINE;
        final long storageEntityId = 14;
        final EntityType storageEntityType = EntityType.STORAGE;
        final ActionInfo move = ActionInfo.newBuilder()
                .setMove(ActionDTO.Move.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(ActionExecutionTestUtils
                                        .createActionEntity(sourceEntityId, sourceEntityType))
                                .setDestination(ActionExecutionTestUtils
                                        .createActionEntity(destinationEntityId, destinationEntityType))
                                .build()))
                .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(primaryTargetId)
                .setActionInfo(move)
                .setActionType(ActionDTO.ActionType.MOVE)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
                .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = mockEntity(entityId, entityType, primaryTargetId);
        entity.setHostedBy(primaryTargetId, sourceEntityId);
        // Build the source provider entity
        mockEntity(sourceEntityId, sourceEntityType, primaryTargetId);
        // Build the destination provider entity
        mockEntity(destinationEntityId, destinationEntityType, secondaryTargetId);
        // For a cross-target move, we also need entity info for the storage
        mockEntity(storageEntityId, storageEntityType, primaryTargetId);
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

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have 2 actionItems
        Assert.assertEquals(2, actionExecutionContext.getActionItems().size());
        Map<Long, ActionItemDTO> actionBySourceId = actionExecutionContext.getActionItems().stream()
            .collect(Collectors.toMap(actionItem -> Long.valueOf(actionItem.getCurrentSE().getId()),
                Function.identity()));

        ActionItemDTO hostActionItem = actionBySourceId.get(sourceEntityId);
        Assert.assertEquals(ActionType.MOVE, hostActionItem.getActionType());
        Assert.assertEquals(String.valueOf(sourceEntityId), hostActionItem.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(destinationEntityId), hostActionItem.getNewSE().getId());

        ActionItemDTO storageActionItem = actionBySourceId.get(storageEntityId);
        Assert.assertEquals(ActionType.CHANGE, storageActionItem.getActionType());
        Assert.assertEquals(String.valueOf(storageEntityId), storageActionItem.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(storageEntityId), storageActionItem.getNewSE().getId());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        // The XL ActionType does not have a separate entry for CROSS_TARGET_MOVE like the SDK one does
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
    public void testCrossTargetMoveContext_ChangeBothHostAndStorage() throws Exception {
        final EntityType hostEntityType = EntityType.PHYSICAL_MACHINE;
        final EntityType storageEntityType = EntityType.STORAGE;
        final long entityId = 22;
        final long sourceEntityId1 = 12;
        final long destinationEntityId1 = 13;
        final long sourceEntityId2 = 14;
        final long destinationEntityId2 = 15;

        // Construct an move action request, where both host and storage are changed
        // first item is storage move and second one is host move
        final ActionInfo move = ActionInfo.newBuilder()
            .setMove(ActionDTO.Move.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionExecutionTestUtils
                        .createActionEntity(sourceEntityId2, storageEntityType))
                    .setDestination(ActionExecutionTestUtils
                        .createActionEntity(destinationEntityId2, storageEntityType))
                    .build())
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionExecutionTestUtils
                        .createActionEntity(sourceEntityId1, hostEntityType))
                    .setDestination(ActionExecutionTestUtils
                        .createActionEntity(destinationEntityId1, hostEntityType))
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
            .setActionType(ActionDTO.ActionType.MOVE)
            .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move)).thenReturn(Collections.emptyList());

        // Build the primary entity
        final Entity entity = mockEntity(entityId, EntityType.VIRTUAL_MACHINE, primaryTargetId);
        entity.setHostedBy(primaryTargetId, sourceEntityId1);
        // Build the source host and destination host
        mockEntity(sourceEntityId1, hostEntityType, primaryTargetId);
        mockEntity(destinationEntityId1, hostEntityType, secondaryTargetId);
        // Build the source storage and destination storage
        mockEntity(sourceEntityId2, storageEntityType, primaryTargetId);
        mockEntity(destinationEntityId2, storageEntityType, secondaryTargetId);

        // We need to provide a mocked TopologyEntityDTO for the primary entity because that is how
        // the cross-target logic will find the storage entity to retrieve
        TopologyEntityDTO primaryTopologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(entityId)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(sourceEntityId1)
                    .setProviderEntityType(hostEntityType.getNumber()))
            .addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(sourceEntityId2)
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

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
            actionExecutionContextFactory.getActionExecutionContext(request);

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        // The XL ActionType does not have a separate entry for CROSS_TARGET_MOVE like the SDK one does
        Assert.assertEquals(ActionType.CROSS_TARGET_MOVE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(primaryTargetId, actionExecutionContext.getTargetId());
        Assert.assertEquals(secondaryTargetId, actionExecutionContext.getSecondaryTargetId().longValue());

        // Move actions should have 2 actionItems
        Assert.assertEquals(2, actionExecutionContext.getActionItems().size());

        // verify that first ActionItemDTO is host move
        ActionItemDTO hostActionItem = actionExecutionContext.getActionItems().get(0);
        Assert.assertEquals(ActionType.MOVE, hostActionItem.getActionType());
        Assert.assertEquals(String.valueOf(sourceEntityId1), hostActionItem.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(destinationEntityId1), hostActionItem.getNewSE().getId());

        // verify that second ActionItemDTO is storage move
        ActionItemDTO storageActionItem = actionExecutionContext.getActionItems().get(1);
        Assert.assertEquals(ActionType.CHANGE, storageActionItem.getActionType());
        Assert.assertEquals(String.valueOf(sourceEntityId2), storageActionItem.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(destinationEntityId2), storageActionItem.getNewSE().getId());
    }

    @Test
    public void testCrossTargetMoveContext_ChangeBothHostAndStorage_MultipleStorages() throws Exception {
        final EntityType hostEntityType = EntityType.PHYSICAL_MACHINE;
        final EntityType storageEntityType = EntityType.STORAGE;
        final long entityId = 22;
        final long sourceEntityId1 = 12;
        final long destinationEntityId1 = 13;
        final long sourceEntityId2 = 14;
        final long destinationEntityId2 = 15;
        final long sourceEntityId3 = 16;

        // Construct an move action request, where the host and one storage are changed
        // first item is storage move and second one is host move
        // Note: the VM consumes two storages which will be mocked later
        final ActionInfo move = ActionInfo.newBuilder()
            .setMove(ActionDTO.Move.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionExecutionTestUtils
                        .createActionEntity(sourceEntityId2, storageEntityType))
                    .setDestination(ActionExecutionTestUtils
                        .createActionEntity(destinationEntityId2, storageEntityType))
                    .build())
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionExecutionTestUtils
                        .createActionEntity(sourceEntityId1, hostEntityType))
                    .setDestination(ActionExecutionTestUtils
                        .createActionEntity(destinationEntityId1, hostEntityType))
                    .build()))
            .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
            .setActionId(actionId)
            .setTargetId(primaryTargetId)
            .setActionInfo(move)
            .setActionType(ActionDTO.ActionType.MOVE)
            .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move)).thenReturn(Collections.emptyList());

        // Mock the primary entity
        final Entity entity = mockEntity(entityId, EntityType.VIRTUAL_MACHINE, primaryTargetId);
        entity.setHostedBy(primaryTargetId, sourceEntityId1);
        // Mock the source host and destination host
        mockEntity(sourceEntityId1, hostEntityType, primaryTargetId);
        mockEntity(destinationEntityId1, hostEntityType, secondaryTargetId);
        // Mock the source storage and destination storage
        mockEntity(sourceEntityId2, storageEntityType, primaryTargetId);
        mockEntity(destinationEntityId2, storageEntityType, secondaryTargetId);
        // Mock the source storage which is not changed
        mockEntity(sourceEntityId3, storageEntityType, primaryTargetId);

        // the VM consumes two storages (one storage is changed, while the other one stays the same)
        TopologyEntityDTO primaryTopologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(entityId)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(sourceEntityId1)
                    .setProviderEntityType(hostEntityType.getNumber()))
            .addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(sourceEntityId2)
                    .setProviderEntityType(storageEntityType.getNumber()))
            .addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(sourceEntityId3)
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

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
            actionExecutionContextFactory.getActionExecutionContext(request);

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        // The XL ActionType does not have a separate entry for CROSS_TARGET_MOVE like the SDK one does
        Assert.assertEquals(ActionType.CROSS_TARGET_MOVE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(primaryTargetId, actionExecutionContext.getTargetId());
        Assert.assertEquals(secondaryTargetId, actionExecutionContext.getSecondaryTargetId().longValue());

        // Move actions should have 3 actionItems: one for host, the other two for storages
        Assert.assertEquals(3, actionExecutionContext.getActionItems().size());

        // verify that first ActionItemDTO is host move, the following are for storage move
        ActionItemDTO hostActionItem = actionExecutionContext.getActionItems().get(0);
        Assert.assertEquals(ActionType.MOVE, hostActionItem.getActionType());
        Assert.assertEquals(String.valueOf(sourceEntityId1), hostActionItem.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(destinationEntityId1), hostActionItem.getNewSE().getId());

        ActionItemDTO storageActionItem1 = actionExecutionContext.getActionItems().get(1);
        Assert.assertEquals(ActionType.CHANGE, storageActionItem1.getActionType());
        Assert.assertEquals(String.valueOf(sourceEntityId2), storageActionItem1.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(destinationEntityId2), storageActionItem1.getNewSE().getId());

        // verify the source and destination are same for the storage which is not changed
        ActionItemDTO storageActionItem2 = actionExecutionContext.getActionItems().get(2);
        Assert.assertEquals(ActionType.CHANGE, storageActionItem2.getActionType());
        Assert.assertEquals(String.valueOf(sourceEntityId3), storageActionItem2.getCurrentSE().getId());
        Assert.assertEquals(String.valueOf(sourceEntityId3), storageActionItem2.getNewSE().getId());
    }

    @Test
    public void testCrossTargetMoveContext_NoActionPolicy() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final long sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.PHYSICAL_MACHINE;
        final long destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.PHYSICAL_MACHINE;

        final ActionInfo move = ActionInfo.newBuilder()
            .setMove(ActionDTO.Move.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionExecutionTestUtils
                        .createActionEntity(sourceEntityId, sourceEntityType))
                    .setDestination(ActionExecutionTestUtils
                        .createActionEntity(destinationEntityId, destinationEntityType))
                    .build()))
            .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
            .setActionId(actionId)
            .setTargetId(primaryTargetId)
            .setActionInfo(move)
            .setActionType(ActionDTO.ActionType.MOVE)
            .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
            .thenReturn(Collections.emptyList());

        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        mockEntity(entityId, entityType, primaryTargetId);


        final ActionPolicyDTO moveActionPolicy =
            SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.VIRTUAL_MACHINE,
                ActionType.MOVE);

        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.toString()).setProbeType(SDKProbeType.VCENTER.toString())
                .setUiProbeCategory(ProbeCategory.HYPERVISOR.toString())
            .addActionPolicy(moveActionPolicy)
            .build();
        Mockito.when(probeStoreMock.getProbe(targetStoreMock.getTarget(primaryTargetId).get().getProbeId())).thenReturn(Optional.of(probeInfo));
        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
            actionExecutionContextFactory.getActionExecutionContext(request);

        //Calling the getSDKActionType to make sure it's not returning ActionType.CrossTarget since it's not in actionPolicy
        final ActionType sdkActionType = actionExecutionContext.getSDKActionType();
        Assert.assertEquals(ActionType.MOVE, sdkActionType);
    }

    @Test
    public void testCrossTargetMoveContext_WithActionPolicy() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final long sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.PHYSICAL_MACHINE;
        final long destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.PHYSICAL_MACHINE;
        final long storageEntityId = 14;
        final EntityType storageEntityType = EntityType.STORAGE;
        final ActionInfo move = ActionInfo.newBuilder()
            .setMove(ActionDTO.Move.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionExecutionTestUtils
                        .createActionEntity(sourceEntityId, sourceEntityType))
                    .setDestination(ActionExecutionTestUtils
                        .createActionEntity(destinationEntityId, destinationEntityType))
                    .build()))
            .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
            .setActionId(actionId)
            .setTargetId(primaryTargetId)
            .setActionInfo(move)
            .setActionType(ActionDTO.ActionType.MOVE)
            .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(move))
            .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = mockEntity(entityId, entityType, primaryTargetId);
        entity.setHostedBy(primaryTargetId, sourceEntityId);
        // Build the source provider entity
        mockEntity(sourceEntityId, sourceEntityType, primaryTargetId);
        // Build the destination provider entity
        mockEntity(destinationEntityId, destinationEntityType, secondaryTargetId);
        // For a cross-target move, we also need entity info for the storage
        mockEntity(storageEntityId, storageEntityType, primaryTargetId);
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
        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
            actionExecutionContextFactory.getActionExecutionContext(request);

        //Calling the getSDKActionType to make sure it's not returning ActionType.CrossTarget since it's not in actionPolicy
        final ActionType sdkActionType = actionExecutionContext.getSDKActionType();
        Assert.assertEquals(ActionType.CROSS_TARGET_MOVE, sdkActionType);
    }

    /**
     * Test that action execution context has the appropriate values in case of a
     * cloud volume move.
     *
     * @throws Exception if something goes terribly wrong
     */
    @Test
    public void testMoveContextCloudVolumeMove() throws Exception {

        final long entityId = 1;
        final int targetId = 2;
        final int actionId = 3;
        final int sourceEntityId = 4;
        final int destinationEntityId = 5;

        mockEntity(entityId, EntityType.VIRTUAL_VOLUME, targetId);
        mockEntity(sourceEntityId, EntityType.STORAGE_TIER, targetId);
        mockEntity(destinationEntityId, EntityType.STORAGE_TIER, targetId);
        final ActionInfo move = ActionInfo.newBuilder()
                .setMove(ActionDTO.Move.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(
                            entityId, EntityType.VIRTUAL_VOLUME))
                        .addChanges(ChangeProvider.newBuilder()
                            .setSource(ActionExecutionTestUtils
                                .createActionEntity(sourceEntityId, EntityType.STORAGE_TIER))
                            .setDestination(ActionExecutionTestUtils
                                .createActionEntity(destinationEntityId, EntityType.STORAGE_TIER))
                        )
                ).build();

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(move)
                .setActionType(ActionDTO.ActionType.MOVE)
                .build();

        final ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));
        Assert.assertEquals(ActionType.MOVE, actionExecutionContext.getSDKActionType());
        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());
    }

    /**
     * Test processing of Scale action request.
     *
     * @throws Exception In case of entity fetching error.
     */
    @Test
    public void testScaleContext() throws Exception {
        // Construct an move action request
        final long entityId = 22;
        final int sourceEntityId = 12;
        final EntityType sourceEntityType = EntityType.COMPUTE_TIER;
        final int destinationEntityId = 13;
        final EntityType destinationEntityType = EntityType.COMPUTE_TIER;
        final ActionInfo scale = ActionInfo.newBuilder()
                .setScale(ActionDTO.Scale.newBuilder()
                        .setTarget(ActionExecutionTestUtils.createActionEntity(entityId))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(ActionExecutionTestUtils
                                        .createActionEntity(sourceEntityId, sourceEntityType))
                                .setDestination(ActionExecutionTestUtils
                                        .createActionEntity(destinationEntityId, destinationEntityType))
                                .build()))
                .build();
        final int actionId = 7;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(actionId)
                .setTargetId(targetId)
                .setActionInfo(scale)
                .setActionType(ActionDTO.ActionType.SCALE)
                .build();

        // Set up the mocks
        Mockito.when(actionDataManagerMock.getContextData(scale))
                .thenReturn(Collections.emptyList());

        // We need entity info for the primary entity and its source and destination providers
        // Build the primary entity
        final EntityType entityType = EntityType.VIRTUAL_MACHINE;
        final Entity entity = mockEntity(entityId, entityType, targetId);
        entity.setHostedBy(targetId, sourceEntityId);
        // Build the source provider entity
        mockEntity(sourceEntityId, sourceEntityType, targetId);
        // Build the destination provider entity
        mockEntity(destinationEntityId, destinationEntityType, targetId);

        // Construct a move action context to pull in additional data for action execution
        // This is the method call being tested
        ActionExecutionContext actionExecutionContext =
                actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have at least one actionItem
        Assert.assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        Assert.assertTrue(actionExecutionContext.getControlAffectedEntities().contains(entityId));

        Assert.assertEquals(ActionType.SCALE, actionExecutionContext.getSDKActionType());

        Assert.assertEquals(actionId, actionExecutionContext.getActionId());
        Assert.assertEquals(targetId, actionExecutionContext.getTargetId());

        // Check that the full entity was retrieved
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(entityId);
        // Source entity info will be retrieved once while building the actionItem and again for
        // setting the hostedBy flag
        Mockito.verify(entityRetrieverMock, Mockito.times(2)).fetchAndConvertToEntityDTO(sourceEntityId);
        Mockito.verify(entityRetrieverMock).fetchAndConvertToEntityDTO(destinationEntityId);
        Mockito.verifyNoMoreInteractions(entityRetrieverMock);

        // Verify the expected call was made to retrieve context data
        Mockito.verify(actionDataManagerMock).getContextData(scale);
        Mockito.verifyNoMoreInteractions(actionDataManagerMock);
    }

    private Entity mockEntity(long entityId, EntityType entityType, long targetId)
                throws EntityRetrievalException {
        final Entity entity = new Entity(entityId, entityType);
        final EntityDTO entityDTO = EntityDTO.newBuilder()
            .setEntityType(entityType)
            .setId(Long.toString(entityId))
            .build();
        entity.addTargetInfo(targetId, entityDTO);
        Mockito.when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        Mockito.when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);
        return entity;
    }
}
