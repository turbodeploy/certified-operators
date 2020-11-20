package com.vmturbo.topology.processor.actions.data.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
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
 * Test for {@link DeleteContext}.
 */
public class DeleteContextTest {

    private final ActionDataManager actionDataManagerMock = mock(ActionDataManager.class);

    private final EntityStore entityStoreMock = mock(EntityStore.class);

    private final EntityRetriever entityRetrieverMock = mock(EntityRetriever.class);

    private final TargetStore targetStoreMock = mock(TargetStore.class);

    private final ProbeStore probeStoreMock = mock(ProbeStore.class);

    private final int awsTargetId = 2;

    // Builds the class under test
    private ActionExecutionContextFactory actionExecutionContextFactory;

    /**
     * Test setup.
     */
    @Before
    public void setup() {
        actionExecutionContextFactory = new ActionExecutionContextFactory(
            actionDataManagerMock,
            entityStoreMock,
            entityRetrieverMock,
            targetStoreMock,
            probeStoreMock);

        // Setup for AWS Probe
        when(targetStoreMock.getProbeTypeForTarget(awsTargetId)).thenReturn(Optional.of(SDKProbeType.AWS));
        final Target awsTarget = mock(Target.class);
        when(targetStoreMock.getTarget(awsTargetId)).thenReturn(Optional.of(awsTarget));
        final ActionPolicyDTO deleteActionPolicy =
            SdkActionPolicyBuilder.build(ActionCapability.SUPPORTED, EntityType.VIRTUAL_VOLUME, ActionType.DELETE);
        final ProbeInfo awsProbeInfo = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.toString()).setProbeType(SDKProbeType.AWS.toString())
                .setUiProbeCategory(ProbeCategory.PUBLIC_CLOUD.toString())
            .addActionPolicy(deleteActionPolicy)
            .build();
        when(probeStoreMock.getProbe(targetStoreMock.getTarget(awsTargetId).get().getProbeId()))
            .thenReturn(Optional.of(awsProbeInfo));
    }

    /**
     * Test Delete Action on {@link ActionExecutionContext} to ensure DeleteContext is being triggered.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDeleteContext() throws Exception {
        final long destinationEntityId = 333333L;
        final EntityType destinationEntityType = EntityType.VIRTUAL_VOLUME;
        final long sourceEntityId = 44444L;
        final EntityType sourceEntityType = EntityType.STORAGE_TIER;

        // Setup Delete Action
        final ActionInfo delete = ActionInfo.newBuilder()
            .setDelete(ActionDTO.Delete.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(destinationEntityId, destinationEntityType))
                .setSource(ActionExecutionTestUtils.createActionEntity(sourceEntityId, sourceEntityType))
                .build())
            .build();
        final long actionId = 66666L;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
            .setActionId(actionId)
            .setTargetId(awsTargetId)
            .setActionInfo(delete)
            .setActionType(ActionDTO.ActionType.DELETE)
            .setActionState(ActionState.IN_PROGRESS)
            .build();

        // Setup VV entity in entityStore
        final Entity destinationEntity = mockEntity(destinationEntityId, destinationEntityType, awsTargetId);
        destinationEntity.setHostedBy(awsTargetId, sourceEntityId);

        when(actionDataManagerMock.getContextData(delete)).thenReturn(Collections.emptyList());
        ActionExecutionContext actionExecutionContext = actionExecutionContextFactory.getActionExecutionContext(request);

        // Move actions should have at least one actionItem
        assertFalse(actionExecutionContext.getActionItems().isEmpty());

        // The primary entity being acted upon should be among those listed as affected entities
        assertTrue(actionExecutionContext.getControlAffectedEntities().contains(destinationEntityId));

        assertEquals(ActionType.DELETE, actionExecutionContext.getSDKActionType());

        assertEquals(actionId, actionExecutionContext.getActionId());
        assertEquals(awsTargetId, actionExecutionContext.getTargetId());

        // Check that the full entity was retrieved
        verify(entityRetrieverMock, times(1)).fetchAndConvertToEntityDTO(eq(destinationEntityId));
        verifyNoMoreInteractions(entityRetrieverMock);

        // Verify the expected call was made to retrieve context data
        verify(actionDataManagerMock).getContextData(delete);
        verifyNoMoreInteractions(actionDataManagerMock);
    }

    /**
     * Test getPrimaryEntityId() method.
     *
     * @throws EntityRetrievalException when entity is not being able to be retrieved.
     */
    @Test
    public void testGetPrimaryEntityId() throws EntityRetrievalException {
        final long destinationEntityId = 333333L;
        final EntityType destinationEntityType = EntityType.VIRTUAL_VOLUME;
        final long sourceEntityId = 44444L;
        final EntityType sourceEntityType = EntityType.STORAGE_TIER;

        // Setup Delete Action
        final ActionInfo delete = ActionInfo.newBuilder()
            .setDelete(ActionDTO.Delete.newBuilder()
                .setTarget(ActionExecutionTestUtils.createActionEntity(destinationEntityId, destinationEntityType))
                .setSource(ActionExecutionTestUtils.createActionEntity(sourceEntityId, sourceEntityType))
                .build())
            .build();
        final long actionId = 66666L;
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
            .setActionId(actionId)
            .setTargetId(awsTargetId)
            .setActionInfo(delete)
            .setActionType(ActionDTO.ActionType.DELETE)
            .setActionState(ActionState.IN_PROGRESS)
            .build();

        // Setup VV entity in entityStore
        final Entity destinationEntity = mockEntity(destinationEntityId, destinationEntityType, awsTargetId);
        destinationEntity.setHostedBy(awsTargetId, sourceEntityId);

        DeleteContext context = new DeleteContext(request, actionDataManagerMock,
            entityStoreMock, entityRetrieverMock, targetStoreMock, probeStoreMock);

        long result = context.getPrimaryEntityId();
        assertEquals(destinationEntityId, result);
    }

    /**
     * Helper method to mock entity in entityStore and entityRetriever.
     *
     * @param entityId id of entity
     * @param entityType type of entity
     * @param targetId target where entity located
     * @return {@link Entity} the entity object
     * @throws EntityRetrievalException when entity is not being able to be retrieved
     */
    @Nonnull
    private Entity mockEntity(long entityId, EntityType entityType, long targetId)
        throws EntityRetrievalException {
        final Entity entity = new Entity(entityId, entityType);
        final EntityDTO entityDTO = EntityDTO.newBuilder()
            .setEntityType(entityType)
            .setId(Long.toString(entityId))
            .build();
        entity.addTargetInfo(targetId, entityDTO);

        when(entityStoreMock.getEntity(entityId)).thenReturn(Optional.of(entity));
        when(entityRetrieverMock.fetchAndConvertToEntityDTO(entityId)).thenReturn(entityDTO);
        return entity;
    }
}
