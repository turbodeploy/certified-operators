package com.vmturbo.topology.processor.planexport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;

/**
 * Unit tests for {@link MoveExecutionDTOConverter}.
 */
public class MoveExecutionDTOConverterTest {

    private EntityRetriever retriever = mock(EntityRetriever.class);
    private MoveExecutionDTOConverter converter = new MoveExecutionDTOConverter(retriever);

    /**
     * Test the move action conversion.
     * @throws ActionDTOConversionException the exception occurs during actionDTO conversion.
     * @throws EntityRetrievalException the exception occurs during entity retrieval.
     */
    @Test
    public void testConvert() throws ActionDTOConversionException, EntityRetrievalException {
        long targetId = 9876;
        long targetEntityOid = 123456L;
        long sourceOid = 22222L;
        long destinationOid = 33333L;
        long volumeOid = 44444L;
        long hypervisorOid = 55555L;
        String hypervisorUuid = "48151623-4248-1516-2342-481516234200";

        ActionEntity target = ActionEntity.newBuilder().setId(targetEntityOid)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE).build();
        ActionEntity source = ActionEntity.newBuilder().setId(sourceOid)
                .setType(EntityType.STORAGE_VALUE).build();
        ActionEntity destination = ActionEntity.newBuilder().setId(destinationOid)
                .setType(EntityType.STORAGE_TIER_VALUE).build();
        ActionEntity volume = ActionEntity.newBuilder().setId(volumeOid)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE).build();
        long actionOid = 7777L;
        Action move = Action.newBuilder().setId(actionOid).setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder().setTarget(target).addChanges(ChangeProvider.newBuilder()
                .setSource(source).setDestination(destination).addResource(volume))))
                .setDeprecatedImportance(1.0).setExplanation(Explanation.newBuilder().setMove(
                        MoveExplanation.getDefaultInstance())).build();

        TopologyEntityDTO targetTE = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityType(EntityType.HYPERVISOR_SERVER_VALUE)
                .setConnectedEntityId(hypervisorOid)
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                .build())
            .setOid(targetEntityOid).build();
        EntityDTO targetDTO = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(String.valueOf(targetEntityOid)).build();
        when(retriever.retrieveTopologyEntity(targetEntityOid)).thenReturn(Optional.of(targetTE));
        when(retriever.convertToEntityDTO(targetTE)).thenReturn(targetDTO);

        TopologyEntityDTO hypervisorTE = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.HYPERVISOR_SERVER_VALUE)
            .setOid(hypervisorOid)
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(targetId, PerTargetEntityInformation.newBuilder()
                        .setVendorId(hypervisorUuid).build())))
            .build();

        when(retriever.retrieveTopologyEntity(hypervisorOid)).thenReturn(Optional.of(hypervisorTE));

        EntityDTO sourceDTO = EntityDTO.newBuilder().setEntityType(EntityType.STORAGE)
                .setId(String.valueOf(sourceOid)).build();
        EntityDTO destinationDTO = EntityDTO.newBuilder().setEntityType(EntityType.STORAGE_TIER)
                .setId(String.valueOf(destinationOid)).build();
        EntityDTO resourceDTO = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_VOLUME)
                .setId(String.valueOf(volumeOid)).build();
        when(retriever.fetchAndConvertToEntityDTO(sourceOid)).thenReturn(sourceDTO);
        when(retriever.fetchAndConvertToEntityDTO(destinationOid)).thenReturn(destinationDTO);
        when(retriever.fetchAndConvertToEntityDTO(targetEntityOid)).thenReturn(targetDTO);
        when(retriever.fetchAndConvertToEntityDTO(volumeOid)).thenReturn(resourceDTO);
        ActionExecutionDTO executionDTO = converter.convert(move);

        assertEquals(ActionType.MOVE, executionDTO.getActionType());
        assertEquals(actionOid, executionDTO.getActionOid());
        assertEquals(1, executionDTO.getActionItemCount());
        ActionItemDTO item = executionDTO.getActionItemList().get(0);
        assertEquals(1, item.getContextDataList().size());
        ContextData contextData = item.getContextDataList().get(0);
        assertEquals(SDKConstants.HYPERVISOR_UUID, contextData.getContextKey());
        assertEquals(hypervisorUuid, contextData.getContextValue());
        assertEquals(targetDTO.getId(), item.getTargetSE().getId());
        assertEquals(sourceDTO.getId(), item.getCurrentSE().getId());
        assertEquals(destinationDTO.getId(), item.getNewSE().getId());
        assertEquals(resourceDTO.getEntityType(), item.getProviders(0).getEntityType());
        assertEquals(resourceDTO.getId(), item.getProviders(0).getIds(0));
    }
}
