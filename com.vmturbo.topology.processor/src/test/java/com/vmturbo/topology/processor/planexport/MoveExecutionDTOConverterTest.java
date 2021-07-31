package com.vmturbo.topology.processor.planexport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
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
        long targetOid = 123456L;
        long sourceOid = 22222L;
        long destinationOid = 33333L;
        long volumeOid = 44444L;
        ActionEntity target = ActionEntity.newBuilder().setId(targetOid)
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
        EntityDTO targetDTO = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(String.valueOf(targetOid)).build();
        EntityDTO sourceDTO = EntityDTO.newBuilder().setEntityType(EntityType.STORAGE)
                .setId(String.valueOf(sourceOid)).build();
        EntityDTO destinationDTO = EntityDTO.newBuilder().setEntityType(EntityType.STORAGE_TIER)
                .setId(String.valueOf(destinationOid)).build();
        EntityDTO resourceDTO = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_VOLUME)
                .setId(String.valueOf(volumeOid)).build();
        when(retriever.fetchAndConvertToEntityDTO(sourceOid)).thenReturn(sourceDTO);
        when(retriever.fetchAndConvertToEntityDTO(destinationOid)).thenReturn(destinationDTO);
        when(retriever.fetchAndConvertToEntityDTO(targetOid)).thenReturn(targetDTO);
        when(retriever.fetchAndConvertToEntityDTO(volumeOid)).thenReturn(resourceDTO);
        ActionExecutionDTO executionDTO = converter.convert(move);

        assertEquals(ActionType.MOVE, executionDTO.getActionType());
        assertEquals(actionOid, executionDTO.getActionOid());
        assertEquals(1, executionDTO.getActionItemCount());
        ActionItemDTO item = executionDTO.getActionItemList().get(0);
        assertEquals(targetDTO.getId(), item.getTargetSE().getId());
        assertEquals(sourceDTO.getId(), item.getCurrentSE().getId());
        assertEquals(destinationDTO.getId(), item.getNewSE().getId());
        assertEquals(resourceDTO.getEntityType(), item.getProviders(0).getEntityType());
        assertEquals(resourceDTO.getId(), item.getProviders(0).getIds(0));
    }
}
