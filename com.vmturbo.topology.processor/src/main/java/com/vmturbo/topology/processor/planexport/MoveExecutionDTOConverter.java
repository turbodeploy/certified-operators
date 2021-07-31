package com.vmturbo.topology.processor.planexport;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ProviderInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;

/**
 * The class to convert move actionDTO to executionDTO.
 */
public class MoveExecutionDTOConverter implements ExecutionDTOConverter {

    private EntityRetriever entityRetriever;

    /**
     * Constructor.
     *
     * @param entityRetriever the object retrieves and converts an entity.
     */
    public MoveExecutionDTOConverter(EntityRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
    }

    @NotNull
    @Override
    public ActionExecutionDTO convert(@NotNull Action action) throws ActionDTOConversionException {
        try {
            Move move = action.getInfo().getMove();
            ActionExecutionDTO.Builder executionDTO = ActionExecutionDTO.newBuilder()
                    .setActionType(ActionType.MOVE).setActionOid(action.getId());
            EntityDTO target = entityRetriever.fetchAndConvertToEntityDTO(
                    move.getTarget().getId());
            for (ChangeProvider change: move.getChangesList()) {
                EntityDTO source = entityRetriever.fetchAndConvertToEntityDTO(
                        change.getSource().getId());
                EntityDTO destination = entityRetriever.fetchAndConvertToEntityDTO(
                        change.getDestination().getId());
                ActionItemDTO.Builder actionBuilder = ActionExecution.ActionItemDTO.newBuilder()
                        .setActionType(ActionType.MOVE)
                        .setUuid(Long.toString(action.getId()))
                        .setTargetSE(target)
                        .setCurrentSE(source)
                        .setNewSE(destination);
                Map<EntityType, Set<String>> resourceIds = new HashMap();
                // Find the resource entityDTO ids which are essentially the ids stored in the
                // EntityDTO's VirtualMachineData diskToStorage map.
                for (ActionEntity resourceEntity : change.getResourceList()) {
                    EntityDTO resourceDTO = entityRetriever.fetchAndConvertToEntityDTO(resourceEntity.getId());
                    Set<String> ids = resourceIds.get(resourceDTO.getEntityType());
                    if (ids == null) {
                        ids = new HashSet();
                        resourceIds.put(resourceDTO.getEntityType(), ids);
                    }
                    ids.add(resourceDTO.getId());
                }
                for (Map.Entry<EntityType, Set<String>> entry : resourceIds.entrySet()) {
                    actionBuilder.addProviders(ProviderInfo.newBuilder()
                            .setEntityType(entry.getKey()).addAllIds(entry.getValue()).build());
                }
                executionDTO.addActionItem(actionBuilder.build());
            }
            return executionDTO.build();
        } catch (Exception e) {
            throw new ActionDTOConversionException(e.getMessage(), e);
        }
    }
}
