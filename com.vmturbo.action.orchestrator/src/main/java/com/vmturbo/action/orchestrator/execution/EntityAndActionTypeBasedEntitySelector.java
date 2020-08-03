package com.vmturbo.action.orchestrator.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Selects a service entity to execute an action against.
 */
public class EntityAndActionTypeBasedEntitySelector implements ActionExecutionEntitySelector {


    /**
     * This map defines the special cases for selecting which service entity to execute an action
     *  against.
     * The map is structured EntityType -> ActionType -> Function, where the Function takes an
     *  action as a parameter and returns the id of the service entity to execute the action against.
     */
    private Map<Integer, Map<ActionTypeCase, Function<Action, ActionEntity>>> entitySelectionMap;

    public EntityAndActionTypeBasedEntitySelector() {
        entitySelectionMap = new HashMap<>();

        addSpecialCase(EntityType.VIRTUAL_MACHINE_VALUE,
                ActionTypeCase.PROVISION,
                // TODO: is this logic right? it makes sense in the case of a VM being cloned,
                // but what does the data look like when a VM is being provisioned from a template?
                action -> action.getInfo().getProvision().getEntityToClone());
    }

    /**
     * Adds a special case for selecting which service entity to execute an action against.
     * @param entityType the entity type of the targetSE of the action
     * @param actionType the type of the action
     * @param chooserFunction takes an action as a parameter and returns the id of the service entity
     *                       to execute the action against.
     */
    private void addSpecialCase(int entityType,
                                ActionTypeCase actionType,
                                Function<Action, ActionEntity> chooserFunction) {
        Map<ActionTypeCase, Function<Action, ActionEntity>> mapForSpecificEntityType =
                entitySelectionMap.computeIfAbsent(entityType, k -> new HashMap<>());
        mapForSpecificEntityType.put(actionType, chooserFunction);
    }

    /**
     * Choose an entity to execute an action against.
     *
     * @param action the action to be executed
     * @return the entity to execute the action against
     */
    @Override
    public Optional<ActionEntity> getEntity(@Nonnull final Action action)
            throws UnsupportedActionException {
        final ActionEntity defaultEntity = ActionDTOUtil.getPrimaryEntity(action);
        final Map<ActionTypeCase, Function<Action, ActionEntity>> actionTypeCaseToEntityGetter =
                entitySelectionMap.get(defaultEntity.getType());
        if (actionTypeCaseToEntityGetter != null) {
            final Function<Action, ActionEntity> entityGetter =
                    actionTypeCaseToEntityGetter.get(action.getInfo().getActionTypeCase());
            if (entityGetter != null) {
                // A special case has been found and is being applied.
                return Optional.of(entityGetter.apply(action));
            }
        }
        return Optional.of(defaultEntity);
    }
}
