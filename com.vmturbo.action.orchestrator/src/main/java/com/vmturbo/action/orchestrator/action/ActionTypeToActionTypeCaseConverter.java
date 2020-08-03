package com.vmturbo.action.orchestrator.action;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

/**
 * Provides mapping between ActionType and ActionTypeCase.
 */
public class ActionTypeToActionTypeCaseConverter {

    private ActionTypeToActionTypeCaseConverter() {}

    /**
     * Actions come with oneOf action type and ActionTypeCase to determine it. ActionType values
     * don't match ActionTypeCase values so we need to do maping between them.
     */
    private static final Map<ActionType, ActionTypeCase> ACTION_TYPE_TO_ACTION_TYPE_CASE =
            ImmutableMap.<ActionType, ActionTypeCase>builder()
                    .put(ActionType.NONE, ActionTypeCase.ACTIONTYPE_NOT_SET)
                    .put(ActionType.ACTIVATE, ActionTypeCase.ACTIVATE)
                    .put(ActionType.DEACTIVATE, ActionTypeCase.DEACTIVATE)
                    .put(ActionType.MOVE, ActionTypeCase.MOVE)
                    .put(ActionType.SCALE, ActionTypeCase.SCALE)
                    .put(ActionType.ALLOCATE, ActionTypeCase.ALLOCATE)
                    .put(ActionType.PROVISION, ActionTypeCase.PROVISION)
                    .put(ActionType.RECONFIGURE, ActionTypeCase.RECONFIGURE)
                    .put(ActionType.RESIZE, ActionTypeCase.RESIZE)
                    /* ActionTypeCase hasn't SUSPEND and START so we have to map it to some another
                       values. See OM-24242 for details.
                     */
                    .put(ActionType.SUSPEND, ActionTypeCase.DEACTIVATE)
                    .put(ActionType.START, ActionTypeCase.ACTIVATE)
                    .put(ActionType.DELETE, ActionTypeCase.DELETE)
                    .put(ActionType.BUY_RI, ActionTypeCase.BUYRI)
                    .build();

    /**
     * Returns ActionTypeCase matched with specified ActionType. If there is no appropriate
     * ActionTypeCase then IllegalArgumentException will be thrown.
     *
     * @param actionType to get ActionTypeCase for
     * @return ActionTypeCase for actionType
     * @throws IllegalArgumentException when action type cannot be resolved
     */
    @Nonnull
    public static ActionTypeCase getActionTypeCaseFor(ActionType actionType)
            throws IllegalArgumentException {
        final ActionTypeCase actionTypeCase = ACTION_TYPE_TO_ACTION_TYPE_CASE.get(actionType);
        if (actionTypeCase == null) {
            throw new IllegalArgumentException("Cannot resolve ActionTypeCase for ActionType " + actionType);
        }
        return ACTION_TYPE_TO_ACTION_TYPE_CASE.get(actionType);
    }
}
