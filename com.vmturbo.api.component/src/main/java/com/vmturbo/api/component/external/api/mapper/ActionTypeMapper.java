package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

/**
 * A static utility to map {@link ActionType} enums to action type strings
 * consumable by the UI. We need this level of indirection to insulate
 * the action types we use in XL from the action types used in legacy (which
 * are the action types the UI is built for).
 */
public class ActionTypeMapper {

    private ActionTypeMapper() {}

    /**
     * Convert an {@link ActionType} to a string that represents the action type in
     * the UI and API.
    * TODO: After UI change to support Activate type, we should change the maps to support one
     * to one mapping.
      */
    @Nonnull
    public static String toApi(@Nonnull final ActionType actionType) {
        return actionType == ActionType.ACTIVATE
                        ? "START"
                        : actionType.name();
    }

    /**
     * Convert an action type string provided via the UI or API to its {@link ActionType}
     * equivalent.
     */
    @Nonnull
    public static ActionType fromApi(@Nonnull final String actionType) {
        return ActionType.valueOf(actionType);
    }
}
