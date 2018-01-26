package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

/**
 * A static utility to map {@link ActionType} enums to action type strings
 * consumable by the UI. We need this level of indirection to insulate
 * the action types we use in XL from the action types used in legacy (which
 * are the action types the UI is built for).
 */
public class ActionTypeMapper {

    /**
     * The strings here come from
     * com.vmturbo.platform.VMTRoot.ManagedEntities.Automation.ActionType.
     * Hurray for indirect dependencies!
     * The UI expexts the legacy action types, so we have to do the conversion
     * here.
     * <p>
     * It is not one to one mapping between UI action type with internal action type. ACTIVATE and
     * START action type will be mapped to string "START".
     * TODO: After UI change to support Activate type, we should change the maps to support one
     * to one mapping.
     */
    private static ImmutableMap<ActionType, String> actionTypeToStrMappings =
        new ImmutableMap.Builder<ActionType, String>()
            .put(ActionType.NONE, "NONE")
            .put(ActionType.START, "START")
            .put(ActionType.MOVE, "MOVE")
            .put(ActionType.SUSPEND, "SUSPEND")
            .put(ActionType.PROVISION, "PROVISION")
            .put(ActionType.RECONFIGURE, "RECONFIGURE")
            .put(ActionType.RESIZE, "RESIZE")
            // UI expect activate to be "START"
            .put(ActionType.ACTIVATE, "START")
            .put(ActionType.DEACTIVATE, "DEACTIVATE")
            .build();

    private ActionTypeMapper() {}

    /**
     * Convert an {@link ActionType} to a string that represents the action type in
     * the UI and API.
     */
    @Nonnull
    public static String toApi(@Nonnull final ActionType actionType) {
        final String apiStr = actionTypeToStrMappings.get(actionType);
        if (apiStr == null) {
            throw new IllegalArgumentException("Invalid action type: " + actionType);
        }
        return apiStr;
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
