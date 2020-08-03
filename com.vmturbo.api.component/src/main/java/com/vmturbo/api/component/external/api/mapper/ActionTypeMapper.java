package com.vmturbo.api.component.external.api.mapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * A static utility to map {@link ActionDTO.ActionType} enums to {@link ActionType} enums
 * returned by the API.
 */
public class ActionTypeMapper {

    /**
     * The approximate mappings from {@link ActionDTO.ActionType} to {@link ActionType}.
     *
     * They are approximate because there are a bunch of rules that determine which XL action
     * is interpreted as which API action. If we try to capture all the possibilities here,
     * API calls (e.g. get action counts) will return counts that are too high.
     *
     * For an example of the logic that gets used to get the {@link ActionType} of an action
     * see {@link ActionSpecMapper}.
     *
     * The right solution (long-term) is to move all legacy <-> XL action type conversions into
     * the action orchestrator.
     */
    @VisibleForTesting
    static final ImmutableMap<ActionDTO.ActionType, ActionType> XL_TO_API_APPROXIMATE_TYPE =
        ImmutableMap.<ActionDTO.ActionType, ActionType>builder()
                .put(ActionDTO.ActionType.NONE, ActionType.NONE)
                .put(ActionDTO.ActionType.START, ActionType.START)
                .put(ActionDTO.ActionType.MOVE, ActionType.MOVE)
                .put(ActionDTO.ActionType.SCALE, ActionType.SCALE)
                .put(ActionDTO.ActionType.ALLOCATE, ActionType.ALLOCATE)
                .put(ActionDTO.ActionType.SUSPEND, ActionType.SUSPEND)
                .put(ActionDTO.ActionType.PROVISION, ActionType.PROVISION)
                .put(ActionDTO.ActionType.RECONFIGURE, ActionType.RECONFIGURE)
                .put(ActionDTO.ActionType.RESIZE, ActionType.RESIZE)
                .put(ActionDTO.ActionType.ACTIVATE, ActionType.START)
                .put(ActionDTO.ActionType.DEACTIVATE, ActionType.SUSPEND)
                .put(ActionDTO.ActionType.DELETE, ActionType.DELETE)
                .put(ActionDTO.ActionType.BUY_RI, ActionType.BUY_RI)
                .build();

    private ActionTypeMapper() {}

    /**
     * Get the {@link ActionType} that a {@link ActionDTO.ActionType} is associated with.
     *
     * This won't be 100% accurate, because the mappings are not directly related, and one
     * {@link ActionDTO.ActionType} can be interpreted as different {@link ActionType}s depending
     * on the involved entities. But we need to provide some kind of one-to-one mapping.
     *
     * @param actionType The XL action type.
     * @return The most common/likely API {@link ActionType}.
     */
    @Nonnull
    public static ActionType toApiApproximate(@Nonnull final ActionDTO.ActionType actionType) {
        ActionType apiType = XL_TO_API_APPROXIMATE_TYPE.get(actionType);
        // API Type shouldn't be null, because we should have entries for every XL type in the map.
        Objects.requireNonNull(apiType);
        return apiType;
    }

    /**
     * Get the {@link ActionDTO.ActionType}s that a {@link ActionType} may be associated with.
     *
     * This won't be 100% accurate, because the mappings are not directly related, and one
     * {@link ActionDTO.ActionType} can be interpreted as different {@link ActionType}s depending
     * on the involved entities. But we need to provide some kind of one-to-one mapping.
     *
     * @param actionType The API {@link ActionType}.
     * @return A set of possible {@link ActionDTO.ActionType}s that may be associated with the
     *         {@link ActionType}.
     */
    @Nonnull
    public static Set<ActionDTO.ActionType> fromApi(@Nonnull final ActionType actionType) {
        final Set<ActionDTO.ActionType> matchingTypes =
                XL_TO_API_APPROXIMATE_TYPE.asMultimap().inverse().get(actionType);
        if (matchingTypes == null || matchingTypes.size() == 0) {
            // If the ActionType is not mapped, return NONE. If we don't return NONE, requesting
            // unmapped types will return all results.
            return new HashSet<>(Arrays.asList(ActionDTO.ActionType.NONE));
        }
        return  matchingTypes;
    }
}
