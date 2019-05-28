package com.vmturbo.common.protobuf.topology;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;

/**
 * This represents the subset of the API/UI entity states (see: com.vmturbo.api.enums.EntityState)
 * that we can map to {@link EntityState}.
 *
 * TODO (roman, Oct 4 2018): There are a lot more UI entity states than these, but there are no
 * associated {@link EntityState} values. We should figure out how to map the UI entity states
 * to the valid power states - or, if the UI entity state represents more than just power state,
 * we should introduce a new enum to capture the other possibilities. As it is right now, a search
 * for, say, "NOT_MONITORED" will not return any results because we don't know how to interpret
 * that state inside XL.
 */
public enum UIEntityState {
    ACTIVE("ACTIVE"),
    IDLE("IDLE"),
    SUSPENDED("SUSPEND"),//Suspend is being used to be consistent with the same values returned in classic
    MAINTENANCE("MAINTENANCE"),
    FAILOVER("FAILOVER"),

    // The catch-all.
    UNKNOWN("UNKNOWN");

    private static final Logger logger = LogManager.getLogger();

    /**
     * Mappings between entityState enum values in TopologyEntityDTO to strings that the UI expects.
     */
    private static final BiMap<EntityState, UIEntityState> ENTITY_STATE_MAPPINGS =
            new ImmutableBiMap.Builder<EntityState, UIEntityState>()
                    .put(EntityState.POWERED_ON,     UIEntityState.ACTIVE)
                    .put(EntityState.POWERED_OFF,    UIEntityState.IDLE)
                    .put(EntityState.FAILOVER,       UIEntityState.FAILOVER)
                    .put(EntityState.MAINTENANCE,    UIEntityState.MAINTENANCE)
                    .put(EntityState.SUSPENDED,      UIEntityState.SUSPENDED)
                    .put(EntityState.UNKNOWN,        UIEntityState.UNKNOWN)
                    .build();

    private final String value;

    UIEntityState(@Nonnull final String value) {
        this.value = value;
    }

    /**
     * Get the string value of this state. This is the value that the UI/API works with, in places
     * where the state is a string.
     *
     * @return The string value.
     */
    @Nonnull
    public String apiStr() {
        return value;
    }

    /**
     * Find the {@link UIEntityState} associated with a particular string. This method is not
     * case-sensitive.
     *
     * @param inputStateStr The state string to map to a {@link UIEntityState}.
     * @return A {@link UIEntityState} representing the input string. If no {@link UIEntityState}
     *         matches the string, returns {@link UIEntityState#UNKNOWN}.
     *
     *         The {@link UIEntityState#apiStr()} method of the returned state will give the
     *         original input (unless the original input is not found, in which case it will return
     *         "UNKNOWN").
     */
    @Nonnull
    public static UIEntityState fromString(@Nullable final String inputStateStr) {
        if (inputStateStr == null) {
            logger.warn("Returning UNKOWN entity state for empty string.");
            return UIEntityState.UNKNOWN;
        } else {
            for (UIEntityState state : UIEntityState.values()) {
                if (inputStateStr.equalsIgnoreCase(state.apiStr())) {
                    return state;
                }
            }
            logger.warn("Unhandled input state {}. Returning UNKNOWN.", inputStateStr);
            return UIEntityState.UNKNOWN;
        }
    }


    /**
     * Get the {@link EntityState} associated with this {@link UIEntityState}. The
     * {@link EntityState} is the state we use inside XL.
     *
     * @return The {@link EntityState} associated with this {@link UIEntityState}, or
     *         {@link EntityState#UNKNOWN} if there is no mapping.
     */
    @Nonnull
    public EntityState toEntityState() {
        return ENTITY_STATE_MAPPINGS.inverse().getOrDefault(this, EntityState.UNKNOWN);
    }

    /**
     * Get the {@link UIEntityState} associated with a {@link EntityState}. This is the inverse
     * of {@link UIEntityState#toEntityState()}.
     *
     * @param entityState The {@link EntityState}.
     * @return The associated {@link UIEntityState}, or {@link UIEntityState#UNKNOWN}.
     */
    @Nonnull
    public static UIEntityState fromEntityState(@Nullable final EntityState entityState) {
        return ENTITY_STATE_MAPPINGS.getOrDefault(entityState, UIEntityState.UNKNOWN);
    }
}
