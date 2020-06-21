package com.vmturbo.common.api.mappers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;

/**
 * Utility for mapping been ENUMs com.vmturbo.api.enums.EntityState and com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState.
 */
public class EntityStateMapper {

    /**
     * Mappings between {@link EntityState} and {@link com.vmturbo.api.enums.EntityState}.
     */
    private static final BiMap<EntityState, com.vmturbo.api.enums.EntityState> ENTITY_STATE_MAPPINGS =
            new ImmutableBiMap.Builder().put(
                    EntityState.POWERED_ON, com.vmturbo.api.enums.EntityState.ACTIVE)
                    .put(EntityState.POWERED_OFF, com.vmturbo.api.enums.EntityState.IDLE)
                    .put(EntityState.FAILOVER, com.vmturbo.api.enums.EntityState.FAILOVER)
                    .put(EntityState.MAINTENANCE, com.vmturbo.api.enums.EntityState.MAINTENANCE)
                    .put(EntityState.SUSPENDED, com.vmturbo.api.enums.EntityState.SUSPEND)
                    .put(EntityState.UNKNOWN, com.vmturbo.api.enums.EntityState.UNKNOWN)
                    .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private EntityStateMapper(){}

    /**
     * Get the {@link com.vmturbo.api.enums.EntityState} associated with a {@link com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState}.
     *
     * @param entityState The {@link EntityState}.
     * @return The associated {@link com.vmturbo.api.enums.EntityState},
     *         or {@link com.vmturbo.api.enums.EntityState#UNKNOWN}.
     */
    @Nonnull
    public static com.vmturbo.api.enums.EntityState fromXLToApi(@Nullable final EntityState entityState) {
        return ENTITY_STATE_MAPPINGS.getOrDefault(entityState, com.vmturbo.api.enums.EntityState.UNKNOWN);
    }

    /**
     * Get the {@link EntityState} associated with a {@link com.vmturbo.api.enums.EntityState}.
     *
     * @param entityState The {@link EntityState}.
     * @return The associated {@link EntityState},
     *         or {@link EntityState#UNKNOWN}.
     */
    @Nonnull
    public static EntityState fromApiToXL(@Nullable final com.vmturbo.api.enums.EntityState entityState) {
        return ENTITY_STATE_MAPPINGS.inverse().getOrDefault(entityState, EntityState.UNKNOWN);

    }
}