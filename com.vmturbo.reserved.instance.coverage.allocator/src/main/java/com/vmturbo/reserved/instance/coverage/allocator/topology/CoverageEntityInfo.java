package com.vmturbo.reserved.instance.coverage.allocator.topology;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;

/**
 * The base information required about coverage entities.
 */
public interface CoverageEntityInfo {

    /**
     * The entity type.
     * @return The entity type.
     */
    int entityType();

    /**
     * The entity state.
     * @return The entity state.
     */
    @Nonnull
    EntityState entityState();
}
