package com.vmturbo.topology.processor.topology;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Utilities for generating {@link TopologyEntity} objects for tests.
 */
public class TopologyEntityUtils {
    /**
     * Create a {@link com.vmturbo.topology.processor.topology.TopologyEntity.Builder}.
     *
     * @param entityBuilder The entity builder the {@link TopologyEntity} should wrap.
     * @return A builder for a {@link TopologyEntity}.
     */
    public static TopologyEntity.Builder topologyEntityBuilder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return TopologyEntity.newBuilder(entityBuilder, TopologyEntity.NEVER_UPDATED_TIME);
    }

    /**
     * Create a {@link com.vmturbo.topology.processor.topology.TopologyEntity.Builder}.
     *
     * @param entityBuilder The entity builder the {@link TopologyEntity} should wrap.
     * @return A {@link TopologyEntity} wrapping the input DTO builder.
     */
    public static TopologyEntity topologyEntity(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return topologyEntityBuilder(entityBuilder).build();
    }
}
