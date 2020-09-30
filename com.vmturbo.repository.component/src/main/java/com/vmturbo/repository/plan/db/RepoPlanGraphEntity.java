package com.vmturbo.repository.plan.db;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * Implementation of the {@link BaseGraphEntity} for plan supply chain calculation.
 */
public class RepoPlanGraphEntity extends BaseGraphEntity<RepoPlanGraphEntity> {

    protected RepoPlanGraphEntity(@Nonnull TopologyEntityDTO src) {
        super(src);
    }

    /**
     * Builder for {@link RepoPlanGraphEntity}.
     */
    public static class Builder extends BaseGraphEntity.Builder<Builder, RepoPlanGraphEntity> {

        /**
         * Create a new entity builder.
         *
         * @param dto The {@link TopologyEntityDTO}.
         */
        public Builder(@Nonnull final TopologyEntityDTO dto) {
            super(dto, new RepoPlanGraphEntity(dto));
        }
    }
}
