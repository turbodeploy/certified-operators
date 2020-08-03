package com.vmturbo.topology.processor.history;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.EntityCommodityReference;

/**
 * {@link ICommodityFieldAccessor} provides a way to apply restriction policies by checking
 */
public interface ICommodityFieldAccessor extends ITopologyGraphAccessor {

    /**
     * Apply restriction policy to the entity.
     *
     * @param commRef reference to the entity commodity field.
     */
    void applyInsufficientHistoricalDataPolicy(@Nonnull EntityCommodityReference commRef);
}
