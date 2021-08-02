package com.vmturbo.topology.processor.history;

import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Helper to access and update the dto builders for topology commodities
 * by the commodity references (identity fields).
 * For the purposes of historical values editing.
 * - alleviate enumerations for repeated multiple look ups of commodities by type
 * - enable synchronization for modifications of historical commodity field values (and only those)
 */
public interface ITopologyGraphAccessor {
    /**
     * Look up the usage value for the commodity.
     *
     * @param field commodity field reference (used or peak)
     * @return value, null if not present
     */
    @Nullable
    Double getRealTimeValue(@Nonnull EntityCommodityFieldReference field);

    /**
     * Get the capacity value for the commodity reference.
     *
     * @param commRef commodity reference
     * @return capacity from the builder of this commodity, if sold
     * capacity of matching provider commodity, if bought
     * null if absent
     */
    @Nullable
    Double getCapacity(@Nonnull EntityCommodityReference commRef);

    /**
     * Get the raw utilization data for the commodity reference.
     *
     * @param commRef commodity reference
     * @return utilization data, null if unset
     */
    @Nullable
    UtilizationData getUtilizationData(@Nonnull EntityCommodityReference commRef);

    /**
     * Remove the raw utilization data for the commodity reference.
     *
     * @param commRef commodity reference
     */
    void clearUtilizationData(@Nonnull EntityCommodityReference commRef);

    /**
     * Change the historical commodity field.
     *
     * @param field field reference
     * @param setter value setter
     * @param description updating source, for gathering statistics
     */
    void updateHistoryValue(@Nonnull EntityCommodityFieldReference field,
            @Nonnull Consumer<Builder> setter, @Nonnull String description);

    /**
     * Change the thresholds on a commodity. Can only be successfully
     * applied to sold commodities. Thresholds affect commodity
     * capacity upper/lower bounds in the market.
     *
     * @param commRef The commodity whose thresholds should be updated.
     * @param setter The setter for updating the thresholds.
     * @param description updating source, for gathering statistics
     */
    void updateThresholds(@Nonnull EntityCommodityReference commRef,
                          @Nonnull Consumer<Thresholds.Builder> setter, @Nonnull String description);

    /**
     * How many times updateHistoryValue was called so far with a given description.
     *
     * @param description caller identifier
     * @return non-negative count
     */
    int getUpdateCount(@Nonnull String description);

    /**
     * Get the time the associated entity was last updated.
     *
     * @param commRef commodityReference
     * @return the time the associated entity was last updated, null if unavailable.
     */
    @Nullable
    Long getLastUpdatedTime(@Nonnull EntityCommodityReference commRef);
}
