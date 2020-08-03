package com.vmturbo.cost.component.reserved.instance;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCostFilter;

/**
 * This interface is used to extract costs related to Bought Reserved Instances.
 */
public interface ReservedInstanceCostStore {

    /**
     * Calculate the aggregated fixed, recurring and amortized costs based on a filter scoped to either Regions,
     * Business Accounts or Availability Zones.
     * For every record within the required scope, the fixed, recurring and amortized costs per instance are multiplied by
     * the number of RIs bought which are all then summed together to get the aggregated costs
     * for all records within the scope.
     *
     * @param filter a filter of type ReservedInstanceCostFilter used to filter by Availability Zones,
     *               Business Accounts and Regions.
     * @return a ReservedInstanceCostStat value indicating the aggregated costs of a scope specified in the filter.
     */
    Cost.ReservedInstanceCostStat getReservedInstanceAggregatedCosts(@Nonnull ReservedInstanceCostFilter filter);
}
