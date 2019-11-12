package com.vmturbo.cost.component.reserved.instance;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCostFilter;

/**
 * This interface is used to extract costs related to Reserved Instances.
 */
public interface ReservedInstanceCostStore {

    /**
     * Calculate the aggregated amortized cost based on a filter scoped to either Regions,
     * Business Accounts or Availability Zones.
     * For every record within the required scope, the amortized_cost per instance is multiplied by
     * the number of RIs bought which are all then summed together to get the aggregated amortized cost
     * for all record within scope.
     *
     * @param filter a filter of type ReservedInstanceCostFilter used to filter by Availability Zones,
     *               Business Accounts and Regions.
     * @return a double value indicating the aggregated amortized cost of a scope specified in the filter.
     */
    Double getReservedInstanceAggregatedAmortizedCost(@Nonnull ReservedInstanceCostFilter filter);
}
