package com.vmturbo.cost.component.reserved.instance;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;

/**
 * This interface is used to extract costs related to Recommended Reserved Instances.
 */
public interface BuyReservedInstanceCostStore {

    /**
     * Calculate the aggregated amortized cost based on a filter scoped to either Regions,
     * Business Accounts or Availability Zones.
     * For every record within the required scope, the amortized_cost per instance is multiplied by
     * the number of RIs bought which are all then summed together to get the aggregated amortized cost
     * for all record within scope.
     * If there are no records in the database or no data retrieved as part of the filter, we default
     * the aggregated amortized cost to 0.
     *
     * @param filter a filter of type BuyReservedInstanceCostFilter used to filter by Availability Zones,
     *               Business Accounts and Regions.
     * @return a double value indicating the aggregated amortized cost of a scope specified in the filter.
     */
    double getReservedInstanceAggregatedAmortizedCost(@Nonnull BuyReservedInstanceCostFilter filter);
}
