package com.vmturbo.cost.component.reserved.instance;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;

/**
 * This interface is used to extract costs related to Recommended Reserved Instances.
 */
public interface BuyReservedInstanceCostStore {

    /**
     * Calculate the aggregated amortized cost, fixed cost and recurring cost based on a filter scoped
     * to either Regions, Business Accounts or Availability Zones.
     * For every record within the required scope, the fixed, recurring and amortized costs per instance are multiplied by
     * the number of RIs bought which are all then summed together to get the aggregated costs
     * for all records within the scope.
     * If there are no records in the database or no data retrieved as part of the filter, we default
     * the aggregated costs to 0.
     *
     * @param filter a filter of type BuyReservedInstanceCostFilter used to filter by Availability Zones,
     *               Business Accounts and Regions.
     * @return an object of type ReservedInstanceCostStat.
     */
    Cost.ReservedInstanceCostStat getReservedInstanceAggregatedAmortizedCost(@Nonnull BuyReservedInstanceCostFilter filter);
}
