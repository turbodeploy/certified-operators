package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest.GroupBy;

/**
 * Filter used to extract RI costs from the underlying tables.
 */
public class BuyReservedInstanceCostFilter extends BuyReservedInstanceTableFilter {

    private GroupBy groupBy;

    /**
     * Constructor that takes topologyContextId, regionIdList, accountIdList, buyRIIdList and their
     * respective boolean attributes indicating if they have been set or not.
     *
     * @param hasTopologyContextId boolean attribute indicating if topology context ID is set in the filter.
     * @param topologyContextId long value indicating Topology Context ID.
     * @param regionFilter A {@link RegionFilter}, used to filter Buy RI instance by region OID, if
     *                     the filter list is set
     * @param accountFilter A {@link AccountFilter}, used to filter Buy RI instances by account OID,
     *                      if the filter list is set
     * @param buyRIIdList List of Buy RI IDs.
     * @param groupBy Enumeration of type GroupBy.
     */
    private BuyReservedInstanceCostFilter(boolean hasTopologyContextId, long topologyContextId,
                                          @Nonnull RegionFilter regionFilter,
                                          @Nonnull AccountFilter accountFilter,
                                          @Nonnull List<Long> buyRIIdList,
                                          @Nonnull GroupBy groupBy) {
        super(hasTopologyContextId, topologyContextId, regionFilter, accountFilter, buyRIIdList);
        this.groupBy = groupBy;
    }

    /**
     * Get the GroupBy enum.
     *
     * @return enum of type GroupBy.
     */
    public GroupBy getGroupBy() {
        return groupBy;
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static class Builder extends AbstractBuilder<BuyReservedInstanceCostFilter, Builder> {
        private Builder() {}

        private GroupBy groupBy;

        /**
         * Add GroupBy enumeration to the builder.
         *
         * @param groupBy Enumeration of type GroupBy.
         * @return Builder object.
         */
        public Builder addGroupBy(GroupBy groupBy) {
            this.groupBy = groupBy;
            return this;
        }

        @Override
        public BuyReservedInstanceCostFilter build() {
            return new BuyReservedInstanceCostFilter(
                    hasTopologyContextId,
                    topologyContextId,
                    regionFilter,
                    accountFilter,
                    buyRIIdList,
                    groupBy);
        }
    }
}
