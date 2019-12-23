package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;

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
     * @param hasRegionFilter boolean attribute indicating if regionIDs are set in the filter.
     * @param regionIdList List of regionIDs set in the filter.
     * @param hasAccountFilter boolean attribute indicating if accountIDs are set in the filter.
     * @param accountIdList List of accountIDs set in the filter.
     * @param buyRIIdList List of Buy RI IDs.
     * @param groupBy Enumeration of type GroupBy.
     */
    private BuyReservedInstanceCostFilter(boolean hasTopologyContextId, long topologyContextId,
                    boolean hasRegionFilter, List<Long> regionIdList, boolean hasAccountFilter,
                    List<Long> accountIdList, List<Long> buyRIIdList, GroupBy groupBy) {
        super(hasTopologyContextId, topologyContextId, hasRegionFilter, regionIdList,
                        hasAccountFilter, accountIdList, buyRIIdList);
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
            return getThis();
        }

        @Override
        public BuyReservedInstanceCostFilter build() {
            return new BuyReservedInstanceCostFilter(hasTopologyContextId, topologyContextId, hasRegionFilter,
                            regionIdList, hasAccountFilter, accountIdList, buyRIIdList, groupBy);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
