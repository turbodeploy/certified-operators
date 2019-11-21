package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;

/**
 * Filter used to extract RI costs from the underlying tables.
 */
public class BuyReservedInstanceCostFilter extends BuyReservedInstanceTableFilter {

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
     */
    private BuyReservedInstanceCostFilter(boolean hasTopologyContextId, long topologyContextId,
                    boolean hasRegionFilter, List<Long> regionIdList, boolean hasAccountFilter,
                    List<Long> accountIdList, List<Long> buyRIIdList) {
        super(hasTopologyContextId, topologyContextId, hasRegionFilter, regionIdList,
                        hasAccountFilter, accountIdList, buyRIIdList);
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

        @Override
        public BuyReservedInstanceCostFilter build() {
            return new BuyReservedInstanceCostFilter(hasTopologyContextId, topologyContextId, hasRegionFilter,
                            regionIdList, hasAccountFilter, accountIdList, buyRIIdList);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
