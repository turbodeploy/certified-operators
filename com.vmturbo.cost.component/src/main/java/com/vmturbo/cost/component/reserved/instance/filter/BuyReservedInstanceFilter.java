package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;

import com.vmturbo.cost.component.db.tables.pojos.BuyReservedInstance;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;

/**
 * A filter to restrict the {@link BuyReservedInstance} from the {@link BuyReservedInstanceStore}.
 * It provides an easier way to define simple search over reserved instances records in the tables.
 */
public class BuyReservedInstanceFilter extends BuyReservedInstanceTableFilter {

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
    private BuyReservedInstanceFilter(boolean hasTopologyContextId, long topologyContextId,
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
    public static class Builder extends AbstractBuilder<BuyReservedInstanceFilter, Builder> {
        private Builder() {}

        @Override
        public BuyReservedInstanceFilter build() {
            return new BuyReservedInstanceFilter(hasTopologyContextId, topologyContextId, hasRegionFilter,
                            regionIdList, hasAccountFilter, accountIdList, buyRIIdList);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
