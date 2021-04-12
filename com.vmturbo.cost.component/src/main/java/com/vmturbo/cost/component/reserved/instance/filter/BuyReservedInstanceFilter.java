package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
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
     * @param regionFilter A {@link RegionFilter}, used to filter Buy RI instance by region OID, if
     *                     the filter list is set
     * @param accountFilter A {@link AccountFilter}, used to filter Buy RI instances by account OID,
     *                      if the filter list is set
     * @param buyRIIdList List of Buy RI IDs.
     */
    private BuyReservedInstanceFilter(boolean hasTopologyContextId,
                                      long topologyContextId,
                                      @Nonnull RegionFilter regionFilter,
                                      @Nonnull AccountFilter accountFilter,
                                      @Nonnull List<Long> buyRIIdList) {
        super(hasTopologyContextId, topologyContextId, regionFilter, accountFilter, buyRIIdList);
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
            return new BuyReservedInstanceFilter(
                    hasTopologyContextId,
                    topologyContextId,
                    regionFilter,
                    accountFilter,
                    buyRIIdList);
        }
    }
}
