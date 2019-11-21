package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.Condition;

import com.vmturbo.cost.component.db.Tables;

/**
 * Abstract Class serving as a parent class for Filters that operate on the BuyReservedInstance Table.
 */
public abstract class BuyReservedInstanceTableFilter {

    //List of conditions.
    private final List<Condition> conditions;

    //Boolean flag indicating if TopologyContextID is set.
    private final boolean hasTopologyContextId;

    //Topology Context ID.
    private final long topologyContextId;

    //Boolean flag indicating if region filter is set.
    private final boolean hasRegionFilter;

    //List of Region IDs.
    private final List<Long> regionIdList;

    //Boolean flag indicating if account filter is set.
    private final boolean hasAccountFilter;

    //List of Account IDs.
    private final List<Long> accountIdList;

    //List of BuyRI IDs.
    private final List<Long> buyRIIdList;

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
    protected BuyReservedInstanceTableFilter(boolean hasTopologyContextId, long topologyContextId,
                    boolean hasRegionFilter, List<Long> regionIdList, boolean hasAccountFilter,
                    List<Long> accountIdList, List<Long> buyRIIdList) {
        this.hasTopologyContextId = hasTopologyContextId;
        this.topologyContextId = topologyContextId;
        this.hasRegionFilter = hasRegionFilter;
        this.regionIdList = regionIdList;
        this.hasAccountFilter = hasAccountFilter;
        this.accountIdList = accountIdList;
        this.buyRIIdList = buyRIIdList;
        this.conditions = generateConditions();
    }

    /**
     * Get the array of {@link Condition} representing the conditions of this filter.
     *
     * @return The array of {@link Condition} representing the filter.
     */
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    private List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();
        if (this.hasTopologyContextId) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.TOPOLOGY_CONTEXT_ID.eq(this.topologyContextId));
        }

        if (this.hasRegionFilter && !this.regionIdList.isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.REGION_ID.in(this.regionIdList));
        }

        if (this.hasAccountFilter && !this.accountIdList.isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.BUSINESS_ACCOUNT_ID.in(this.accountIdList));
        }

        if (!this.buyRIIdList.isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.ID.in(
                            this.buyRIIdList));
        }
        return conditions;
    }


    /**
     * Abstract Class serving as parent class for Builders to build the required Filters.
     *
     * @param <T> Child Filter Classes that extend the BuyReservedInstanceTableFilter class.
     * @param <U> Child Bulilder Classes that extend the AbstractBuilder class.
     */
    abstract static class AbstractBuilder<T extends BuyReservedInstanceTableFilter, U extends AbstractBuilder<T, U>> {
        boolean hasTopologyContextId = false;
        long topologyContextId = 0L;
        boolean hasRegionFilter = false;
        final List<Long> regionIdList = new ArrayList<>();
        boolean hasAccountFilter = false;
        final List<Long> accountIdList = new ArrayList<>();
        final List<Long> buyRIIdList = new ArrayList<>();

        AbstractBuilder() {}

        public abstract T build();

        abstract U getThis();

        /**
         * Add the Topology Context ID. Also set the hasTopologyContextId flag to true.
         *
         * @param topologyContextId long value indicating the topologyContextID.
         * @return Builder for this class.
         */
        @Nonnull
        public U addTopologyContextId(long topologyContextId) {
            this.topologyContextId = topologyContextId;
            this.hasTopologyContextId = true;
            return getThis();
        }

        /**
         * Add the list of Region IDs. Also set the hasRegionFilter flag to true.
         *
         * @param ids List of region IDs.
         * @return Builder for this class.
         */
        @Nonnull
        public U addAllRegionIdList(@Nonnull final List<Long> ids) {
            this.regionIdList.addAll(ids);
            this.hasRegionFilter = true;
            return getThis();
        }

        /**
         * Add the list of Account IDs. Also set the hasAccountFilter flag to true.
         *
         * @param ids List of AccountIDs.
         * @return Builder for this class.
         */
        @Nonnull
        public U addAllAccountIdList(@Nonnull final List<Long> ids) {
            this.accountIdList.addAll(ids);
            this.hasAccountFilter = true;
            return getThis();
        }

        /**
         * Add the list of BuyRI IDs. Also set the count of BuyRI IDs.
         *
         * @param ids : List of BuyRI IDs
         * @return Builder for this class.
         */
        @Nonnull
        public U addBuyRIIdList(@Nonnull final List<Long> ids) {
            this.buyRIIdList.addAll(ids);
            return getThis();
        }
    }
}
