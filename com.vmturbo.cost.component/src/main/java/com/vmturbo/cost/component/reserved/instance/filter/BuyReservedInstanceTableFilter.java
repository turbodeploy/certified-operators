package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
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

    private final RegionFilter regionFilter;

    private final AccountFilter accountFilter;

    //List of BuyRI IDs.
    private final List<Long> buyRIIdList;

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
    protected BuyReservedInstanceTableFilter(boolean hasTopologyContextId, long topologyContextId,
                                             @Nonnull RegionFilter regionFilter,
                                             @Nonnull AccountFilter accountFilter,
                                             @Nonnull List<Long> buyRIIdList) {
        this.hasTopologyContextId = hasTopologyContextId;
        this.topologyContextId = topologyContextId;
        this.regionFilter = Objects.requireNonNull(regionFilter);
        this.accountFilter = Objects.requireNonNull(accountFilter);
        this.buyRIIdList = Objects.requireNonNull(buyRIIdList);
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

        if (regionFilter.getRegionIdCount() > 0) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.REGION_ID.in(regionFilter.getRegionIdList()));
        }

        if (accountFilter.getAccountIdCount() > 0 ) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.BUSINESS_ACCOUNT_ID.in(accountFilter.getAccountIdList()));
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
        protected boolean hasTopologyContextId = false;
        protected long topologyContextId = 0L;
        protected RegionFilter regionFilter = RegionFilter.getDefaultInstance();
        protected AccountFilter accountFilter = AccountFilter.getDefaultInstance();
        protected final List<Long> buyRIIdList = new ArrayList<>();

        AbstractBuilder() {}

        public abstract T build();

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
            return (U)this;
        }

        /**
         * Add a region filter to the aggregate filter
         * @param regionFilter The region filter or null, if BuyRI instances should not
         *                     be filtered by region
         * @return This builder for method chaining
         */
        @Nonnull
        public U setRegionFilter(@Nullable RegionFilter regionFilter) {
            this.regionFilter = Optional.ofNullable(regionFilter)
                    .orElseGet(RegionFilter::getDefaultInstance);
            return (U)this;
        }

        /**
         * Add the list of Account IDs. Also set the hasAccountFilter flag to true.
         *
         * @param accountFilter The account filter or null, if BuyRI instances should not
         *                      be filtered by purchasing account ID
         * @return Builder for this class.
         */
        @Nonnull
        public U setAccountFilter(@Nullable AccountFilter accountFilter) {
            this.accountFilter = Optional.ofNullable(accountFilter)
                    .orElseGet(AccountFilter::getDefaultInstance);
            return (U)this;
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
            return (U)this;
        }
    }
}
