package com.vmturbo.cost.component.reserved.instance.filter;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;

/**
 * Filter used to extract RI costs from the underlying tables.
 */
public class ReservedInstanceCostFilter extends ReservedInstanceBoughtTableFilter {

    private Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy;

    /**
     * Get the groupBy enum.
     *
     * @return enum of type GroupBy.
     */
    public Cost.GetReservedInstanceCostStatsRequest.GroupBy getGroupBy() {
        return groupBy;
    }

    private ReservedInstanceCostFilter(@Nonnull Builder builder) {
        super(builder);
        this.groupBy = builder.groupBy;
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceCostFilter}
     */
    public static class Builder extends
            ReservedInstanceBoughtTableFilter.Builder<ReservedInstanceCostFilter, Builder> {

        private Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy;

        /**
         * Add an enum of type GroupBy to the filter.
         *
         * @param groupBy GroupBy Enumeration
         * @return Builder object.
         */
        public Builder addGroupBy(Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy) {
            this.groupBy = groupBy;
            return this;
        }

        /**
         * Creates a new instance of {@link ReservedInstanceCostFilter}.
         * @return The newly created instance of {@link ReservedInstanceCostFilter}.
         */
        @Override
        public ReservedInstanceCostFilter build() {
            return new ReservedInstanceCostFilter(this);
        }
    }
}
