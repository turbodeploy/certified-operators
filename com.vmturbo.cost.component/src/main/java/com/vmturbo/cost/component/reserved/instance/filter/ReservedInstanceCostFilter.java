package com.vmturbo.cost.component.reserved.instance.filter;

import javax.annotation.Nonnull;

/**
 * Filter used to extract RI costs from the underlying tables.
 */
public class ReservedInstanceCostFilter extends ReservedInstanceBoughtTableFilter {

    private ReservedInstanceCostFilter(@Nonnull Builder builder) {
        super(builder);
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
