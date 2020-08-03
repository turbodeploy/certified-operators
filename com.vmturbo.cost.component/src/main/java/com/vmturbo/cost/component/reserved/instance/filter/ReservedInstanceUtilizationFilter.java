package com.vmturbo.cost.component.reserved.instance.filter;

import javax.annotation.Nonnull;

import org.jooq.Table;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the reserved instance coverage records from the
 * {@link com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtilizationStore}.
 * It provider a easier way to define simple search over reserved instances utilization records
 * in the tables.
 */
public class ReservedInstanceUtilizationFilter extends ReservedInstanceStatsFilter {

    private ReservedInstanceUtilizationFilter(@Nonnull Builder builder) {
        super(builder);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table<?> getTableName() {
        if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
            return Tables.RESERVED_INSTANCE_UTILIZATION_LATEST;
        } else if (this.timeFrame.equals(TimeFrame.HOUR)) {
            return Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
        } else if (this.timeFrame.equals(TimeFrame.DAY)) {
            return Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY;
        } else {
            return Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
        }
    }

    /**
     * Converts this filter to an {@link ReservedInstanceBoughtFilter} for querying of the
     * {@link com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore}. Note: the
     * configured start/end dates and timeframe are not converted to the RI bought filter.
     * @return The newly created {@link ReservedInstanceBoughtFilter}.
     */
    @Nonnull
    public ReservedInstanceBoughtFilter toReservedInstanceBoughtFilter() {
        return ReservedInstanceBoughtFilter.newBuilder()
                .regionFilter(regionFilter)
                .accountFilter(accountFilter)
                .availabilityZoneFilter(availabilityZoneFilter)
                .build();
    }

    @Nonnull
    public ReservedInstanceUtilizationFilter toLatestFilter() {
        return ReservedInstanceUtilizationFilter.newBuilder()
                .regionFilter(regionFilter)
                .accountFilter(accountFilter)
                .availabilityZoneFilter(availabilityZoneFilter)
                .build();
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static ReservedInstanceUtilizationFilter.Builder newBuilder() {
        return new ReservedInstanceUtilizationFilter.Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceUtilizationFilter}.
     */
    public static class Builder extends
            ReservedInstanceStatsFilter.Builder<ReservedInstanceUtilizationFilter, Builder> {

        /**
         * {@inheritDoc}
         */
        @Override
        public ReservedInstanceUtilizationFilter build() {
            return new ReservedInstanceUtilizationFilter(this);
        }
    }
}
