package com.vmturbo.cost.component.reserved.instance.filter;

import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.AVAILABILITY_ZONE_ID;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.BUSINESS_ACCOUNT_ID;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.REGION_ID;
import static com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil.SNAPSHOT_TIME;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;

/**
 * A abstract class represent a filter object which will be used to query reserved instance stats
 * related tables.
 */
public abstract class ReservedInstanceStatsFilter extends ReservedInstanceFilter {

    @Nullable
    protected final Instant startDate;

    @Nullable
    protected final Instant endDate;

    @Nullable
    protected final TimeFrame timeFrame;

    protected ReservedInstanceStatsFilter(@Nonnull Builder builder) {
        super(builder);
        this.startDate = builder.startDate;
        this.endDate = builder.endDate;
        this.timeFrame = builder.timeFrame;
    }


    /**
     * Generate the conditions to be used as filters in querying for RI stats. The {@link DSLContext}
     * is required in cases where we are querying for the latest statistics (in order to support
     * a nested select query).
     * @param dslContext The {@link DSLContext} for the associated query.
     * @return The generated SQL conditions.
     */
    @Nonnull
    public Condition[] generateConditions(@Nonnull DSLContext dslContext) {

        final Table<?> table = getTableName();
        final List<Condition> conditions = new ArrayList<>();


        // If scoped to both a region and an AZ, we use an OR condition, as the AZ filter will
        // exclude any RIs within only the region (regional RIs)
        final boolean scopedByRegionAndAZ = regionFilter.getRegionIdCount() > 0 &&
                availabilityZoneFilter.getAvailabilityZoneIdCount() > 0;
        if (scopedByRegionAndAZ) {
            Condition conditionRegion = table.field(REGION_ID).in(
                    regionFilter.getRegionIdList());
            Condition conditionAz = table.field(AVAILABILITY_ZONE_ID).in(
                    availabilityZoneFilter.getAvailabilityZoneIdList());
            conditions.add(conditionRegion.or(conditionAz));

        } else {
            if (regionFilter.getRegionIdCount() > 0) {
                conditions.add(table.field(REGION_ID).in(
                        regionFilter.getRegionIdList()));
            }

            if (availabilityZoneFilter.getAvailabilityZoneIdCount() > 0) {
                conditions.add(table.field(AVAILABILITY_ZONE_ID).in(
                        availabilityZoneFilter.getAvailabilityZoneIdList()));
            }
        }

        if (accountFilter.getAccountIdCount() > 0) {
            conditions.add(table.field(BUSINESS_ACCOUNT_ID).in(
                    accountFilter.getAccountIdList()));
        }


        final Field<Timestamp> snapshotTimeField = (Field<Timestamp>)table.field(SNAPSHOT_TIME);
        if (startDate != null) {
            final Timestamp startDateTimestamp = new Timestamp(startDate.toEpochMilli());
            conditions.add(snapshotTimeField.greaterThan(startDateTimestamp));
        }

        if (endDate != null) {
            final Timestamp endDateTimestamp = new Timestamp(endDate.toEpochMilli());
            conditions.add(snapshotTimeField.lessThan(endDateTimestamp));
        }

        // check if querying for latest records, if no start or end date are specified.
        // If so, first select the max SNAPSHOT_TIME as a nested select.
        if (startDate == null && endDate == null) {
            // Assume table is the _latest table
            conditions.add(snapshotTimeField.eq(
                    dslContext.select(DSL.max(snapshotTimeField))
                            .from(table)));
        }


        return conditions.toArray(new Condition[conditions.size()]);
    }

    /**
     * Determines the table name to use as part of this query, based on the requested start and end dates.
     * @return The target table for the query.
     */
    abstract Table<?> getTableName();

    /**
     * An abstract builder for subclasses of {@link ReservedInstanceStatsFilter}.
     * @param <T>
     * @param <U>
     */
    public abstract static class Builder<T extends ReservedInstanceStatsFilter, U extends Builder>
            extends ReservedInstanceFilter.Builder<T, U> {

        @Nullable
        protected Instant startDate;

        @Nullable
        protected Instant endDate;

        @Nullable
        protected TimeFrame timeFrame;

        /**
         * Set the start date for the filter. If null, the behavior of the filter is based on endDate.
         * If endDate is also null, only the latest value is queried (to get all data, startDate should
         * be set to {@link Instant#MIN} and endDate should be set to {@link Instant#MAX}). If endDate
         * is not null, all values up to endDate will be queried.
         * @param startDate The target startDate, represented as an {@link Instant}.
         * @return The {@link Builder} for method chaining.
         */
        public U startDate(@Nullable Instant startDate) {
            this.startDate = startDate;
            return (U)this;
        }

        /**
         * Sets the end date. If null, the behavior mirrors the behavior of startDate (except reversed
         * in the case where startDate is not null).
         * @param endDate The target endDate, represented as an {@link Instant}.
         * @return The {@link Builder} for method chaining.
         */
        public U endDate(@Nullable Instant endDate) {
            this.endDate = endDate;
            return (U)this;
        }

        /**
         * Set the target timeframe/data interval. If null, the latest table will be used.
         * @param timeFrame The target timeframe
         * @return The {@link Builder} for method chaining.
         */
        public U timeFrame(@Nullable TimeFrame timeFrame) {
            this.timeFrame = timeFrame;
            return (U)this;
        }
    }
}
