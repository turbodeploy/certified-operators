package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;

/**
 * A abstract class represents the filter object which will be used to query tables related to
 * RIs (coverage/utilization/details/cost). This filter is not applicable to entity RI coverage.
 */
public abstract class ReservedInstanceFilter {

    protected final RegionFilter regionFilter;

    protected final AccountFilter accountFilter;

    protected final AvailabilityZoneFilter availabilityZoneFilter;

    /**
     * Construct a {@link ReservedInstanceFilter} instance.
     * @param builder The {@link Builder} instance, used in contructing the filter
     */
    public ReservedInstanceFilter(@Nonnull Builder builder) {
        this.regionFilter = Objects.requireNonNull(builder.regionFilter);
        this.accountFilter = Objects.requireNonNull(builder.accountFilter);
        this.availabilityZoneFilter = Objects.requireNonNull(builder.availabilityZoneFilter);
    }

    public boolean isZoneFiltered() {
        return availabilityZoneFilter.getAvailabilityZoneIdCount() > 0;
    }

    @Nonnull
    public RegionFilter getRegionFilter() {
        return regionFilter;
    }

    @Nonnull
    public AccountFilter getAccountFilter() {
        return accountFilter;
    }

    /**
     * An abstract builder class for subclasses of {@link ReservedInstanceFilter}
     * @param <FILTER_CLASS> The subclass of {@link ReservedInstanceFilter}
     * @param <FILTER_BUILDER_CLASS> The subclass of this {@link Builder}, used in defining the correct
     *                              return type of the builder methods
     */
    protected abstract static class Builder<
            FILTER_CLASS extends ReservedInstanceFilter,
            FILTER_BUILDER_CLASS extends Builder> {

        protected RegionFilter regionFilter = RegionFilter.getDefaultInstance();
        protected AccountFilter accountFilter = AccountFilter.getDefaultInstance();
        protected AvailabilityZoneFilter availabilityZoneFilter =
                AvailabilityZoneFilter.getDefaultInstance();

        /**
         * Add a {@link RegionFilter} to this aggregate filter. If both a {@link RegionFilter} and
         * {@link AvailabilityZoneFilter} are configured, they will be OR'd together
         * @param regionFilter The region filter, or null if no filtering based on region is desired
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public FILTER_BUILDER_CLASS regionFilter(@Nullable RegionFilter regionFilter) {
            this.regionFilter = Optional.ofNullable(regionFilter)
                    .orElseGet(RegionFilter::getDefaultInstance);
            return (FILTER_BUILDER_CLASS)this;
        }

        /**
         * Add an {@link AccountFilter} to this filter.
         * @param accountFilter The account filter, or null if no filtering based on account is desired.
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public FILTER_BUILDER_CLASS accountFilter(@Nullable AccountFilter accountFilter) {
            this.accountFilter = Optional.ofNullable(accountFilter)
                    .orElseGet(AccountFilter::getDefaultInstance);
            return (FILTER_BUILDER_CLASS)this;
        }

        /**
         * Add an {@link AvailabilityZoneFilter} to this filter. If both an AZ and region filter are
         * configured, they will be OR'd as part of the query
         * @param azFilter The AZ filter, or null if no filtering based on AZ is desired.
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public FILTER_BUILDER_CLASS availabilityZoneFilter(@Nullable AvailabilityZoneFilter azFilter) {
            this.availabilityZoneFilter = Optional.ofNullable(azFilter)
                    .orElseGet(AvailabilityZoneFilter::getDefaultInstance);
            return (FILTER_BUILDER_CLASS)this;
        }

        /**
         * Builds an instance of {@link FILTER_CLASS}.
         * @return The newly built instance of {@link FILTER_CLASS}.
         */
        @Nonnull
        public abstract FILTER_CLASS build();
    }
}
