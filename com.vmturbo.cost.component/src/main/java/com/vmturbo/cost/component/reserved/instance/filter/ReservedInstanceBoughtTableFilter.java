package com.vmturbo.cost.component.reserved.instance.filter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentReferenceFilterType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.cost.component.db.Tables;

/**
 * Abstract Class serving as a parent class for filters that operate on the ReservedInstanceBought Table.
 */
public abstract class ReservedInstanceBoughtTableFilter extends ReservedInstanceFilter {

    protected final Cost.ReservedInstanceBoughtFilter riBoughtFilter;

    // Needs to set to true, if any filter needs to get from reserved instance spec table.
    protected boolean joinWithSpecTable;

    /**
     * Set to true fo the RIS reterned by filter should not include the
     * ReservedInstances from undiscovered accounts.
     */
    protected boolean excludeRIsFromUndiscoveredAcccounts = false;

    protected ReservedInstanceBoughtTableFilter(@Nonnull Builder builder) {
        super(builder);
        this.riBoughtFilter = Objects.requireNonNull(builder.riBoughtFilter);
        this.joinWithSpecTable = regionFilter.getRegionIdCount() > 0;
        this.excludeRIsFromUndiscoveredAcccounts = builder.excludeRIsFromUndiscoveredAcccounts;
    }

    /**
     * If true, indicates the ReservedInstanceBought table should be joined with the ReservedInstanceSpec
     * table, allowing for filtering of RIs based on RI spec attributes.
     * @return True, if the ReservedInstanceBought table should be joined with the
     * ReservedInstanceSpec table
     */
    public boolean isJoinWithSpecTable() {
        return this.joinWithSpecTable;
    }

    /**
     *
     * Set to true fo the RIS reterned by filter should not include the
     * ReservedInstances from undiscovered accounts.
     * @return flag set for filtering the reserved instances from undiscovered accounts.
     */
    public boolean isExcludeRIsFromUndiscoveredAcccounts() {
        return excludeRIsFromUndiscoveredAcccounts;
    }


    /**
     * Generate the conditions to be used as filters in querying for RI instances.
     * @return The generated SQL conditions.
     */
    @Nonnull
    public Condition[] generateConditions() {

        final List<Condition> conditions = new ArrayList<>();

        final boolean scopedByRegionAndAZ = regionFilter.getRegionIdCount() > 0 &&
                availabilityZoneFilter.getAvailabilityZoneIdCount() > 0;

        // If filtering by both region and AZ are requested, create a single
        // condition where an either matches the region filter OR matches the AZ filter
        if (scopedByRegionAndAZ) {
            Condition conditionRegion = Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(
                    regionFilter.getRegionIdList());
            Condition conditionAz = Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(
                    availabilityZoneFilter.getAvailabilityZoneIdList());
            conditions.add(conditionRegion.or(conditionAz));

        } else {
            if (regionFilter.getRegionIdCount() > 0) {
                conditions.add(Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(
                        regionFilter.getRegionIdList()));
            }

            if (availabilityZoneFilter.getAvailabilityZoneIdCount() > 0) {
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(
                        availabilityZoneFilter.getAvailabilityZoneIdList()));
            }
        }

        if (accountFilter.getAccountIdCount() > 0) {
            CloudCommitmentReferenceFilterType filterType = accountFilter.getAccountFilterType();
            switch (filterType) {
                    case PURCHASED_BY:
                        conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(
                            accountFilter.getAccountIdList()));
                    break;

            }
        }


        if (riBoughtFilter.getRiBoughtIdCount() > 0) {
            if (riBoughtFilter.getExclusionFilter()) {
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.ID.notIn(
                        riBoughtFilter.getRiBoughtIdList()));
            } else {
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.ID.in(
                        riBoughtFilter.getRiBoughtIdList()));
            }
        }

        if (!includeExpired) {
            // Ignore expired RIs
            final LocalDateTime currentTime =
                    LocalDateTime.ofInstant(Instant.now(), ZoneId.from(ZoneOffset.UTC));
            conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.EXPIRY_TIME.ge(currentTime));
        }

        return conditions.toArray(new Condition[conditions.size()]);
    }


    protected abstract static class Builder<
            T extends ReservedInstanceBoughtTableFilter,
            U extends Builder> extends ReservedInstanceFilter.Builder<T, U> {

        /**
         * Set to true fo the RIS reterned by filter should not include the
         * ReservedInstances from undiscovered accounts.
         */
        protected boolean excludeRIsFromUndiscoveredAcccounts = false;


        private Cost.ReservedInstanceBoughtFilter riBoughtFilter =
                Cost.ReservedInstanceBoughtFilter.getDefaultInstance();

        /**
         * Add an {@link Cost.ReservedInstanceBoughtFilter} to this filter, in order to filter RI
         * instances by ID.
         * @param riBoughtFilter The RI filter, or null if no filtering based on RI ID is desired.
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public U riBoughtFilter(@Nullable Cost.ReservedInstanceBoughtFilter riBoughtFilter) {
            this.riBoughtFilter = Optional.ofNullable(riBoughtFilter)
                    .orElseGet(Cost.ReservedInstanceBoughtFilter::getDefaultInstance);
            return (U)this;
        }

        /**
         *
         * Set to true fo the RIS reterned by filter should not include the
         * ReservedInstances from undiscovered accounts.
         * @param excludeRIsFromUndiscoveredAcccounts flag for filtering RIs from undiscovered accounts.
         */
        public U excludeRIsFromUndiscoveredAcccounts(final boolean excludeRIsFromUndiscoveredAcccounts) {
            this.excludeRIsFromUndiscoveredAcccounts = excludeRIsFromUndiscoveredAcccounts;
            return (U)this;
        }

    }
}

