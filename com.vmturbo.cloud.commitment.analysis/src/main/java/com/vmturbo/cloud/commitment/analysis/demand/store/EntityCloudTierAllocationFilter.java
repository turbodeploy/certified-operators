package com.vmturbo.cloud.commitment.analysis.demand.store;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;

/**
 * A filter for records of entity cloud tier allocation demand. Cloud scope filtering is supported.
 * A filter with multiple criteria (e.g. entity and account OIDs) is expected to scope to the intersection
 * of the criteria.
 */
@Immutable
public interface EntityCloudTierAllocationFilter {

    /**
     * A filter on the start time of the allocation.
     *
     * @return A {@link TimeFilter} to be applied to the start time of scoped allocations or
     * {@link Optional#empty()}, if no filtering based on start time is desired.
     */
    @Nonnull
    Optional<TimeFilter> startTimeFilter();

    /**
     * A filter on the end time of the allocation.
     *
     * @return A {@link TimeFilter} to be applied to the end time of scoped allocations or
     * {@link Optional#empty()}, if no filtering based on end time is desired.
     */
    @Nonnull
    Optional<TimeFilter> endTimeFilter();

    /**
     * A filter on the entities of the allocation.
     *
     * @return The OIDs of the entities to filter again. An empty set is returned if no filtering
     * based on the entity is desired.
     */
    @Nonnull
    Set<Long> entityOids();

    /**
     * A filter on the account associated with the entity of the allocation.
     *
     * @return The OIDs of the accounts to filter again. An empty set is returned if no filtering
     * based on the account is desired.
     */
    @Nonnull
    Set<Long> accountOids();

    /**
     * A filter on the region associated with the entity of the allocation.
     *
     * @return The OIDs of the regions to filter again. An empty set is returned if no filtering
     * based on the region is desired.
     */
    @Nonnull
    Set<Long> regionOids();

    /**
     * A filter on the service provider associated with the entity of the allocation.
     *
     * @return The OIDs of the service providers to filter again. An empty set is returned if no filtering
     * based on the service provider is desired.
     */
    @Nonnull
    Set<Long> serviceProviderOids();

}
