package com.vmturbo.cloud.commitment.analysis.demand.store;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * A filter of allocated {@link com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand} for
 * a set of entities.
 */
@HiddenImmutableImplementation
@Immutable
public interface EntityComputeTierAllocationFilter extends EntityCloudTierAllocationFilter {

    /**
     * An {@link EntityComputeTierAllocationFilter} that returns all available records.
     */
    @Nonnull
    EntityComputeTierAllocationFilter ALL = EntityComputeTierAllocationFilter.builder().build();

    /**
     * The set of platforms to filter. An empty set indicates all platforms should pass the filter.
     *
     * @return The set of platforms to filter. An empty set indicates all platforms should
     * pass the filter.
     */
    Set<OSType> platforms();

    /**
     * The set of tenancies to filter. An empty set indicates all tenancies should pass the filter.
     *
     * @return The set of tenancies to filter. An empty set indicates all tenancies should
     * pass the filter.
     */
    Set<Tenancy> tenancies();

    /**
     * The set of compute tier OIDs to include in the filter. AN empty set indicates all compute tiers
     * pass the filter.
     *
     * @return The set of compute tier OIDs to include in the filter. AN empty set indicates all
     * compute tiers pass the filter.
     */
    Set<Long> computeTierOids();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link EntityComputeTierAllocationFilter} instances.
     */
    class Builder extends ImmutableEntityComputeTierAllocationFilter.Builder {}
}
