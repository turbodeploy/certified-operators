package com.vmturbo.cost.component.cloud.commitment.coverage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;

/**
 * A filter for topology-specific entity coverage.
 */
@HiddenImmutableImplementation
@Immutable
public interface TopologyEntityCoverageFilter {

    /**
     * Checks whether this filter contains an entity filter.
     * @return True, if this filter contains an entity filter.
     */
    default boolean hasEntityFilter() {
        return entityFilter() != null && entityFilter().getEntityIdCount() > 0;
    }

    /**
     * The entity filter.
     * @return The entity filter.
     */
    @Nullable
    EntityFilter entityFilter();

    /**
     * Checks whether this filter contains a region filter.
     * @return True, if this filter contains a region filter.
     */
    default boolean hasRegionFilter() {
        return regionFilter() != null && regionFilter().getRegionIdCount() > 0;
    }

    /**
     * The region filter.
     * @return The region filter.
     */
    @Nullable
    RegionFilter regionFilter();

    /**
     * Checks whether this filter contains an account filter.
     * @return True, if this filter contains an account filter.
     */
    default boolean hasAccountFilter() {
        return accountFilter() != null && accountFilter().getAccountIdCount() > 0;
    }

    /**
     * The account filter.
     * @return The account filter.
     */
    @Nullable
    AccountFilter accountFilter();

    /**
     * Checks whether this filter contains a service provider filter.
     * @return True, if this filter contains a service provider filter.
     */
    default boolean hasServiceProviderFilter() {
        return serviceProviderFilter() != null && serviceProviderFilter().getServiceProviderIdCount() > 0;
    }

    /**
     * The service provider filter.
     * @return The service provider filter.
     */
    @Nullable
    ServiceProviderFilter serviceProviderFilter();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link TopologyEntityCoverageFilter} instances.
     */
    class Builder extends ImmutableTopologyEntityCoverageFilter.Builder {}
}
