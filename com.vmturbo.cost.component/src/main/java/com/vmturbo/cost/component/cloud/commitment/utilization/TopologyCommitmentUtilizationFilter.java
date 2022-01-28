package com.vmturbo.cost.component.cloud.commitment.utilization;

import javax.annotation.Nullable;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudCommitmentFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;

/**
 * A filter for commitment utilization tied to a specific topology.
 */
@HiddenImmutableImplementation
@Immutable
public interface TopologyCommitmentUtilizationFilter {

    /**
     * Checks whether a commitment filter is configured.
     * @return True, if a commitment filter is configured.
     */
    default boolean hasCommitmentFilter() {
        return cloudCommitmentFilter() != null && cloudCommitmentFilter().getCloudCommitmentIdCount() > 0;
    }

    /**
     * The cloud commitment filter.
     * @return The cloud commitment filter.
     */
    @Nullable
    CloudCommitmentFilter cloudCommitmentFilter();

    /**
     * Checks whether a region filter is configured.
     * @return True, if a region filter is configured.
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
     * Checks whether an account filter is configured.
     * @return True, if an account filter is configured.
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
     * Checks whether a service provider filter is configured.
     * @return True, if a service provider filter is configured.
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
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link TopologyCommitmentUtilizationFilter} instances.
     */
    class Builder extends ImmutableTopologyCommitmentUtilizationFilter.Builder {}
}
