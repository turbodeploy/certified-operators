package com.vmturbo.cloud.commitment.analysis.demand;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

@Immutable
public interface EntityComputeTierAllocationFilter extends EntityCloudTierAllocationFilter {

    /**
     * An {@link EntityComputeTierAllocationFilter} that returns all available records.
     */
    @Nonnull
    EntityComputeTierAllocationFilter ALL = ImmutableEntityComputeTierAllocationFilter.builder().build();

    Set<OSType> platforms();

    Set<Tenancy> tenancies();
}
