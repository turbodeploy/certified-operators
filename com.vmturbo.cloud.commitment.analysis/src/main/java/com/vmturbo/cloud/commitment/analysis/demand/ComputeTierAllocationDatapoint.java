package com.vmturbo.cloud.commitment.analysis.demand;

import org.immutables.value.Value.Immutable;

/**
 * Represents allocated compute tier demand (OS, tenancy, cloud tier) for an entity for a
 * single point in time.
 */
@Immutable
public interface ComputeTierAllocationDatapoint extends EntityCloudTierDatapoint<ComputeTierAllocation> {
}
