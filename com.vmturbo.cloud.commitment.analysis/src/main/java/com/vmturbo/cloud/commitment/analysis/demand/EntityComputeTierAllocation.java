package com.vmturbo.cloud.commitment.analysis.demand;

import org.immutables.value.Value.Immutable;

/**
 * A mapping representing an entity (expected to be a virtual machine) allocated on a compute tier.
 */
@Immutable
public interface EntityComputeTierAllocation extends EntityCloudTierMapping<ComputeTierAllocation> {

}
