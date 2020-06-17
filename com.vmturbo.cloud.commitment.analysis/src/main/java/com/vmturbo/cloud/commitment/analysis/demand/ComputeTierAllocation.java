package com.vmturbo.cloud.commitment.analysis.demand;

import org.immutables.value.Value.Immutable;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Represents demand from a virtual machine for a compute cloud. Implicit in this allocation is that
 * the VM is in a POWERED_ON state.
 */
@Immutable
public interface ComputeTierAllocation {

    /**
     * The compute tier OID of the allocated demand.
     *
     * @return The compute tier OID of the allocated demand.
     */
    long cloudTierOid();

    /**
     * The OS of the allocated demand.
     *
     * @return The OS of the allocated demand.
     */
    OSType osType();

    /**
     * The tenancy of the allocated demand.
     *
     * @return The tenancy of the allocated demand.
     */
    Tenancy tenancy();
}
