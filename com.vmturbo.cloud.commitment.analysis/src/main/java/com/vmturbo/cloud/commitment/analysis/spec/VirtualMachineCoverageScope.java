package com.vmturbo.cloud.commitment.analysis.spec;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Contains the attributes for matching a cloud commitment to a virtual machine.
 */
@Immutable
public interface VirtualMachineCoverageScope extends CloudCommitmentCoverageScope {

    /**
     * The region OID of the VM.
     * @return The region OID of the VM.
     */
    long regionOid();

    /**
     * The OS of the VM.
     * @return The OS of the VM.
     */
    @Nonnull
    OSType osType();

    /**
     * The tenancy of the VM.
     * @return The tenancy of the VM.
     */
    @Nonnull
    Tenancy tenancy();
}
