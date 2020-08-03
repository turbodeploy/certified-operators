package com.vmturbo.cloud.commitment.analysis.demand;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * {@link CloudTierDemand} scoped to cloud infrastructure.
 */
public interface ScopedCloudTierDemand {

    /**
     * The OID of the account tied to the associated demand.
     * @return The OID of the account tied to the associated demand.
     */
    long accountOid();

    /**
     * The OID of the region tied to the associated demand.
     * @return The OID of the region tied to the associated demand.
     */
    long regionOid();

    /**
     * The OID of the availability zone tied to the associated demand. or {@link Optional#empty()},
     * if the demand is not within an AZ.
     * @return The OID of the availability zone tied to the associated demand or {@link Optional#empty()},
     * if the demand is not within an AZ.
     */
    @Nonnull
    Optional<Long> availabilityZoneOid();

    /**
     * The OID of the service provider tied to the demand.
     * @return The OID of the service provider tied to the demand.
     */
    long serviceProviderOid();

    /**
     * The cloud tier demand of the mapping. The demand type will be specific to the type of the cloud
     * tier.
     *
     * @return The cloud tier demand of the mapping.
     */
    @Nonnull
    CloudTierDemand cloudTierDemand();

    /**
     * The {@link CloudTierType} of {@link #cloudTierDemand()}.
     * @return The {@link CloudTierType} of {@link #cloudTierDemand()}.
     */
    @Value.Derived
    @Nonnull
    default CloudTierType cloudTierType() {
        if (cloudTierDemand() instanceof ComputeTierDemand) {
            return CloudTierType.COMPUTE_TIER;
        } else {
            throw new UnsupportedOperationException("Unknown cloud tier demand type");
        }
    }
}
