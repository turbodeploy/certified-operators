package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Represents the tuple of (account OID, region/zone OID, compute tier OID, platform, tenancy) which
 * is stored within a row in the DB.
 */
@Value.Immutable
public interface RIBuyDemandCluster {

    /**
     * The account OID of the cluster.
     *
     * @return The account OID of the cluster.
     */
    long accountOid();

    /**
     * The platform/OS of the cluster.
     *
     * @return The platform/OS of the cluster.
     */
    OSType platform();

    /**
     * The tenancy of the cluster.
     *
     * @return The tenancy of the cluster.
     */
    Tenancy tenancy();

    /**
     * The computer tier OID associated with the cluster. This value is derived from {@link #computeTier()}.
     *
     * @return The compute tier OID.
     */
    @Value.Derived
    default long computeTierOid() {
        return computeTier().getOid();
    }

    /**
     * For AWS, this value will represent the zone OID. For Azure, this will be the region OID.
     *
     * @return The location OID.
     */
    long regionOrZoneOid();

    /**
     * The computer tier associated with the demand. This value is not used in hashing or equality checks.
     * Instead, the {@link #computeTierOid()} is used to determine equality.
     *
     * @return The compute tier of the demand cluster.
     */
    @Nonnull
    @Value.Auxiliary
    TopologyEntityDTO computeTier();

    /**
     * The number of coupons associated with the {@link #computeTier()} of this cluster.
     * @return The number of coupons of the compute tier.
     *
     */
    @Value.Auxiliary
    @Value.Derived
    default int computeTierCoupons() {
        return computeTier().getTypeSpecificInfo().getComputeTier().getNumCoupons();
    }
}
