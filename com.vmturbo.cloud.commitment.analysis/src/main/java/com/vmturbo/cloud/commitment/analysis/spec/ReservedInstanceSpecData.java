package com.vmturbo.cloud.commitment.analysis.spec;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;

/**
 * An aggregate set of data about a {@link ReservedInstanceSpec} instance.
 */
@Value.Immutable
public interface ReservedInstanceSpecData extends CloudCommitmentSpecData<ReservedInstanceSpec> {

    /**
     * The coupons per instance of RI spec. This is derived from the compute tier of the RI spec.
     * This value is not used in equality or hashing checks.
     *
     * @return THe coupon capacity of the compute tier.
     */
    @Value.Derived
    @Value.Auxiliary
    default int couponsPerInstance() {
        return cloudTier().getTypeSpecificInfo()
                .getComputeTier()
                .getNumCoupons();
    }

    /**
     * The RI spec ID, which is the sole identifier used in equality and hashing checks.
     *
     * @return The RI spec ID.
     */
    @Value.Derived
    @Override
    default long specId() {
        return spec().getId();
    }
}
