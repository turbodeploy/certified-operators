package com.vmturbo.cloud.commitment.analysis.spec;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * An aggregate set of data about a {@link ReservedInstanceSpec} instance.
 */
@Value.Immutable
public interface ReservedInstanceSpecData {

    /**
     * The target RI spec. This value is not used in equality or hashing checks.
     *
     * @return The {@link ReservedInstanceSpec}
     */
    @Value.Auxiliary
    ReservedInstanceSpec reservedInstanceSpec();

    /**
     * The compute tier associated with {@link #reservedInstanceSpec()}. This value is not used
     * in equality or hashing checks.
     *
     * @return The compute tier associated with the {@link ReservedInstanceSpec}.
     */
    @Value.Auxiliary
    TopologyEntityDTO computeTier();

    /**
     * The coupons per instance of RI spec. This is derived from the compute tier of the RI spec.
     * This value is not used in equality or hashing checks.
     *
     * @return THe coupon capacity of the compute tier.
     */
    @Value.Auxiliary
    int couponsPerInstance();

    /**
     * The RI spec ID, which is the sole identifier used in equality and hashing checks.
     *
     * @return The RI spec ID.
     */
    @Value.Derived
    default long reservedInstanceSpecId() {
        return reservedInstanceSpec().getId();
    }
}
