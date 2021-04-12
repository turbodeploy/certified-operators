package com.vmturbo.cloud.commitment.analysis.spec;

import java.time.Period;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;

/**
 * An aggregate set of data about a {@link ReservedInstanceSpec} instance.
 */
@HiddenImmutableImplementation
@Value.Immutable
public interface ReservedInstanceSpecData extends CloudCommitmentSpecData<ReservedInstanceSpec> {

    /**
     * The cloud tier OID.
     * @return The cloud tier OID.
     */
    @Value.Derived
    default long tierOid() {
        return spec().getReservedInstanceSpecInfo().getTierId();
    }

    /**
     * The coupons per instance of RI spec. This is derived from the compute tier of the RI spec.
     * This value is not used in equality or hashing checks.
     *
     * @return THe coupon capacity of the compute tier.
     */
    @Value.Derived
    @Value.Auxiliary
    default double couponsPerInstance() {
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

    @Value.Derived
    @Nonnull
    @Override
    default CloudCommitmentType type() {
        return CloudCommitmentType.RESERVED_INSTANCE;
    }

    @Value.Derived
    @Override
    default Period term() {
        return Period.ofYears(spec().getReservedInstanceSpecInfo().getType().getTermYears());
    }

    /**
     * {@inheritDoc}.
     */
    @Value.Lazy
    @Override
    default float scalingPenalty() {
        if (cloudTier().getTypeSpecificInfo().hasComputeTier()) {
            return cloudTier().getTypeSpecificInfo().getComputeTier()
                    .getScalePenalty()
                    .getPenalty();
        } else {
            return 0.0f;
        }
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link ReservedInstanceSpecData} instances.
     */
    class Builder extends ImmutableReservedInstanceSpecData.Builder {}
}
