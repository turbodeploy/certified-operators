package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Redacted;

import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Recommendation info for a reserved instance recommendation.
 */
@HiddenImmutableImplementation
@Immutable
public interface ReservedInstanceRecommendationInfo extends RecommendationInfo {

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default CloudCommitmentType commitmentType() {
        return CloudCommitmentType.RESERVED_INSTANCE;
    }

    /**
     * {@inheritDoc}.
     */
    @Redacted
    @Override
    ReservedInstanceSpecData commitmentSpecData();

    /**
     * The {@link RIPricingData} for this recommendation.
     * @return The {@link RIPricingData} for this recommendation.
     */
    @Nonnull
    @Override
    RIPricingData cloudCommitmentPricingData();

    /**
     * The region of the RI recommendation.
     * @return The region of the RI recommendation.
     */
    @Derived
    default long regionOid() {
        return commitmentSpecData()
                .spec()
                .getReservedInstanceSpecInfo()
                .getRegionId();
    }

    /**
     * The cloud tier of the RI recommendation.
     * @return The cloud tier of the RI recommendation.
     */
    @Derived
    default long tierOid() {
        return commitmentSpecData()
                .spec()
                .getReservedInstanceSpecInfo()
                .getTierId();
    }

    /**
     * The platform of the RI recommendation. This can be ignored if {@link #platformFlexible()}
     * is true.
     * @return The platform of the RI recommendation.
     */
    @Derived
    default OSType platform() {
        return commitmentSpecData().spec().getReservedInstanceSpecInfo().getOs();
    }

    /**
     * Whether the RI recommendation is platform flexible.
     * @return Whether the RI recommendation is platform flexible.
     */
    @Derived
    default boolean platformFlexible() {
        return commitmentSpecData().spec().getReservedInstanceSpecInfo().getPlatformFlexible();
    }

    /**
     * Whether the RI recommendation is size flexible.
     * @return Whether the RI recommendation is size flexible.
     */
    @Derived
    default boolean sizeFlexible() {
        return commitmentSpecData().spec().getReservedInstanceSpecInfo().getSizeFlexible();
    }

    /**
     * The entity type of the covered cloud tier.
     * @return The entity type of the covered cloud tier.
     */
    @Derived
    default int tierType() {
        return commitmentSpecData().cloudTier().getEntityType();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link ReservedInstanceRecommendationInfo} instances.
     */
    class Builder extends ImmutableReservedInstanceRecommendationInfo.Builder {}
}
