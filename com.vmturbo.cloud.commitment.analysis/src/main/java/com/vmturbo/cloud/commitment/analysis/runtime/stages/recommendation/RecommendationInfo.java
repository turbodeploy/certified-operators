package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;

import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * Recommendation info containing purchasing information (e.g. purchasing account, type, etc).
 */
public interface RecommendationInfo {

    /**
     * The cloud commitment type.
     * @return The cloud commitment type.
     */
    @Nonnull
    CloudCommitmentType commitmentType();

    /**
     * The cloud commitment spec data.
     * @return The cloud commitment spec data.
     */
    @Nonnull
    CloudCommitmentSpecData commitmentSpecData();

    /**
     * The pricing data for this cloud commitment.
     * @return The pricing data for this cloud commitment.
     */
    @Nonnull
    CloudCommitmentPricingData cloudCommitmentPricingData();

    /**
     * The purchasing account OID.
     * @return The purchasing account OID.
     */
    long purchasingAccountOid();

    /**
     * The cloud commitment spec ID.
     * @return The cloud commitment spec ID.
     */
    @Derived
    default long cloudCommitmentSpecId() {
        return commitmentSpecData().specId();
    }
}
