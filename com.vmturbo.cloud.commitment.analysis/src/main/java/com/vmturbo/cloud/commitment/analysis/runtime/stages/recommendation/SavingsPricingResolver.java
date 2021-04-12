package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.ComputeTierPricingData;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * The savings pricing resolver is responsible for converting cloud tier pricing to the rates
 * required by the cloud commitment savings calculator.
 */
public class SavingsPricingResolver {

    private final CloudCommitmentType commitmentType;

    private final RecommendationSettings recommendationSettings;

    private SavingsPricingResolver(@Nonnull CloudCommitmentType commitmentType,
                                   @Nonnull RecommendationSettings recommendationSettings) {

        this.commitmentType = Objects.requireNonNull(commitmentType);
        this.recommendationSettings = Objects.requireNonNull(recommendationSettings);
    }

    /**
     * Calculates the on-demand rate for the specified {@code pricingData}. Determination of the
     * on-demand rate will be based on the recommendation settings for the analysis (e.g. whether
     * the recommendations settings indicate whether reserved license rates should be used for RI
     * compute recommendations).
     *
     * @param pricingData The pricing data source.
     * @return The on-demand rate to use for savings calculations.
     */
    public double calculateOnDemandRate(@Nonnull CloudTierPricingData pricingData) {

        if (pricingData.equals(CloudTierPricingData.EMPTY_PRICING_DATA)) {
            return 0.0;
        } else if (pricingData instanceof ComputeTierPricingData) {
            return calculateComputeTierRate((ComputeTierPricingData)pricingData);
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s is not a supported pricing data type", pricingData.getClass().getSimpleName()));
        }
    }

    private double calculateComputeTierRate(@Nonnull ComputeTierPricingData pricingData) {

        if (commitmentType == CloudCommitmentType.RESERVED_INSTANCE) {

            final boolean includeReservedLicense = recommendationSettings.getReservedInstanceSettings()
                    .getIncludeReservedLicense();

            if (includeReservedLicense) {
                return pricingData.onDemandComputeRate() + pricingData.onDemandLicenseRate()
                        - pricingData.reservedLicenseRate().orElse(0.0);
            } else {
                // If there is a reserved license rate, we should not include the on-demand license.
                // However, if there is no reserved license rate then the RI will always cover
                // the on-demand license.
                if (pricingData.reservedLicenseRate().isPresent()) {
                    return pricingData.onDemandComputeRate();
                } else {
                    return pricingData.onDemandComputeRate() + pricingData.onDemandLicenseRate();
                }
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s is not a supported cloud commitment type", commitmentType));
        }
    }

    /**
     * A factory class for creating {@link SavingsPricingResolver} instances.
     */
    public static class SavingsPricingResolverFactory {

        /**
         * Constructs a new pricing resolver.
         * @param commitmentType The commitment type to recommend, used to determine which recommendation
         *                       settings are relevant.
         * @param recommendationSettings The recommendation settings.
         * @return The newly constructed pricing resolver instance.
         */
        @Nonnull
        public SavingsPricingResolver newResolver(@Nonnull CloudCommitmentType commitmentType,
                                                  @Nonnull RecommendationSettings recommendationSettings) {
            return new SavingsPricingResolver(commitmentType, recommendationSettings);
        }
    }

}
