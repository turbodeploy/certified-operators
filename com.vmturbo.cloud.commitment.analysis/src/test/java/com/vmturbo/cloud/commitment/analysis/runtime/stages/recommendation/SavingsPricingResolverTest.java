package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.ComputeTierPricingData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.SavingsPricingResolver.SavingsPricingResolverFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings.ReservedInstanceRecommendationSettings;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

public class SavingsPricingResolverTest {

    private static final double TOLERANCE = .0001;

    private final SavingsPricingResolverFactory resolverFactory = new SavingsPricingResolverFactory();

    private final ComputeTierPricingData computeTierPricingData = ComputeTierPricingData.builder()
            .onDemandComputeRate(5.0)
            .onDemandLicenseRate(2.0)
            .reservedLicenseRate(1.0)
            .build();

    @Test
    public void testEmptyPricing() {

        final RecommendationSettings recommendationSettings = RecommendationSettings.newBuilder()
                .setReservedInstanceSettings(ReservedInstanceRecommendationSettings.newBuilder()
                        .setIncludeReservedLicense(true)
                        .build())
                .build();

        final SavingsPricingResolver resolver = resolverFactory.newResolver(
                CloudCommitmentType.RESERVED_INSTANCE, recommendationSettings);

        final double onDemandRate = resolver.calculateOnDemandRate(CloudTierPricingData.EMPTY_PRICING_DATA);

        assertThat(onDemandRate, closeTo(0.0, TOLERANCE));
    }

    @Test
    public void testReservedInstanceWithReservedLicense() {

        final RecommendationSettings recommendationSettings = RecommendationSettings.newBuilder()
                .setReservedInstanceSettings(ReservedInstanceRecommendationSettings.newBuilder()
                        .setIncludeReservedLicense(true)
                        .build())
                .build();

        final SavingsPricingResolver resolver = resolverFactory.newResolver(
                CloudCommitmentType.RESERVED_INSTANCE, recommendationSettings);

        final double onDemandRate = resolver.calculateOnDemandRate(computeTierPricingData);

        assertThat(onDemandRate, closeTo(6.0, TOLERANCE));
    }

    @Test
    public void testReservedInstanceWithoutReservedLicense() {

        final RecommendationSettings recommendationSettings = RecommendationSettings.newBuilder()
                .setReservedInstanceSettings(ReservedInstanceRecommendationSettings.newBuilder()
                        .setIncludeReservedLicense(false)
                        .build())
                .build();

        final SavingsPricingResolver resolver = resolverFactory.newResolver(
                CloudCommitmentType.RESERVED_INSTANCE, recommendationSettings);

        final double onDemandRate = resolver.calculateOnDemandRate(computeTierPricingData);

        // This should just match the on-demand compute rate
        assertThat(onDemandRate, closeTo(5.0, TOLERANCE));
    }

    @Test
    public void testReservedInstanceNoReservedLicense() {

        final RecommendationSettings recommendationSettings = RecommendationSettings.newBuilder()
                .setReservedInstanceSettings(ReservedInstanceRecommendationSettings.newBuilder()
                        .setIncludeReservedLicense(false)
                        .build())
                .build();

        final ComputeTierPricingData pricingData = ComputeTierPricingData.builder()
                .onDemandComputeRate(5.0)
                .onDemandLicenseRate(2.0)
                .build();

        final SavingsPricingResolver resolver = resolverFactory.newResolver(
                CloudCommitmentType.RESERVED_INSTANCE, recommendationSettings);

        final double onDemandRate = resolver.calculateOnDemandRate(pricingData);

        // This should just match the on-demand compute rate
        assertThat(onDemandRate, closeTo(7.0, TOLERANCE));
    }
}
