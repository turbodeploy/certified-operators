package com.vmturbo.cloud.commitment.analysis.runtime.stages.pricing;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingAnalyzer;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingAnalyzer.CloudCommitmentPricingAnalyzerFactory;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext.Builder;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemandSet;
import com.vmturbo.cloud.commitment.analysis.spec.SpecMatcherOutput;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;

/**
 * The pricing resolver stage is used to resolve pricing for given commitment spec demands.
 */
public class PricingResolverStage extends AbstractStage<SpecMatcherOutput, PricingResolverOutput> {

    private static final String STAGE_NAME = "Pricing Resolver";

    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentPricingAnalyzerFactory pricingAnalyzerFactory;

    /**
     * Constructor for the pricing resolver stage.
     *
     * @param id The cloud commitment analysis context id.
     * @param config The cloud commitment analysis config.
     * @param context The cloud commitment analysis context.
     * @param pricingAnalyzerFactory The cloud commitment pricing analyzer factory which is responsible for
     * the actual resolution of pricing.
     */
    private PricingResolverStage(long id,
            @Nonnull final CloudCommitmentAnalysisConfig config,
            @Nonnull final CloudCommitmentAnalysisContext context,
            @Nonnull final CloudCommitmentPricingAnalyzerFactory pricingAnalyzerFactory) {
        super(id, config, context);
        this.pricingAnalyzerFactory = pricingAnalyzerFactory;
    }

    @Nonnull
    @Override
    public StageResult<PricingResolverOutput> execute(
            SpecMatcherOutput specMatcherOutput) throws CloudCostDataRetrievalException {

        final CloudCommitmentPricingAnalyzer pricingAnalyzer =
                pricingAnalyzerFactory.newPricingAnalyzer(analysisContext.getCloudTierTopology());

        CommitmentSpecDemandSet commitmentSpecDemandSet = specMatcherOutput.commitmentSpecDemandSet();
        Set<CommitmentSpecDemand> commitmentSpecDemands = commitmentSpecDemandSet.commitmentSpecDemand();
        Set<RateAnnotatedCommitmentContext> rateAnnotatedCommitmentContextSet = new HashSet<>();

        for (CommitmentSpecDemand commitmentSpecDemand: commitmentSpecDemands) {
            final Builder rateAnnotatedCommitmentContext = RateAnnotatedCommitmentContext.builder()
                    .cloudCommitmentSpecData(commitmentSpecDemand.cloudCommitmentSpecData());
            final Set<ScopedCloudTierInfo> cloudTierInfoSet = commitmentSpecDemand.cloudTierInfo();
            final Set<Long> demandScopedBusinessAccountOids = commitmentSpecDemand.cloudTierInfo()
                    .stream()
                    .map(ScopedCloudTierInfo::accountOid)
                    .collect(Collectors.toSet());
            final CloudCommitmentPricingData cloudCommitmentPricingData =
                    pricingAnalyzer.getCloudCommitmentPricing(
                            commitmentSpecDemand.cloudCommitmentSpecData(),
                            demandScopedBusinessAccountOids);
            if (cloudCommitmentPricingData != null) {
                rateAnnotatedCommitmentContext.cloudCommitmentPricingData(cloudCommitmentPricingData);
            } else {
                logger.error("No pricing found for cloud commitment with data {}. Setting 0 price"
                        + "for cloud commitments on the rate annotated commitment context.", commitmentSpecDemand.cloudCommitmentSpecData());
                continue;
            }
            for (ScopedCloudTierInfo cloudTierInfo : cloudTierInfoSet) {
                if (!(cloudTierInfo.cloudTierDemand() instanceof ComputeTierDemand)) {
                    logger.error("The cloud tier demand is not an instance of compute tier demand. Ignoring this demand {}"
                            + "for price calculation", cloudTierInfo.cloudTierDemand());
                    continue;
                }
                final CloudTierPricingData scopedDemandPricingData =
                        pricingAnalyzer.getTierDemandPricing(cloudTierInfo);
                if (scopedDemandPricingData != null && !scopedDemandPricingData.isEmpty()) {
                    rateAnnotatedCommitmentContext.putCloudTierPricingByScope(
                            cloudTierInfo,
                            scopedDemandPricingData);
                } else {
                    logger.error("No pricing found for demand with data {}. Skipping demand.",
                            cloudTierInfo);
                }

            }
            rateAnnotatedCommitmentContextSet.add(rateAnnotatedCommitmentContext.build());
        }
       PricingResolverOutput.Builder pricingResolverOutputBuilder = PricingResolverOutput.builder()
               .addAllRateAnnotatedCommitmentContextSet(rateAnnotatedCommitmentContextSet).analysisTopology(specMatcherOutput.analysisTopology());
       StageResult.Builder<PricingResolverOutput> outputBuilder = StageResult.<PricingResolverOutput>builder().output(pricingResolverOutputBuilder.build());
       return outputBuilder.build();
    }

    @Nonnull
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * A factory class used for creating the PricingResolverStage.
     */
    public static class PricingResolverStageFactory implements AnalysisStage.StageFactory<SpecMatcherOutput, PricingResolverOutput> {

        private final CloudCommitmentPricingAnalyzerFactory pricingAnalyzerFactory;

        /**
         * Constructor for the PricingResolverStageFactory.
         *
         * @param pricingAnalyzerFactory Takes in the cloud commitment pricing analyzer factory.
         */
        public PricingResolverStageFactory(CloudCommitmentPricingAnalyzerFactory pricingAnalyzerFactory) {
            this.pricingAnalyzerFactory = pricingAnalyzerFactory;
        }

        @Nonnull
        @Override
        public AnalysisStage<SpecMatcherOutput, PricingResolverOutput> createStage(
                long id, @Nonnull CloudCommitmentAnalysisConfig config,
                @Nonnull CloudCommitmentAnalysisContext context) {

            return new PricingResolverStage(id, config, context, pricingAnalyzerFactory);
        }
    }
}
