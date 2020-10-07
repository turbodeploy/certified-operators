package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import static com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData.EMPTY_RI_PRICING_DATA;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingAnalyzer;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.PricingResolverOutput;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext.Builder;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedDemandPricing;
import com.vmturbo.cloud.commitment.analysis.pricing.TierDemandPricingData;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemandSet;
import com.vmturbo.cloud.commitment.analysis.spec.SpecMatcherOutput;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The pricing resolver stage is used to resolve pricing for given commitment spec demands.
 */
public class PricingResolverStage extends AbstractStage<SpecMatcherOutput, PricingResolverOutput> {
    private static final String stageName = "PRICING RESOLVER";

    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentPricingAnalyzer cloudCommitmentPricingAnalyzer;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    /**
     * Constructor for the pricing resolver stage.
     *
     * @param id The cloud commitment analysis context id.
     * @param config The cloud commitment analysis config.
     * @param context The cloud commitment analysis context.
     * @param cloudCommitmentPricingAnalyzer The cloud commitment pricing analyzer which is responsible for
     * the actual resolution of pricing.
     */
    public PricingResolverStage(long id,
            @Nonnull final CloudCommitmentAnalysisConfig config,
            @Nonnull final CloudCommitmentAnalysisContext context,
            @Nonnull final CloudCommitmentPricingAnalyzer cloudCommitmentPricingAnalyzer) {
        super(id, config, context);
        this.cloudCommitmentPricingAnalyzer = cloudCommitmentPricingAnalyzer;
        this.cloudTopology = context.getCloudTierTopology();
    }

    @Nonnull
    @Override
    public StageResult<PricingResolverOutput> execute(
            SpecMatcherOutput specMatcherOutput) {
        CommitmentSpecDemandSet commitmentSpecDemandSet = specMatcherOutput.commitmentSpecDemandSet();
        Set<CommitmentSpecDemand> commitmentSpecDemands = commitmentSpecDemandSet.commitmentSpecDemand();
        Set<RateAnnotatedCommitmentContext> rateAnnotatedCommitmentContextSet = new HashSet<>();
        for (CommitmentSpecDemand commitmentSpecDemand: commitmentSpecDemands) {
            Builder rateAnnotatedCommitmentContext = RateAnnotatedCommitmentContext.builder();
            Set<AggregateCloudTierDemand> aggregateCloudTierDemandSet = commitmentSpecDemand.aggregateCloudTierDemandSet();
            Set<Long> demandScopedBusinessAccountOids = commitmentSpecDemand.aggregateCloudTierDemandSet().stream().map(s -> s.accountOid()).collect(
                    Collectors.toSet());
            CloudCommitmentPricingData cloudCommitmentPricingData = cloudCommitmentPricingAnalyzer
                    .getCloudCommitmentPricing(commitmentSpecDemand.cloudCommitmentSpecData(), demandScopedBusinessAccountOids, cloudTopology);
            if (cloudCommitmentPricingData != null) {
                rateAnnotatedCommitmentContext.cloudCommitmentPricingData(cloudCommitmentPricingData);
            } else {
                logger.error("No pricing found for cloud commitment with data {}. Setting 0 price"
                        + "for cloud commitments on the rate annotated commitment context.", cloudCommitmentPricingData.toString());
                rateAnnotatedCommitmentContext.cloudCommitmentPricingData(EMPTY_RI_PRICING_DATA);
            }
            for (AggregateCloudTierDemand aggregateCloudTierDemand: aggregateCloudTierDemandSet) {
                if (!(aggregateCloudTierDemand.cloudTierDemand() instanceof ComputeTierDemand)) {
                    logger.error("The cloud tier demand is not an instance of compute tier demand. Ignoring this demand {}"
                            + "for price calculation", aggregateCloudTierDemand.cloudTierDemand());
                    continue;
                }
                RateAnnotatedDemandPricing.Builder rateAnnotatedDemandPricingBuilder = RateAnnotatedDemandPricing
                        .builder().scopedCloudTierDemand(aggregateCloudTierDemand);
                TierDemandPricingData scopedDemandPricingData = cloudCommitmentPricingAnalyzer.getTierDemandPricing(aggregateCloudTierDemand, cloudTopology);
                if (scopedDemandPricingData != null) {
                    rateAnnotatedDemandPricingBuilder.tierDemandPricingData(scopedDemandPricingData);
                } else {
                    logger.error("No pricing found for demand with data {}. Setting 0 price"
                            + "for aggregate cloud tier demand on the rate annotated commitment context.", aggregateCloudTierDemand.toString());
                    rateAnnotatedDemandPricingBuilder.tierDemandPricingData(TierDemandPricingData.EMPTY_TIER_DEMAND_PRICING_DATA);
                }
                rateAnnotatedCommitmentContext.addScopedDemandPricingData(rateAnnotatedDemandPricingBuilder.build());
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
        return stageName;
    }

    /**
     * A factory class used for creating the PricingResolverStage.
     */
    public static class PricingResolverStageFactory implements AnalysisStage.StageFactory<SpecMatcherOutput, PricingResolverOutput> {

        private final CloudCommitmentPricingAnalyzer cloudCommitmentPricingAnalyzer;

        /**
         * Constructor for the PricingResolverStageFactory.
         *
         * @param cloudCommitmentPricingAnalyzer Takes in the cloud commitment pricing analyzer.
         */
        public PricingResolverStageFactory(CloudCommitmentPricingAnalyzer cloudCommitmentPricingAnalyzer) {
            this.cloudCommitmentPricingAnalyzer = cloudCommitmentPricingAnalyzer;
        }

        @Nonnull
        @Override
        public AnalysisStage<SpecMatcherOutput, PricingResolverOutput> createStage(
                long id, @Nonnull CloudCommitmentAnalysisConfig config,
                @Nonnull CloudCommitmentAnalysisContext context) {
            return new PricingResolverStage(id, config, context, cloudCommitmentPricingAnalyzer);
        }
    }
}
