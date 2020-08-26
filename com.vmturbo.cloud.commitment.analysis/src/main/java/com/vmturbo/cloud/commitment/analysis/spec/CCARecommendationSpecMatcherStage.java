package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStageResult.Builder;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.data.CloudTierCoverageDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;

/**
 * The CCARecommendationSpecMatcherStage class is responsible for resolving uncovered demand into RI specs
 * which make up a list of potential recommendations.
 */
public class CCARecommendationSpecMatcherStage extends AbstractStage<CloudTierCoverageDemand, CommitmentSpecDemandSet> {

    private static final String STAGE_NAME = "Commitment Spec Matcher";

    private static final Logger logger = LogManager.getLogger();

    private final boolean logDetailedSummary;

    private final CommitmentPurchaseProfile commitmentPurchaseProfile;

    /**
     * Constructor of the CCARecommendationSpecMatcherStage.
     *
     * @param id id of the CCA analysis.
     * @param config The cloud commitment analysis config.
     * @param context The cloud commitment analysis context.
     */
    public CCARecommendationSpecMatcherStage(long id,
            @Nonnull final CloudCommitmentAnalysisConfig config,
            @Nonnull final CloudCommitmentAnalysisContext context) {
        super(id, config, context);
        this.commitmentPurchaseProfile = config.getPurchaseProfile();
        this.logDetailedSummary = commitmentPurchaseProfile.getLogDetailedSummary();
    }

    @Nonnull
    @Override
    public AnalysisStage.StageResult<CommitmentSpecDemandSet> execute(CloudTierCoverageDemand cloudTierCoverageDemand) {
        ReservedInstanceSpecMatcher specMatcher = (ReservedInstanceSpecMatcher)analysisContext.getCloudCommitmentSpecMatcher();
        Map<CloudCommitmentSpecData, Set<AggregateCloudTierDemand>> specByDemand = new HashMap<>();
        for (AggregateCloudTierDemand scopedCloudTierDemand : cloudTierCoverageDemand.demandSegment().aggregateCloudTierDemand()) {
            Optional<ReservedInstanceSpecData> reservedInstanceSpecData = specMatcher.matchDemandToSpecs(scopedCloudTierDemand);
            if (reservedInstanceSpecData.isPresent()) {
                specByDemand.computeIfAbsent(reservedInstanceSpecData.get(), t -> new HashSet<>()).add(scopedCloudTierDemand);
            } else {
                logger.debug(
                        "No RI Specs found for entities {} from account {} having tier type {} and demand {}",
                        scopedCloudTierDemand.entityOids(), scopedCloudTierDemand.accountOid(),
                        scopedCloudTierDemand.cloudTierType(), scopedCloudTierDemand.demandAmount());
            }
        }
        final CCARecommendationSpecMatcherStageSummary recommendationSpecMatcherStageSummary = new CCARecommendationSpecMatcherStageSummary(logDetailedSummary);
        CommitmentSpecDemandSet.Builder commitmentSpecDemandSetBuilder = CommitmentSpecDemandSet.builder();
        for (Map.Entry<CloudCommitmentSpecData, Set<AggregateCloudTierDemand>> entry : specByDemand.entrySet()) {
            CommitmentSpecDemand commitmentSpecDemand = CommitmentSpecDemand.builder().addAllAggregateCloudTierDemandSet(
                    entry.getValue()).cloudCommitmentSpecData(entry.getKey()).build();
            commitmentSpecDemandSetBuilder.addCommitmentSpecDemand(commitmentSpecDemand);
        }

        CommitmentSpecDemandSet output = commitmentSpecDemandSetBuilder.build();
        Builder<CommitmentSpecDemandSet> builder = ImmutableStageResult.<CommitmentSpecDemandSet>builder().output(output);
        builder.resultSummary(recommendationSpecMatcherStageSummary.toSummaryCollector(
                    cloudTierCoverageDemand, specByDemand));
        return builder.build();
    }

    @Nonnull
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * Static class representing the detailed summary of the demand.
     */
    @NotThreadSafe
    private static class CCARecommendationSpecMatcherStageSummary {

        private final boolean logDetailedSummary;

        /**
         * The CCARecommendationSpecMatcherStageSummary takes in the logDetailedSummary boolean.
         *
         * @param logDetailedSummary true if we want a detailed summary logged.
         */
        CCARecommendationSpecMatcherStageSummary(boolean logDetailedSummary) {
            this.logDetailedSummary = logDetailedSummary;
        }

        private static final String ccaRecommendationSpecMatcherStageSummaryTemplate =
                "Number of demand records: <numDemandRecords>\n"
                        + "Number of RI Specs matched: <numRISpecsMatched>\n"
                        + "Percentage of RI Specs Matched: <percentageOfRISpecsMatched>\n";

        public String toSummaryCollector(final CloudTierCoverageDemand cloudTierCoverageDemand,
                final Map<CloudCommitmentSpecData, Set<AggregateCloudTierDemand>> specByDemandMap) {
            int specByDemandMapSize = specByDemandMap.size();
            int cloudTierCoverageDemandSize = cloudTierCoverageDemand.demandSegment().aggregateCloudTierDemand().size();
            final ST template = new ST(ccaRecommendationSpecMatcherStageSummaryTemplate);
            template.add("NUMBER OF RECORDS TO MATCH:", cloudTierCoverageDemandSize);
            template.add("TOTAL RI SPECS MATCHED:", specByDemandMapSize);
            if (logDetailedSummary) {
                for (Map.Entry<CloudCommitmentSpecData, Set<AggregateCloudTierDemand>> entry : specByDemandMap
                        .entrySet()) {
                    CloudCommitmentSpecData spec = entry.getKey();
                    Set<AggregateCloudTierDemand> demand = entry.getValue();
                    template.add("Spec: ", spec.spec());
                    // This is basically the percentage of records an RI spec matches to. Lets say you have
                    // 100 demand records and this RI spec matched to 2 of them. The percentage should be
                    // 2 %.
                    template.add("matched to % of demand: ",
                            (float)demand.size() / (float)cloudTierCoverageDemandSize * 100);
                }
            }
            return template.render();
        }

    }

    /**
     * The CCARecommendationSpecMatcherStageFactory class is responsible for creating an instance of
     * the CCARecommendationSpecMatcherStage.
     */
    public static class CCARecommendationSpecMatcherStageFactory implements
            AnalysisStage.StageFactory<CloudTierCoverageDemand, CommitmentSpecDemandSet> {

        @Nonnull
        @Override
        public AnalysisStage<CloudTierCoverageDemand, CommitmentSpecDemandSet> createStage(long id,
                @Nonnull CloudCommitmentAnalysisConfig config,
                @Nonnull CloudCommitmentAnalysisContext context) {
            return new CCARecommendationSpecMatcherStage(id, config, context);
        }
    }
}
