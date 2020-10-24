package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemandSet;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecMatcher;
import com.vmturbo.cloud.commitment.analysis.spec.SpecMatcherOutput;
import com.vmturbo.cloud.commitment.analysis.spec.SpecMatcherOutput.Builder;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;

/**
 * The CCARecommendationSpecMatcherStage class is responsible for resolving uncovered demand into RI specs
 * which make up a list of potential recommendations.
 */
public class RecommendationSpecMatcherStage extends AbstractStage<AnalysisTopology, SpecMatcherOutput> {

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
    public RecommendationSpecMatcherStage(long id,
            @Nonnull final CloudCommitmentAnalysisConfig config,
            @Nonnull final CloudCommitmentAnalysisContext context) {
        super(id, config, context);
        this.commitmentPurchaseProfile = config.getPurchaseProfile();
        this.logDetailedSummary = commitmentPurchaseProfile.getLogDetailedSummary();
    }

    @Nonnull
    @Override
    public AnalysisStage.StageResult<SpecMatcherOutput> execute(AnalysisTopology analysisTopology) {
        final ReservedInstanceSpecMatcher specMatcher = (ReservedInstanceSpecMatcher)analysisContext.getCloudCommitmentSpecMatcher();
        final Map<CloudCommitmentSpecData, Set<ScopedCloudTierInfo>> specByDemand = new HashMap<>();
        analysisTopology.segments().stream().forEach(s -> {

            // Only match specs to recommendation candidate demand.
            Set<ScopedCloudTierInfo> cloudTierInfoSet = s.aggregateCloudTierDemandSet().entries()
                    .stream()
                    .filter(demandEntry -> demandEntry.getValue().isRecommendationCandidate())
                    .map(Map.Entry::getKey)
                    .collect(ImmutableSet.toImmutableSet());
            for (ScopedCloudTierInfo cloudTierInfo : cloudTierInfoSet) {
                final Optional<ReservedInstanceSpecData> reservedInstanceSpecData =
                        specMatcher.matchDemandToSpecs(cloudTierInfo);

                if (reservedInstanceSpecData.isPresent()) {
                    specByDemand.computeIfAbsent(reservedInstanceSpecData.get(), t -> new HashSet<>())
                            .add(cloudTierInfo);
                } else {
                    logger.debug(
                            "No RI Specs found for entities from account {} having tier type {}",
                            cloudTierInfo.accountOid(), cloudTierInfo.cloudTierType());
                }
            }
        });
        final CCARecommendationSpecMatcherStageSummary recommendationSpecMatcherStageSummary = new CCARecommendationSpecMatcherStageSummary(logDetailedSummary);
        Builder specMatcherOutputBuilder = SpecMatcherOutput.builder();
        CommitmentSpecDemandSet.Builder commitmentSpecDemandSetBuilder = CommitmentSpecDemandSet.builder();
        for (Map.Entry<CloudCommitmentSpecData, Set<ScopedCloudTierInfo>> entry : specByDemand.entrySet()) {
            CommitmentSpecDemand commitmentSpecDemand = CommitmentSpecDemand.builder()
                    .addAllCloudTierInfo(entry.getValue())
                    .cloudCommitmentSpecData(entry.getKey())
                    .build();
            commitmentSpecDemandSetBuilder.addCommitmentSpecDemand(commitmentSpecDemand);
        }
        specMatcherOutputBuilder.analysisTopology(analysisTopology);
        specMatcherOutputBuilder.commitmentSpecDemandSet(commitmentSpecDemandSetBuilder.build());
        SpecMatcherOutput specMatcherOutput = specMatcherOutputBuilder.build();
        StageResult.Builder<SpecMatcherOutput> builder = StageResult.<SpecMatcherOutput>builder().output(specMatcherOutput);
        builder.resultSummary(recommendationSpecMatcherStageSummary.toSummaryCollector(analysisTopology, specByDemand));
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

        private static final String SPEC_MATCHER_SUMMARY =
                "Number of scoped info records: <numDemandRecords>\n"
                        + "Number of RI Specs matched: <numRISpecsMatched>\n";

        public String toSummaryCollector(final AnalysisTopology analysisTopology,
                final Map<CloudCommitmentSpecData, Set<ScopedCloudTierInfo>> specByDemandMap) {

            final long scopedInfoSize = specByDemandMap.values()
                    .stream()
                    .mapToLong(Set::size)
                    .reduce(0L, Long::sum);
            final int specByDemandMapSize = specByDemandMap.size();

            final ST template = new ST(SPEC_MATCHER_SUMMARY);
            template.add("numDemandRecords", scopedInfoSize);
            template.add("numRISpecsMatched", specByDemandMapSize);

            return template.render();
        }
    }

    /**
     * The CCARecommendationSpecMatcherStageFactory class is responsible for creating an instance of
     * the CCARecommendationSpecMatcherStage.
     */
    public static class RecommendationSpecMatcherStageFactory implements
            AnalysisStage.StageFactory<AnalysisTopology, SpecMatcherOutput> {

        @Nonnull
        @Override
        public AnalysisStage<AnalysisTopology, SpecMatcherOutput> createStage(long id,
                @Nonnull CloudCommitmentAnalysisConfig config,
                @Nonnull CloudCommitmentAnalysisContext context) {
            return new RecommendationSpecMatcherStage(id, config, context);
        }
    }
}
