package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.RecommendationAppraisal;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;

/**
 * Generates a human-readable summary of the {@link RecommendationAnalysisStage} output.
 */
public class RecommendationSummary {

    private static final String RECOMMENDATIONS_SUMMARY =
            "<recommendationSummaries;separator=\"\\n\">\n";

    private static final String RESERVED_INSTANCE_INFO_SUMMARY =
            "Cloud Tier: <cloudTierName> (SizeFlexible=<sizeFlexible> OID=<cloudTierOid>)\n"
                    + "Region: <regionName> (OID=<regionOid>)\n"
                    + "Platform: <platform> (PlatformFlexible=<platformFlexible>)\n"
                    + "Purchasing Account: <purchasingAccountName> (OID=<purchasingAccountOid>)\n";

    private static final String SAVINGS_CALCULATION_APPRAISAL_SUMMARY =
            "Count <recommendationCount>\n"
                    + "Total Recorded Demand: <aggregateNormalizedDemand>\n"
                    + "Inventory Coverage: <inventoryCoverage>%\n"
                    + "Total Uncovered On-demand Cost: <aggregateOnDemandCost>\n"
                    + "Covered On-Demand Cost: <coveredOnDemandCost>\n"
                    + "Recommendation Coverage Percentage: <recommendationCoverage>%\n"
                    + "Recommendation Cost: <recommendationCost>\n"
                    + "Savings Over On-Demand: <savingsOverOnDemand>%\n"
                    + "Recommendation Utilization: <recommendationUtilization>\n";

    private static final String RECOMMENDATION_SUMMARY =
            "======== <recommendationType> Summary ========\n"
                    + "<recommendationInfo>"
                    + "<appraisalSummary>"
                    + "===========================================\n";

    private static final String RECOMMENDATION_WITH_REJECTION_SUMMARY =
            "======== <recommendationType> Summary ========\n"
                    + "<recommendationInfo>"
                    + "<appraisalSummary>"
                    + "Last Rejected Recommendation:\n"
                    + "<rejectedAppraisal>"
                    + "===========================================\n";

    private final MinimalCloudTopology<MinimalEntity> cloudTopology;

    private final List<CloudCommitmentRecommendation> recommendations;

    private RecommendationSummary(@Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology,
                                  @Nonnull Collection<CloudCommitmentRecommendation> recommendations) {

        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.recommendations = ImmutableList.copyOf(Objects.requireNonNull(recommendations));
    }

    @Nonnull
    private String convertAppraisalToString(@Nonnull RecommendationAppraisal appraisal) {
        final ST appraisalTemplate = new ST(SAVINGS_CALCULATION_APPRAISAL_SUMMARY);

        appraisalTemplate.add("recommendationCount", appraisal.recommendationQuantity());
        appraisalTemplate.add("aggregateNormalizedDemand", appraisal.aggregateNormalizedDemand());
        appraisalTemplate.add("inventoryCoverage", appraisal.inventoryCoverage());
        appraisalTemplate.add("aggregateOnDemandCost", appraisal.aggregateOnDemandCost());
        appraisalTemplate.add("coveredOnDemandCost", appraisal.coveredOnDemandCost());
        appraisalTemplate.add("recommendationCoverage", appraisal.recommendationCoverage());
        appraisalTemplate.add("recommendationCost", appraisal.recommendationCost());
        appraisalTemplate.add("savingsOverOnDemand", appraisal.savingsOverOnDemand());
        appraisalTemplate.add("recommendationUtilization", appraisal.recommendationUtilization());

        return appraisalTemplate.render();
    }

    @Nonnull
    private String getDisplayName(long entityOid) {
        return cloudTopology.getEntity(entityOid)
                .map(MinimalEntity::getDisplayName)
                .orElse("N/A");
    }

    private String convertReservedInstanceInfoToString(@Nonnull ReservedInstanceRecommendationInfo recommendationInfo) {

        final ST infoTemplate = new ST(RESERVED_INSTANCE_INFO_SUMMARY);

        infoTemplate.add("cloudTierName", getDisplayName(recommendationInfo.tierOid()));
        infoTemplate.add("sizeFlexible", recommendationInfo.sizeFlexible());
        infoTemplate.add("cloudTierOid", recommendationInfo.tierOid());
        infoTemplate.add("regionName", getDisplayName(recommendationInfo.regionOid()));
        infoTemplate.add("regionOid", recommendationInfo.regionOid());
        infoTemplate.add("platform", recommendationInfo.platform());
        infoTemplate.add("platformFlexible", recommendationInfo.platformFlexible());
        infoTemplate.add("purchasingAccountName", getDisplayName(recommendationInfo.purchasingAccountOid()));
        infoTemplate.add("purchasingAccountOid", recommendationInfo.purchasingAccountOid());

        return infoTemplate.render();
    }

    @Nonnull
    private String toRecommendationSummary(@Nonnull CloudCommitmentRecommendation recommendation) {

        final CloudCommitmentType commitmentType = recommendation.recommendationInfo().commitmentType();
        final SavingsCalculationRecommendation savingsCalculationRecommendation =
                recommendation.savingsCalculationResult().recommendation();

        final Optional<RecommendationAppraisal> rejectedAppraisal =
                savingsCalculationRecommendation.rejectedRecommendation();
        final ST template;
        if (rejectedAppraisal.isPresent()) {
            template = new ST(RECOMMENDATION_WITH_REJECTION_SUMMARY);
            template.add("rejectedAppraisal", "    "
                    + convertAppraisalToString(rejectedAppraisal.get()).replaceAll("\n", "\n    "));
        } else {
            template = new ST(RECOMMENDATION_SUMMARY);
        }

        template.add("recommendationType", commitmentType);
        template.add("appraisalSummary", convertAppraisalToString(savingsCalculationRecommendation));

        if (commitmentType == CloudCommitmentType.RESERVED_INSTANCE) {

            template.add("recommendationInfo", convertReservedInstanceInfoToString(
                    (ReservedInstanceRecommendationInfo)recommendation.recommendationInfo()));
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s is not a supported commitment type", commitmentType));
        }

        return template.render();
    }

    @Nonnull
    private String toSummaryString() {
        final ST template = new ST(RECOMMENDATIONS_SUMMARY);

        template.add("recommendationSummaries",
                recommendations.stream()
                        .sorted(Comparator.comparing(rec ->
                                rec.savingsCalculationResult()
                                        .recommendation()
                                        .recommendationQuantity()))
                        .map(this::toRecommendationSummary)
                        .collect(ImmutableList.toImmutableList()));

        return template.render();
    }

    /**
     * Generates a summary of the {@code recommendations}.
     * @param cloudTopology The cloud topology, used to derive referenced entity display names.
     * @param recommendations The recommendations to summarize.
     * @return The summary string.
     */
    @Nonnull
    public static String generateSummary(@Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology,
                                         @Nonnull Collection<CloudCommitmentRecommendation> recommendations) {

        final RecommendationSummary summary = new RecommendationSummary(cloudTopology, recommendations);
        return summary.toSummaryString();
    }
}
