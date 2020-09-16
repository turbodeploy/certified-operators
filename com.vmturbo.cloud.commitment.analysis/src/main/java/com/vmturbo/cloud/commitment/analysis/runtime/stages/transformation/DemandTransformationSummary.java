package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;

/**
 * Generates a string summary for demand transformation.
 */
public class DemandTransformationSummary {

    private static final String TRANSFORMATION_SUMMARY =
            "Aggregated Entities: <aggregatedEntities>\n"
            + "Aggregated Demand: <aggregateDemand>\n"
            + "Skipped Entities: <skippedEntities>\n"
            + "Skipped Demand Duration: <skippedDemandDuration>\n"
            + "Analysis Demand Amount: <analysisDemandAmount>\n"
            + "Recommendation Demand Amount: <recommendationDemandAmount>\n";

    private static final String DETAILED_TRANSFORMATION_SUMMARY =
            TRANSFORMATION_SUMMARY
            + "Demand x Aggregation:\n"
            + "    <aggregateDemandSummary;separator=\"    \\n\">\n"
            + "Demand x Bucket:\n"
            + "    <bucketDemandSummary;separator=\"    \\n\">\n";


    private static final String AGGREGATE_DEMAND_SUMMARY =
            "    <aggregateDemandScope>: <aggregateDemandStats>";

    private static final String BUCKET_DEMAND_SUMMARY =
            "    <bucketDemandTime>: <bucketDemandStats>";

    private final DemandTransformationResult transformationResult;

    private final DemandTransformationStatistics transformationStats;

    private final boolean detailedSummary;


    private DemandTransformationSummary(@Nonnull DemandTransformationResult transformationResult,
                                        boolean detailedSummary) {
        this.transformationResult = Objects.requireNonNull(transformationResult);
        this.transformationStats = transformationResult.transformationStats();
        this.detailedSummary = detailedSummary;
    }

    /**
     * Generates a summary of the {@link DemandTransformationResult}.
     * @return The generated summary string.
     */
    @Override
    public String toString() {
        return generateSummary();
    }

    private String generateSummary() {

        final ST template = new ST(detailedSummary
                ? DETAILED_TRANSFORMATION_SUMMARY
                : TRANSFORMATION_SUMMARY);

        template.add("aggregatedEntities", transformationStats.aggregatedEntitiesCount());
        template.add("aggregateDemand", transformationStats.uniqueAggregateDemand());
        template.add("skippedEntities", transformationStats.skippedEntitiesCount());
        template.add("skippedDemandDuration", transformationStats.skippedDemandDuration());
        template.add("analysisDemandAmount", transformationStats.analysisAmount());
        template.add("recommendationDemandAmount", transformationStats.recommendationAmount());

        if (detailedSummary) {
            final List<String> aggregateDemandSummaries = transformationStats.demandStatsByTierScope()
                    .entrySet()
                    .stream()
                    .map(e -> {
                        final ST aggregateTemplate = new ST(AGGREGATE_DEMAND_SUMMARY);
                        aggregateTemplate.add("aggregateDemandScope", e.getKey());
                        aggregateTemplate.add("aggregateDemandStats", e.getValue());

                        return aggregateTemplate.render();
                    }).collect(Collectors.toList());
            template.add("aggregateDemandSummary", aggregateDemandSummaries);


            final List<String> bucketDemandSummaries = new ArrayList<>();
            transformationStats.demandStatsByBucket().forEach((bucket, bucketStats) -> {

                final ST bucketTemplate = new ST(BUCKET_DEMAND_SUMMARY);

                bucketTemplate.add("bucketDemandTime", bucket);
                bucketTemplate.add("bucketDemandStats", bucketStats);

                bucketDemandSummaries.add(bucketTemplate.render());
            });
            template.add("bucketDemandSummary", bucketDemandSummaries);
        }

        return template.render();
    }

    /**
     * Generates a string summary of the provided {@code aggregationResult}.
     * @param transformationResult The transformation result to summarize.
     * @param detailedSummary Whether detailed stats (e.g. demand by scope and analysis bucket) should
     *                        be included in the generated summary.
     * @return The generated summary.
     */
    @Nonnull
    public static String generateSummary(@Nonnull DemandTransformationResult transformationResult,
                                         boolean detailedSummary) {

        final DemandTransformationSummary summary = new DemandTransformationSummary(
                transformationResult, detailedSummary);
        return summary.toString();
    }
}
