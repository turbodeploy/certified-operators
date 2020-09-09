package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector;

/**
 * Represents the pipeline of analysis selection, demand transformation, recommendation selection,
 * and demand aggregation.
 */
public class DemandTransformationPipeline {

    private final AnalysisSelector analysisSelector;

    private final DemandTransformer demandTransformer;

    private final RecommendationSelector recommendationSelector;

    private final AggregateDemandCollector aggregateDemandCollector;

    private DemandTransformationPipeline(@Nonnull AnalysisSelector analysisSelector,
                                         @Nonnull DemandTransformer demandTransformer,
                                         @Nonnull RecommendationSelector recommendationSelector,
                                         @Nonnull AggregateDemandCollector aggregateDemandCollector) {

        this.analysisSelector = Objects.requireNonNull(analysisSelector);
        this.demandTransformer = Objects.requireNonNull(demandTransformer);
        this.recommendationSelector = Objects.requireNonNull(recommendationSelector);
        this.aggregateDemandCollector = Objects.requireNonNull(aggregateDemandCollector);
    }

    /**
     * Runs the transformation pipeline on the provided set of entity aggregates.
     * @param entityAggregateSet The set of entity aggregate demand to transform and collect.
     */
    public void transformAndCollectDemand(@Nonnull Set<ClassifiedEntityDemandAggregate> entityAggregateSet) {

        entityAggregateSet.stream()
                .map(analysisSelector::filterEntityAggregate)
                .filter(ClassifiedEntityDemandAggregate::hasClassifiedDemand)
                .map(demandTransformer::transformDemand)
                .filter(ClassifiedEntityDemandAggregate::hasClassifiedDemand)
                .map(recommendationSelector::selectRecommendationDemand)
                .flatMap(Set::stream)
                .forEach(aggregateDemandCollector::collectEntitySelection);
    }

    /**
     * A factory class for producing {@link DemandTransformationPipeline} instances.
     */
    public static class DemandTransformationPipelineFactory {

        /**
         * Creates a new {@link DemandTransformationPipeline} instance.
         * @param analysisSelector Selects demand for further analysis. This is the completion
         *                         of the historical demand selection, filtering out demand once
         *                         it is properly classified.
         * @param demandTransformer Transformers the recorded demand.
         * @param recommendationSelector Marks all demand as either considered for a recommendation
         *                               or only considered for coverage analysis.
         * @param aggregateDemandCollector Aggregates and collects all demand as the output of the pipeline.
         * @return The newly created {@link DemandTransformationPipeline} instance.
         */
        @Nonnull
        public DemandTransformationPipeline newPipeline(@Nonnull AnalysisSelector analysisSelector,
                                                        @Nonnull DemandTransformer demandTransformer,
                                                        @Nonnull RecommendationSelector recommendationSelector,
                                                        @Nonnull AggregateDemandCollector aggregateDemandCollector) {

            return new DemandTransformationPipeline(
                    analysisSelector,
                    demandTransformer,
                    recommendationSelector,
                    aggregateDemandCollector);
        }
    }
}
