package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandCollector.AggregateDemandCollectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationPipeline.DemandTransformationPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.FlexibleRIComputeTransformer.FlexibleRIComputeTransformerFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector.AnalysisSelectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector.RecommendationSelectorFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * A factory for generating a {@link DemandTransformationPipeline} for allocated demand.
 */
public class AllocatedTransformationPipelineFactory {

    private final Logger logger = LogManager.getLogger();

    private final DemandTransformationPipelineFactory demandTransformationPipelineFactory;

    private final AnalysisSelectorFactory analysisSelectorFactory;

    private final FlexibleRIComputeTransformerFactory flexibleRIComputeTransformerFactory;

    private final RecommendationSelectorFactory recommendationSelectorFactory;

    private final AggregateDemandCollectorFactory aggregateDemandCollectorFactory;

    /**
     * Constructs a new factory instance.
     * @param demandTransformationPipelineFactory A factory for creating demand transformation pipelines.
     * @param analysisSelectorFactory A factory for {@link AnalysisSelector} instances.
     * @param flexibleRIComputeTransformerFactory A factory for {@link FlexibleRIComputeTransformer} instances.
     * @param recommendationSelectorFactory A factory for {@link RecommendationSelector} instances.
     * @param aggregateDemandCollectorFactory A factory for {@link AggregateDemandCollector} instances.
     */
    public AllocatedTransformationPipelineFactory(
            @Nonnull DemandTransformationPipelineFactory demandTransformationPipelineFactory,
            @Nonnull AnalysisSelectorFactory analysisSelectorFactory,
            @Nonnull FlexibleRIComputeTransformerFactory flexibleRIComputeTransformerFactory,
            @Nonnull RecommendationSelectorFactory recommendationSelectorFactory,
            @Nonnull AggregateDemandCollectorFactory aggregateDemandCollectorFactory) {

        this.demandTransformationPipelineFactory = Objects.requireNonNull(demandTransformationPipelineFactory);
        this.analysisSelectorFactory = Objects.requireNonNull(analysisSelectorFactory);
        this.flexibleRIComputeTransformerFactory = Objects.requireNonNull(flexibleRIComputeTransformerFactory);
        this.recommendationSelectorFactory = Objects.requireNonNull(recommendationSelectorFactory);
        this.aggregateDemandCollectorFactory = Objects.requireNonNull(aggregateDemandCollectorFactory);

    }

    /**
     * Constructs a new {@link DemandTransformationPipeline} for allocated demand, based on the provided
     * config.
     * @param analysisConfig The analysis config.
     * @param analysisContext THe analysis context.
     * @param transformationJournal The {@link DemandTransformationJournal}, used to construct the
     *                              discrete stages of the pipeline.
     * @return The newly constructed {@link DemandTransformationPipeline}.
     */
    @Nonnull
    public DemandTransformationPipeline newPipeline(
            @Nonnull CloudCommitmentAnalysisConfig analysisConfig,
            @Nonnull CloudCommitmentAnalysisContext analysisContext,
            @Nonnull DemandTransformationJournal transformationJournal) {

        final HistoricalDemandSelection demandSelection = analysisConfig.getDemandSelection();

        // The initialization stage will verify this is set
        final AllocatedDemandSelection analysisSelection = demandSelection.getAllocatedSelection();
        final AnalysisSelector analysisSelector = analysisSelectorFactory.newSelector(
                transformationJournal, analysisSelection);

        final DemandTransformer demandTransformer = createFlexibleDemandTransformer(
                transformationJournal,
                demandSelection,
                analysisContext);

        // The initialization stage will verify this is set
        final AllocatedDemandSelection recommendationSelection = analysisConfig.getPurchaseProfile()
                .getAllocatedSelection();
        final RecommendationSelector recommendationSelector =
                recommendationSelectorFactory.fromDemandSelection(recommendationSelection);

        final TimeInterval analysisWindow = analysisContext.getAnalysisWindow()
                .orElseThrow(() -> new IllegalStateException("Analysis window must be set"));
        final AggregateDemandCollector aggregateDemandCollector =
                aggregateDemandCollectorFactory.newCollector(
                        transformationJournal,
                        analysisWindow,
                        analysisContext.getAnalysisBucket());

        return demandTransformationPipelineFactory.newPipeline(
                analysisSelector,
                demandTransformer,
                recommendationSelector,
                aggregateDemandCollector);
    }

    @Nonnull
    private DemandTransformer createFlexibleDemandTransformer(
            @Nonnull DemandTransformationJournal transformationJournal,
            @Nonnull HistoricalDemandSelection demandSelection,
            @Nonnull CloudCommitmentAnalysisContext analysisContext) {

        if (demandSelection.getCloudTierType() == CloudTierType.COMPUTE_TIER
                && analysisContext.getRecommendationType() == CloudCommitmentType.RESERVED_INSTANCE) {

            logger.info("Creating RI Compute demand transformer");

            return flexibleRIComputeTransformerFactory.newTransformer(
                    transformationJournal,
                    analysisContext.getComputeTierFamilyResolver());
        } else {
            throw new NotImplementedException("Only RI allocation demand transformation is currently supported");
        }
    }
}
