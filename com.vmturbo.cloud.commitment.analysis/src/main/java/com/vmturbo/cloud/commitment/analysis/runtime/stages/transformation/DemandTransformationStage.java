package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * The demand transformation stage, responsible for filtering entity demand based on state (suspended/terminated),
 * filtering demand based on the classification (e.g. ignoring stale/unstable demand) and aggregating entity demand
 * with the same scope/classification together.
 */
public class DemandTransformationStage extends AbstractStage<ClassifiedEntityDemandSet, AggregateAnalysisDemand> {

    private final Logger logger = LogManager.getLogger();

    private static final String STAGE_NAME = "Demand Transformation";

    private final AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory;

    protected DemandTransformationStage(final long id,
                                        @Nonnull final CloudCommitmentAnalysisConfig analysisConfig,
                                        @Nonnull final CloudCommitmentAnalysisContext analysisContext,
                                        @Nonnull AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory) {

        super(id, analysisConfig, analysisContext);

        this.allocatedTransformationPipelineFactory = Objects.requireNonNull(allocatedTransformationPipelineFactory);
    }

    /**
     * Filters and aggregates the classified entity demand into analyzable aggregated chunks.
     * @param input The classified entity demand set to transform.
     * @return The aggregated analysis demand.
     */
    @Nonnull
    @Override
    public synchronized StageResult<AggregateAnalysisDemand> execute(final ClassifiedEntityDemandSet input) {


        final DemandTransformationJournal transformationJournal = DemandTransformationJournal.newJournal();
        final DemandTransformationPipeline allocatedTransformationPipeline =
                allocatedTransformationPipelineFactory.newPipeline(
                        analysisConfig, analysisContext, transformationJournal);

        allocatedTransformationPipeline.transformAndCollectDemand(input.classifiedAllocatedDemand());

        final TimeInterval analysisWindow = analysisContext.getAnalysisWindow()
                .orElseThrow(() -> new IllegalStateException("Analysis window must be set"));
        final DemandTransformationResult transformationResult = transformationJournal.getTransformationResult();
        return StageResult.builder()
                .output(AnalysisDemandCreator.createAnalysisDemand(
                        transformationResult, analysisWindow, analysisContext.getAnalysisBucket()))
                .resultSummary(DemandTransformationSummary.generateSummary(
                        transformationResult,
                        analysisConfig.getDemandSelection().getLogDetailedSummary()))
                .build();
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * A factory class for producing {@link DemandTransformationStage} instances.
     */
    public static class DemandTransformationFactory implements
            AnalysisStage.StageFactory<ClassifiedEntityDemandSet, AggregateAnalysisDemand> {

        private final AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory;

        /**
         * Constructs a new demand transformation factory.
         * @param allocatedTransformationPipelineFactory A factory for producing demand transformation
         *                                               pipelines for allocated demand.
         */
        public DemandTransformationFactory(
                @Nonnull AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory) {

            this.allocatedTransformationPipelineFactory = Objects.requireNonNull(allocatedTransformationPipelineFactory);
        }

        /**
         * Constructs a new {@link DemandTransformationStage} instance.
         * @param id The unique ID of the stage.
         * @param config The configuration of the analysis.
         * @param context The context of the analysis, used to share context data across stages
         * @return THe newly constructed {@link DemandTransformationStage} instance.
         */
        @Nonnull
        @Override
        public AnalysisStage<ClassifiedEntityDemandSet, AggregateAnalysisDemand> createStage(
                final long id,
                @Nonnull final CloudCommitmentAnalysisConfig config,
                @Nonnull final CloudCommitmentAnalysisContext context) {

            return new DemandTransformationStage(
                    id,
                    config,
                    context,
                    allocatedTransformationPipelineFactory);
        }
    }

}
