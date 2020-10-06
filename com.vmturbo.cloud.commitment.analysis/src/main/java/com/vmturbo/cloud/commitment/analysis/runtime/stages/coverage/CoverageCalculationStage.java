package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopologySegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationSummary.CoverageCalculationSummaryFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationTaskFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.CoverageInfo;
import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * This stage is responsible for taking in all aggregate demand and the cloud commitment inventory
 * and running the coverage analysis, in order to determine the remaining uncovered demand after
 * the commitment inventory has been applied.
 */
public class CoverageCalculationStage extends AbstractStage<AnalysisTopology, AnalysisTopology> {

    private static final String STAGE_NAME = "Coverage Calculation";

    private final Logger logger = LogManager.getLogger();

    private final CoverageCalculationTaskFactory coverageCalculationTaskFactory;

    private final CoverageCalculationSummaryFactory coverageSummaryFactory;

    private final ExecutorService executorService;

    private final Duration coverageTaskSummaryInterval;

    private CoverageCalculationStage(final long id,
                                     @Nonnull final CloudCommitmentAnalysisConfig analysisConfig,
                                     @Nonnull final CloudCommitmentAnalysisContext analysisContext,
                                     @Nonnull CoverageCalculationTaskFactory coverageCalculationTaskFactory,
                                     @Nonnull CoverageCalculationSummaryFactory coverageSummaryFactory,
                                     @Nonnull Duration coverageTaskSummaryInterval) {
        super(id, analysisConfig, analysisContext);

        this.coverageCalculationTaskFactory = Objects.requireNonNull(coverageCalculationTaskFactory);
        this.coverageSummaryFactory = Objects.requireNonNull(coverageSummaryFactory);
        this.coverageTaskSummaryInterval = Objects.requireNonNull(coverageTaskSummaryInterval);

        this.executorService = analysisContext.getAnalysisExecutorService();
    }

    /**
     * Based on the provided analysis topology, containing recorded demand and the cloud commitment inventory,
     * executes the coverage analysis in order to calculate uncovered demand.
     * @param analysisTopology The analysis topology to analyze.
     * @return The analysis topology, updated with coverage data.
     * @throws Exception An exception during execution of the stage.
     */
    @Nonnull
    @Override
    public StageResult<AnalysisTopology> execute(final AnalysisTopology analysisTopology) throws Exception {

        logger.info("Running coverage calculation (Commitments={} Segments={})",
                analysisTopology.cloudCommitmentsByOid().size(),
                analysisTopology.segments().size());

        final Set<CoverageCalculationTask> calculationTasks = createCalculationTasks(analysisTopology);
        final List<CoverageCalculationInfo> calculationResults = executeCalculationTasks(calculationTasks);

        final AnalysisTopology updatedTopology = processResults(analysisTopology, calculationResults);

        final CoverageCalculationSummary coverageSummary = coverageSummaryFactory.newSummary(
                updatedTopology, calculationResults);
        return StageResult.<AnalysisTopology>builder()
                .output(updatedTopology)
                .resultSummary(coverageSummary.toString())
                .build();
    }

    private List<CoverageCalculationInfo> executeCalculationTasks(
            @Nonnull Set<CoverageCalculationTask> calculationTasks) throws ExecutionException, InterruptedException {

        final List<CoverageCalculationInfo> calculationResults = new ArrayList<>();
        final ExecutorCompletionService<CoverageCalculationInfo> completionService =
                new ExecutorCompletionService(executorService);
        calculationTasks.forEach(completionService::submit);

        final Stopwatch aggregateTime = Stopwatch.createStarted();

        final int tasksSize = calculationTasks.size();
        Instant summaryTime = Instant.now().plus(coverageTaskSummaryInterval);
        while (calculationResults.size() < tasksSize) {

            final Duration timeToSummary = Duration.between(Instant.now(), summaryTime);
            if (timeToSummary.isNegative() || timeToSummary.isZero()) {
                logger.info("Coverage calculation task summary (Completed={}, Total={})",
                        calculationResults.size(), tasksSize);

                summaryTime = Instant.now().plus(coverageTaskSummaryInterval);
            } else {
                final Future<CoverageCalculationInfo> resultFuture =
                        completionService.poll(timeToSummary.toNanos(), TimeUnit.NANOSECONDS);
                if (resultFuture != null) {
                    final CoverageCalculationInfo calculationInfo = resultFuture.get();
                    calculationResults.add(calculationInfo);
                    logger.debug("Coverage calculation completed (Analysis Time Interval={})",
                            calculationInfo.results().analysisSegment().timeInterval());
                } // else repeat
            }
        }

        logger.info("Coverage calculation completed (Total segments={}, Aggregate time={})",
                tasksSize, aggregateTime);
        return calculationResults;
    }

    private Set<CoverageCalculationTask> createCalculationTasks(@Nonnull AnalysisTopology analysisTopology) {

        final CloudTopology<TopologyEntityDTO> cloudTierTopology = analysisContext.getCloudTierTopology();
        final MinimalCloudTopology<MinimalEntity> cloudTopology = analysisContext.getSourceCloudTopology();
        final Map<Long, CloudCommitmentData> cloudCommitmentDataMap = analysisTopology.cloudCommitmentsByOid();

        return analysisTopology.segments()
                .stream()
                .map(analysisSegment ->
                        coverageCalculationTaskFactory.newTask(
                                cloudTierTopology,
                                cloudTopology,
                                cloudCommitmentDataMap,
                                analysisSegment))
                .collect(ImmutableSet.toImmutableSet());
    }

    private AnalysisTopology processResults(@Nonnull AnalysisTopology analysisTopology,
                                            @Nonnull List<CoverageCalculationInfo> calculationResults) {

        final List<AnalysisTopologySegment> updatedSegments = Lists.newArrayList();
        for (CoverageCalculationInfo calculationInfo : calculationResults) {

            final CoverageCalculationResult calculationResult = calculationInfo.results();
            final AnalysisTopologySegment originalSegment = calculationResult.analysisSegment();
            final SetMultimap<AggregateCloudTierDemand, CoverageInfo> coverageInfoDemandMap =
                    calculationResult.coverageInfoByDemand();

            final Set<AggregateCloudTierDemand> updatedDemandAggregates =
                    originalSegment.aggregateCloudTierDemandSet()
                            .stream()
                            .map(demandAggregate -> demandAggregate.toBuilder()
                                    .coverageInfo(coverageInfoDemandMap.get(demandAggregate))
                                    .build())
                            .collect(ImmutableSet.toImmutableSet());

            updatedSegments.add(originalSegment.toBuilder()
                    .aggregateCloudTierDemandSet(updatedDemandAggregates)
                    .build());
        }

        return analysisTopology.toBuilder()
                .segments(TimeSeries.newTimeSeries(updatedSegments))
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
     * A factory class for producing {@link CoverageCalculationStage} instances.
     */
    public static class CoverageCalculationFactory implements
            AnalysisStage.StageFactory<AnalysisTopology, AnalysisTopology> {

        private final CoverageCalculationTaskFactory coverageCalculationTaskFactory;

        private final CoverageCalculationSummaryFactory coverageSummaryFactory;

        private final Duration coverageTaskSummaryInterval;

        /**
         * Constructs a new {@link CoverageCalculationFactory} instance.
         * @param coverageCalculationTaskFactory The {@link CoverageCalculationTaskFactory}.
         * @param coverageSummaryFactory The {@link CoverageCalculationSummaryFactory}.
         * @param coverageTaskSummaryInterval The interval used in logging a summary of the calculation
         *                                    tasks during execution.
         */
        public CoverageCalculationFactory(
                @Nonnull CoverageCalculationTaskFactory coverageCalculationTaskFactory,
                @Nonnull CoverageCalculationSummaryFactory coverageSummaryFactory,
                @Nonnull Duration coverageTaskSummaryInterval) {

            this.coverageCalculationTaskFactory = Objects.requireNonNull(coverageCalculationTaskFactory);
            this.coverageSummaryFactory = Objects.requireNonNull(coverageSummaryFactory);
            this.coverageTaskSummaryInterval = Objects.requireNonNull(coverageTaskSummaryInterval);
        }

        /**
         * {@inheritDoc}.
         */
        @Nonnull
        @Override
        public AnalysisStage<AnalysisTopology, AnalysisTopology> createStage(
                final long id,
                @Nonnull final CloudCommitmentAnalysisConfig config,
                @Nonnull final CloudCommitmentAnalysisContext context) {

            return new CoverageCalculationStage(
                    id,
                    config,
                    context,
                    coverageCalculationTaskFactory,
                    coverageSummaryFactory,
                    coverageTaskSummaryInterval);
        }
    }
}
