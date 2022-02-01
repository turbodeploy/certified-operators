package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopologySegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AggregateDemandPreference.AggregateDemandPreferenceFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AnalysisCoverageTopology.AnalysisCoverageTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.CoverageInfo;
import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocationConfig;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;

/**
 * Responsible for running coverage allocation analysis on a single {@link AnalysisTopologySegment}.
 */
public class CoverageCalculationTask implements Callable<CoverageCalculationInfo> {

    private final Logger logger = LogManager.getLogger();

    private final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory;

    private final AnalysisCoverageTopologyFactory coverageTopologyFactory;

    private final CoverageAllocatorFactory coverageAllocatorFactory;

    private final AggregateDemandPreferenceFactory aggregateDemandPreferenceFactory;

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology;

    private final Map<Long, CloudCommitmentData> cloudCommitmentDataMap;

    private final AnalysisTopologySegment analysisSegment;

    private CoverageCalculationTask(@Nonnull CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory,
                                    @Nonnull AnalysisCoverageTopologyFactory coverageTopologyFactory,
                                    @Nonnull CoverageAllocatorFactory coverageAllocatorFactory,
                                    @Nonnull AggregateDemandPreferenceFactory aggregateDemandPreferenceFactory,
                                    @Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                    @Nonnull Map<Long, CloudCommitmentData> cloudCommitmentDataMap,
                                    @Nonnull AnalysisTopologySegment analysisSegment) {

        this.cloudCommitmentAggregatorFactory = Objects.requireNonNull(cloudCommitmentAggregatorFactory);
        this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
        this.coverageAllocatorFactory = Objects.requireNonNull(coverageAllocatorFactory);
        this.aggregateDemandPreferenceFactory = Objects.requireNonNull(aggregateDemandPreferenceFactory);
        this.cloudTierTopology = Objects.requireNonNull(cloudTierTopology);
        this.cloudCommitmentDataMap = ImmutableMap.copyOf(Objects.requireNonNull(cloudCommitmentDataMap));
        this.analysisSegment = Objects.requireNonNull(analysisSegment);
    }

    /**
     * Runs the coverage allocation analysis on the {@link AnalysisTopologySegment}, determining
     * assignments of cloud commitments referenced within the segment to demand.
     * @return The {@link CoverageCalculationInfo}, containing the coverage assignments and a summary
     * of the data.
     * @throws Exception Any exception thrown by the coverage analysis.
     */
    @Override
    public CoverageCalculationInfo call() throws Exception {

        logger.debug("Running coverage analysis for segment (Interval={}, Demand Count={}, Commitments={})",
                analysisSegment.timeInterval(), analysisSegment.aggregateCloudTierDemandSet().size(),
                analysisSegment.cloudCommitmentByOid().size());

        final Stopwatch analysisTimer = Stopwatch.createStarted();
        final CoverageCalculationResult calculationResults = runCoverageAnalysis();

        final CoverageCalculationInfo calculationInfo = CoverageCalculationInfo.builder()
                .results(calculationResults)
                .summary(createSummary(calculationResults, analysisTimer.elapsed()))
                .build();

        logger.debug("Coverage analysis finished (Interval={}, Summary={})",
                analysisSegment.timeInterval(), calculationInfo.summary());
        return calculationInfo;
    }

    private CoverageCalculationResult runCoverageAnalysis() throws Exception {

        final Set<CloudCommitmentAggregate> commitmentAggregates =
                createCommitmentAggregatesForSegment();
        final Map<Long, CloudCommitmentAmount> commitmentCapacityById =
                calculateCommitmentAggregateCapacity(commitmentAggregates, analysisSegment);

        final AnalysisCoverageTopology coverageTopology = coverageTopologyFactory.newTopology(
                cloudTierTopology,
                analysisSegment.aggregateCloudTierDemandSet().values(),
                commitmentAggregates,
                commitmentCapacityById);

        final CoverageAllocationConfig allocationConfig = CoverageAllocationConfig.builder()
                .concurrentProcessing(false)
                .validateCoverages(false)
                .coverageTopology(coverageTopology)
                // Add custom demand preference logic to prioritize recommendation demand over demand
                // only used for coverage calculations.
                .coverageEntityPreference(aggregateDemandPreferenceFactory.newPreference(coverageTopology))
                .build();

        final CloudCommitmentCoverageAllocator coverageAllocator =
                coverageAllocatorFactory.createAllocator(allocationConfig);

        final CloudCommitmentCoverageAllocation coverageAllocation = coverageAllocator.allocateCoverage();

        return processResults(coverageTopology, coverageAllocation);
    }

    private Set<CloudCommitmentAggregate> createCommitmentAggregatesForSegment() throws AggregationFailureException {

        final CloudCommitmentAggregator commitmentAggregator =
                cloudCommitmentAggregatorFactory.newAggregator(cloudTierTopology);

        final Set<Long> commitmentIdsInScope = analysisSegment.cloudCommitmentByOid().keySet();
        final Set<CloudCommitmentData> commitmentsInScope = cloudCommitmentDataMap.entrySet()
                .stream()
                .filter(commitmentEntry -> commitmentIdsInScope.contains(commitmentEntry.getKey()))
                .map(Map.Entry::getValue)
                .collect(ImmutableSet.toImmutableSet());

        for (CloudCommitmentData commitmentData : commitmentsInScope) {
            commitmentAggregator.collectCommitment(commitmentData);
        }

        return commitmentAggregator.getAggregates();
    }

    private Map<Long, CloudCommitmentAmount> calculateCommitmentAggregateCapacity(
            @Nonnull Set<CloudCommitmentAggregate> commitmentAggregateSet,
            @Nonnull AnalysisTopologySegment analysisSegment) {

        final Map<Long, CloudCommitmentAmount> capacityByCommitmentId = analysisSegment.cloudCommitmentByOid();
        return commitmentAggregateSet.stream()
                .collect(ImmutableMap.toImmutableMap(
                        CloudCommitmentAggregate::aggregateId,
                        (aggregate) -> aggregate.commitments()
                                .stream()
                                .map(commitmentData -> capacityByCommitmentId.getOrDefault(
                                        commitmentData.commitmentId(),
                                        CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT))
                                .reduce(CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT,
                                        CommitmentAmountCalculator::sum)));

    }

    @Nonnull
    private CoverageCalculationResult processResults(@Nonnull AnalysisCoverageTopology coverageTopology,
                                                     @Nonnull CloudCommitmentCoverageAllocation coverageAllocation) {

        final Map<Long, AggregateCloudTierDemand> aggregateDemandById =
                coverageTopology.getAggregatedDemandById();
        final Map<Long, CloudCommitmentAggregate> commitmentAggregatesById =
                coverageTopology.getCommitmentAggregatesById();

        final SetMultimap<AggregateCloudTierDemand, CoverageInfo> coverageInfoDemandMap =
                coverageAllocation.totalCoverageTable().cellSet()
                        .stream()
                        // Need to convert the coverage amount (in coupons) to the aggregate demand
                        // unit (generally hours). In the future, the allocators coverage table
                        // should be in a percentage.
                        .map(allocation -> coverageTopology.convertAllocationDemandToAggregate(
                                allocation.getRowKey(),
                                allocation.getColumnKey(),
                                allocation.getValue()))
                        .filter(Objects::nonNull)
                        .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                                (allocation) -> aggregateDemandById.get(allocation.getLeft()),
                                (allocation) -> CoverageInfo.builder()
                                        .cloudCommitmentAggregate(
                                                commitmentAggregatesById.get(allocation.getMiddle()))
                                        .coverageAmount(allocation.getRight())
                                        .build()));

        return CoverageCalculationResult.builder()
                .analysisSegment(analysisSegment)
                .coverageInfoByDemand(coverageInfoDemandMap)
                .build();
    }

    private CoverageCalculationSummary createSummary(@Nonnull CoverageCalculationResult calculationResults,
                                                     @Nonnull Duration analysisDuration) {

        final double totalCoverage = calculationResults.coverageInfoByDemand().values()
                .stream()
                .mapToDouble(CoverageInfo::coverageAmount)
                .sum();
        final double totalDemand = analysisSegment.aggregateCloudTierDemandSet()
                .values()
                .stream()
                .mapToDouble(AggregateCloudTierDemand::demandAmount)
                .sum();

        final double totalCapacity = analysisSegment.cloudCommitmentByOid().values()
                .stream()
                .mapToDouble(CloudCommitmentAmount::getCoupons)
                .sum();

        return CoverageCalculationSummary.builder()
                .aggregateDemand(totalDemand)
                .coveredDemand(totalCoverage)
                .calculationDuration(analysisDuration)
                .build();

    }

    /**
     * The results of the coverage analysis.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface CoverageCalculationResult {

        /**
         * The {@link AnalysisTopologySegment} analyzed.
         * @return The {@link AnalysisTopologySegment} analyzed.
         */
        @Nonnull
        AnalysisTopologySegment analysisSegment();

        /**
         * A map of the {@link AggregateCloudTierDemand} instances from {@link #analysisSegment()} to
         * the set of {@link CoverageInfo} instances, representing coverage assignments from
         * {@link CloudCommitmentAggregate} instances.
         * @return An immutable {@link SetMultimap} of the aggregate demand to the set of
         * coverage assignments.
         */
        @Nonnull
        SetMultimap<AggregateCloudTierDemand, CoverageInfo> coverageInfoByDemand();

        /**
         * Creates and returns a new {@link Builder} instance.
         * @return A newly created builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link CoverageCalculationResult} instances.
         */
        class Builder extends ImmutableCoverageCalculationResult.Builder {}
    }

    /**
     * A summary of {@link CoverageCalculationResult}.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface CoverageCalculationSummary {

        /**
         * The duration of the coverage allocation analysis.
         * @return The duration of the coverage allocation analysis.
         */
        @Nonnull
        Duration calculationDuration();

        /**
         * The amount of covered demand. The amount will be in hours of demand.
         * @return The amount of covered demand.
         */
        double coveredDemand();

        /**
         * The amount of total aggregate demand.
         * @return The amount of total aggregate demand.
         */
        double aggregateDemand();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link CoverageCalculationSummary} instances.
         */
        class Builder extends ImmutableCoverageCalculationSummary.Builder {}
    }

    /**
     * A container class for {@link CoverageCalculationResult} and {@link CoverageCalculationSummary},
     * representing the output of a {@link CoverageCalculationTask} instance.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface CoverageCalculationInfo {

        /**
         * The calculation results.
         * @return The calculation results.
         */
        @Nonnull
        CoverageCalculationResult results();

        /**
         * The calculation summary.
         * @return The calculation summary.
         */
        @Nonnull
        CoverageCalculationSummary summary();

        /**
         * Constructs and return a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link CoverageCalculationInfo} instances.
         */
        class Builder extends ImmutableCoverageCalculationInfo.Builder {}
    }

    /**
     * A factory class for producing {@link CoverageCalculationTask} instances.
     */
    public static class CoverageCalculationTaskFactory {

        private final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory;

        private final AnalysisCoverageTopologyFactory coverageTopologyFactory;

        private final CoverageAllocatorFactory coverageAllocatorFactory;

        private final AggregateDemandPreferenceFactory aggregateDemandPreferenceFactory;

        /**
         * Constructs a new factory instance.
         * @param cloudCommitmentAggregatorFactory The {@link CloudCommitmentAggregatorFactory}.
         * @param coverageTopologyFactory The {@link AnalysisCoverageTopologyFactory}.
         * @param coverageAllocatorFactory The {@link CoverageAllocatorFactory}.
         * @param aggregateDemandPreferenceFactory The {@link AggregateDemandPreferenceFactory}.
         */
        public CoverageCalculationTaskFactory(
                @Nonnull CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory,
                @Nonnull AnalysisCoverageTopologyFactory coverageTopologyFactory,
                @Nonnull CoverageAllocatorFactory coverageAllocatorFactory,
                @Nonnull AggregateDemandPreferenceFactory aggregateDemandPreferenceFactory) {

            this.cloudCommitmentAggregatorFactory = Objects.requireNonNull(cloudCommitmentAggregatorFactory);
            this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
            this.coverageAllocatorFactory = Objects.requireNonNull(coverageAllocatorFactory);
            this.aggregateDemandPreferenceFactory = Objects.requireNonNull(aggregateDemandPreferenceFactory);
        }

        /**
         * Creates a new {@link CoverageCalculationTask}.
         * @param cloudTierTopology The cloud topology containing cloud tier entities.
         * @param cloudCommitmentDataMap The map of cloud commitments, indexed by commitment OID.
         * @param analysisSegment The analysis segment for this coverage calculation task.
         * @return The {@link CoverageCalculationTask}.
         */
        @Nonnull
        public CoverageCalculationTask newTask(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                               @Nonnull Map<Long, CloudCommitmentData> cloudCommitmentDataMap,
                                               @Nonnull AnalysisTopologySegment analysisSegment) {

            return new CoverageCalculationTask(
                    cloudCommitmentAggregatorFactory,
                    coverageTopologyFactory,
                    coverageAllocatorFactory,
                    aggregateDemandPreferenceFactory,
                    cloudTierTopology,
                    cloudCommitmentDataMap,
                    analysisSegment);
        }
    }

}
