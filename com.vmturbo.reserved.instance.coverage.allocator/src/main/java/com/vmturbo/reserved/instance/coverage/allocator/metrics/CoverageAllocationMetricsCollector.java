package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

/**
 * Collects metrics for discrete stages of the RI coverage allocator. Generally, the stage is
 * directly measured by a {@link InstrumentedAllocationOperation}, which wraps a metric specific
 * to the operation type.
 */
public class CoverageAllocationMetricsCollector {

    private final CoverageAllocationMetricsProvider metricsProvider;

    private final Map<ServiceProviderInfo, AtomicLong> allocationCountByCSP =
            new ConcurrentHashMap<>();

    private final Map<ServiceProviderCoverageKey, AtomicDouble> allocatedCoverageMap =
            new ConcurrentHashMap<>();

    /**
     * Constructs an instance of {@link CoverageAllocationMetricsCollector}. The lifetime of the
     * collector is intended to be a single RI coverage allocation analysis
     * @param metricsProvider A provider of metrics for specific stages of the coverage analysis. Metrics
     *                        for each stage will only be available, if configured by the caller of the
     *                        coverage library
     */
    public CoverageAllocationMetricsCollector(@Nonnull CoverageAllocationMetricsProvider metricsProvider) {
        this.metricsProvider = Objects.requireNonNull(metricsProvider);
    }

    /**
     * Collects the full analysis duration.
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public CoverageAllocationOperation onCoverageAnalysis() {
        return metricsProvider.totalCoverageAnalysisDuration()
                .map(timerProvider ->
                        new InstrumentedAllocationOperation(timerProvider, this::onAnalysisCompletion))
                .map(CoverageAllocationOperation.class::cast)
                .orElseGet(() -> new InstrumentedAllocationOperation(
                        DataMetricTimerProvider.EMPTY_PROVIDER, this::onAnalysisCompletion));
    }

    /**
     * Collects the duration of the first pass coverage filter.
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public CoverageAllocationOperation onFirstPassCoverageFilter() {
        return metricsProvider.firstPassRIFilterDuration()
                .map(InstrumentedAllocationOperation::new)
                .map(CoverageAllocationOperation.class::cast)
                .orElse(CoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects the duration of the first pass entity filter.
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public CoverageAllocationOperation onFirstPassEntityFilter() {
        return metricsProvider.firstPassEntityFilterDuration()
                .map(InstrumentedAllocationOperation::new)
                .map(CoverageAllocationOperation.class::cast)
                .orElse(CoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects the duration of context creation, which will sort entities and RIs into buckets
     * by CSP.
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public CoverageAllocationOperation onContextCreation() {
        return metricsProvider.contextCreationDuration()
                .map(InstrumentedAllocationOperation::new)
                .map(CoverageAllocationOperation.class::cast)
                .orElse(CoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects the duration of coverage analysis for a specific CSP.
     *
     * @param coverageContext The coverage context, specific to an individual CSP.
     * @param coverageJournal The {@link CloudCommitmentCoverageJournal} for the coverage context,
     *                        used to collect metrics for coverage capacity prior to analysis
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public CoverageAllocationOperation onCoverageAnalysisForCSP(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull CloudCommitmentCoverageJournal coverageJournal) {

        Preconditions.checkNotNull(coverageContext);
        Preconditions.checkNotNull(coverageJournal);

        final ServiceProviderInfo csp = coverageContext.serviceProviderInfo();
        // Record entity + RI counts for the CSP
        metricsProvider.coverableEntityCountForCSP(csp)
                .ifPresent(metric -> metric.observe((double)coverageContext.coverableEntityOids().size()));
        metricsProvider.cloudCommitmentCount(csp)
                .ifPresent(metric -> metric.observe((double)coverageContext.cloudCommitmentOids().size()));
        // Record entity + RI coverage capacity for the CSP,
        // prior to analysis
        /*
        TODO:ejf - make this a percentage and add commitment type

        metricsProvider.uncoveredEntityCapacityForCSP(csp)
                .ifPresent(metric -> metric.observe(
                        coverageContext.coverableEntityOids()
                                .stream()
                                .mapToDouble(coverageJournal::getUncoveredCapacity)
                                .sum()));
        metricsProvider.unallocatedRICapacityForCSP(csp)
                .ifPresent(metric -> metric.observe(
                        coverageContext.reservedInstanceOids()
                                .stream()
                                .mapToDouble(coverageJournal::getUnallocatedCapacity)
                                .sum()));
         */
        // return a timer for the allocation operation
        return metricsProvider.allocationDurationForCSP(csp)
                .map(InstrumentedAllocationOperation::new)
                .map(CoverageAllocationOperation.class::cast)
                .orElse(CoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects metrics on a coverage assignment. THe collected metrics are to track the number of
     * allocations by CSP and the allocated coverage amount by CSP
     *
     * @param coverageEntry The allocated coverage to track
     */
    public void onCoverageAssignment(@Nonnull CoverageJournalEntry coverageEntry) {
        Preconditions.checkNotNull(coverageEntry);

        final ServiceProviderInfo csp = coverageEntry.cloudServiceProvider();
        allocationCountByCSP.computeIfAbsent(csp, __ -> new AtomicLong())
                .incrementAndGet();
        /*
        TODO:ejf add coverage type
        allocatedCoverageByCSP.computeIfAbsent(csp, __ -> new AtomicDouble())
                .addAndGet(coverageEntry.allocatedCoverage());
CloudCommitmentUtils.java
         */
    }

    private void onAnalysisCompletion() {

        allocationCountByCSP.forEach((csp, allocationCount) ->
                metricsProvider.allocationCountForCSP(csp)
                        .ifPresent(metric -> metric.observe(allocationCount.doubleValue())));
        allocatedCoverageMap.forEach((cspCoverageKey, coverageAmount) ->
                metricsProvider.allocatedCoverageAmount(cspCoverageKey.cloudServiceProvider(), cspCoverageKey.coverageTypeInfo())
                        .ifPresent(metric -> metric.observe(coverageAmount.get())));
    }

    /**
     * A tuple of ({@link ServiceProviderInfo}, {@link CloudCommitmentCoverageTypeInfo}).
     */
    @HiddenImmutableTupleImplementation
    @Immutable(prehash = true)
    interface ServiceProviderCoverageKey {

        @Nonnull
        ServiceProviderInfo cloudServiceProvider();

        @Nonnull
        CloudCommitmentCoverageTypeInfo coverageTypeInfo();
    }
}
