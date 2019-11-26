package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;

/**
 * Collects metrics for discrete stages of the RI coverage allocator. Generally, the stage is
 * directly measured by a {@link InstrumentedAllocationOperation}, which wraps a metric specific
 * to the operation type.
 */
public class RICoverageAllocationMetricsCollector {

    private final RICoverageAllocationMetricsProvider metricsProvider;

    private final Map<CloudServiceProvider, AtomicLong> allocationCountByCSP =
            new ConcurrentHashMap<>();

    private final Map<CloudServiceProvider, AtomicDouble> allocatedCoverageByCSP =
            new ConcurrentHashMap<>();

    /**
     * Constructs an instance of {@link RICoverageAllocationMetricsCollector}. The lifetime of the
     * collector is intended to be a single RI coverage allocation analysis
     * @param metricsProvider A provider of metrics for specific stages of the coverage analysis. Metrics
     *                        for each stage will only be available, if configured by the caller of the
     *                        coverage library
     */
    public RICoverageAllocationMetricsCollector(@Nonnull RICoverageAllocationMetricsProvider metricsProvider) {
        this.metricsProvider = Objects.requireNonNull(metricsProvider);
    }

    /**
     * Collects the full analysis duration.
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public RICoverageAllocationOperation onCoverageAnalysis() {
        return metricsProvider.totalCoverageAnalysisDuration()
                .map(timerProvider ->
                        new InstrumentedAllocationOperation(timerProvider, this::onAnalysisCompletion))
                .map(RICoverageAllocationOperation.class::cast)
                .orElseGet(() -> new InstrumentedAllocationOperation(
                        DataMetricTimerProvider.EMPTY_PROVIDER, this::onAnalysisCompletion));
    }

    /**
     * Collects the duration of the first pass RI filter
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public RICoverageAllocationOperation onFirstPassRIFilter() {
        return metricsProvider.firstPassRIFilterDuration()
                .map(InstrumentedAllocationOperation::new)
                .map(RICoverageAllocationOperation.class::cast)
                .orElse(RICoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects the duration of the first pass entity filter
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public RICoverageAllocationOperation onFirstPassEntityFilter() {
        return metricsProvider.firstPassEntityFilterDuration()
                .map(InstrumentedAllocationOperation::new)
                .map(RICoverageAllocationOperation.class::cast)
                .orElse(RICoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects the duration of context creation, which will sort entities and RIs into buckets
     * by CSP.
     *
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public RICoverageAllocationOperation onContextCreation() {
        return metricsProvider.contextCreationDuration()
                .map(InstrumentedAllocationOperation::new)
                .map(RICoverageAllocationOperation.class::cast)
                .orElse(RICoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects the duration of coverage analysis for a specific CSP
     *
     * @param coverageContext The coverage context, specific to an individual CSP.
     * @param coverageJournal The {@link ReservedInstanceCoverageJournal} for the coverage context,
     *                        used to collect metrics for coverage capacity prior to analysis
     * @return An {@link InstrumentedAllocationOperation}.
     */
    @Nonnull
    public RICoverageAllocationOperation onCoverageAnalysisForCSP(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal) {

        Preconditions.checkNotNull(coverageContext);
        Preconditions.checkNotNull(coverageJournal);

        final CloudServiceProvider csp = coverageContext.cloudServiceProvider();
        // Record entity + RI counts for the CSP
        metricsProvider.coverableEntityCountForCSP(csp)
                .ifPresent(metric -> metric.observe((double)coverageContext.coverableEntityOids().size()));
        metricsProvider.reservedInstanceCountForCSP(csp)
                .ifPresent(metric -> metric.observe((double)coverageContext.reservedInstanceOids().size()));
        // Record entity + RI coverage capacity for the CSP,
        // prior to analysis
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

        // return a timer for the allocation operation
        return metricsProvider.allocationDurationForCSP(csp)
                .map(InstrumentedAllocationOperation::new)
                .map(RICoverageAllocationOperation.class::cast)
                .orElse(RICoverageAllocationOperation.PASS_THROUGH_OPERATION);
    }

    /**
     * Collects metrics on a coverage assignment. THe collected metrics are to track the number of
     * allocations by CSP and the allocated coverage amount by CSP
     *
     * @param coverageEntry The allocated coverage to track
     */
    public void onCoverageAssignment(@Nonnull CoverageJournalEntry coverageEntry) {
        Preconditions.checkNotNull(coverageEntry);

        final CloudServiceProvider csp = coverageEntry.cloudServiceProvider();
        allocationCountByCSP.computeIfAbsent(csp, __ -> new AtomicLong())
                .incrementAndGet();
        allocatedCoverageByCSP.computeIfAbsent(csp, __ -> new AtomicDouble())
                .addAndGet(coverageEntry.allocatedCoverage());
    }

    private void onAnalysisCompletion() {

        allocationCountByCSP.forEach((csp, allocationCount) ->
                metricsProvider.allocationCountForCSP(csp)
                        .ifPresent(metric -> metric.observe(allocationCount.doubleValue())));
        allocatedCoverageByCSP.forEach((csp, coverageAmount) ->
                metricsProvider.allocatedCoverageAmountForCSP(csp)
                        .ifPresent(metric -> metric.observe(coverageAmount.get())));
    }

}
