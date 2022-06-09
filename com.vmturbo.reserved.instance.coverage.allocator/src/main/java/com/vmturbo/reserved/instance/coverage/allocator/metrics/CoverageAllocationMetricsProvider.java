package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import java.util.Arrays;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricSummary.SummaryData;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

/**
 * Provides metrics to be collected during RI coverage allocation analysis. The metrics collected
 * focus
 * on analysis duration and per-cloud service provider metrics (such as the number of coverage
 * entries/allocations). Metrics specific to a CSP are qualified by the target CSP through a metric
 * label.
 */
public class CoverageAllocationMetricsProvider {

    /**
     * An empty metrics provider.
     */
    public static final CoverageAllocationMetricsProvider EMPTY_PROVIDER =
            CoverageAllocationMetricsProvider.newBuilder().build();

    private static final String CLOUD_SERVICE_PROVIDER_LABEL = "cloud_service_provider";

    private static final String COVERAGE_TYPE_LABEL = "coverage_type";

    private static final String COVERAGE_SUBTYPE_LABEL = "coverage_subtype";

    private final DataMetricSummary totalCoverageAnalysisDuration;

    private final DataMetricSummary firstPassRIFilterDuration;

    private final DataMetricSummary firstPassEntityFilterDuration;

    private final DataMetricSummary contextCreationDuration;

    private final DataMetricSummary allocationDurationByCSP;

    private final DataMetricSummary coverableEntityCountByCSP;

    private final DataMetricSummary cloudCommitmentCount;

    private final DataMetricSummary uncoveredEntityPercentage;

    private final DataMetricSummary unallocatedCommitmentCapacity;

    private final DataMetricSummary allocationCountByCSP;

    private final DataMetricSummary allocatedCoverageAmount;


    private CoverageAllocationMetricsProvider(@Nonnull Builder builder) {
        this.totalCoverageAnalysisDuration = builder.totalCoverageAnalysisDuration;
        this.firstPassRIFilterDuration = builder.firstPassRIFilterDuration;
        this.firstPassEntityFilterDuration = builder.firstPassEntityFilterDuration;
        this.contextCreationDuration = builder.contextCreationDuration;
        this.allocationDurationByCSP = builder.allocationDurationByCSP;
        this.coverableEntityCountByCSP = builder.coverableEntityCountByCSP;
        this.cloudCommitmentCount = builder.cloudCommitmentCount;
        this.uncoveredEntityPercentage = builder.uncoveredEntityPercentage;
        this.unallocatedCommitmentCapacity = builder.unallocatedCommitmentCapacity;
        this.allocationCountByCSP = builder.allocationCountByCSP;
        this.allocatedCoverageAmount = builder.allocatedCoverageAmount;
    }

    /**
     * A {@link DataMetricSummary} metric as a container for timing the total analysis time of
     * coverage allocation analysis.
     *
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> totalCoverageAnalysisDuration() {
        return Optional.ofNullable(totalCoverageAnalysisDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric as a container for timing the first-pass filtering of
     * reserved
     * instances. The first pass filter is provider-agnostic logic for filtering out invalid or
     * fully
     * allocated RIs.
     *
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> firstPassRIFilterDuration() {
        return Optional.ofNullable(firstPassRIFilterDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric as a container for timing the first-pass filtering of
     * entities.
     * The first pass filter is provider-agnostic logic for filtering out entities which are fully
     * covered
     * or have a coverage capacity of zero.
     *
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> firstPassEntityFilterDuration() {
        return Optional.ofNullable(firstPassEntityFilterDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric as a container for timing the creation of contexts for
     * coverage allocation analysis of individual cloud service providers.
     *
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> contextCreationDuration() {
        return Optional.ofNullable(contextCreationDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric to collect the number of coverable entities, indexed by
     * the
     * cloud service provider. Coverable entities will be those entities that pass the first-pass
     * filter.
     *
     * @param csp The target cloud service provider.
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<SummaryData> coverableEntityCountForCSP(@Nonnull ServiceProviderInfo csp) {
        return Optional.ofNullable(coverableEntityCountByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link DataMetricSummary} metric to collect the number of coverable entities, indexed by
     * the
     * cloud service provider. Coverable entities will be those entities that pass the first-pass
     * filter.
     *
     * @param csp The target cloud service provider.
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<SummaryData> cloudCommitmentCount(@Nonnull ServiceProviderInfo csp) {
        return Optional.ofNullable(cloudCommitmentCount)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link DataMetricSummary} metric to collect the uncovered entity coverage, indexed by
     * cloud service provider. The uncovered entity coverage amount will be collected prior to
     * RI coverage analysis.
     *
     * @param csp The target cloud service provider.
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<SummaryData> uncoveredEntityPercentage(@Nonnull ServiceProviderInfo csp) {
        return Optional.ofNullable(uncoveredEntityPercentage)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link DataMetricSummary} metric to collect the unallocated RI coverage, indexed by
     * cloud service provider. The unallocated RI coverage amount will be collected prior to
     * RI coverage analysis.
     *
     * @param csp The target cloud service provider.
     * @param coverageTypeInfo The coverage type info.
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<SummaryData> unallocatedCommitmentCapacity(@Nonnull ServiceProviderInfo csp,
                                                               @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return Optional.ofNullable(unallocatedCommitmentCapacity)
                .map(summaryMetric -> summaryMetric.labels(
                        csp.name(),
                        coverageTypeInfo.getCoverageType().name(),
                        Integer.toString(coverageTypeInfo.getCoverageSubtype())));
    }

    /**
     * A {@link SummaryData} metric as a container for timing the coverage allocation analysis
     * duration per cloud service provider. It should be noted CSP analysis will happen in parallel
     * and therefore the analysis duration for any individual provider is likely to be impacted by
     * concurrent analysis of other CSPs.
     *
     * @param csp The target cloud service provider.
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> allocationDurationForCSP(@Nonnull ServiceProviderInfo csp) {
        return Optional.ofNullable(allocationDurationByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()))
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link SummaryData} metric to collect the number of coverage allocations per
     * cloud service provider. The data is qualified by the CSP through a label.
     *
     * @param csp The target cloud service provider.
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the
     *         summary metric is not present, no data should be collected.
     */
    @Nonnull
    public Optional<SummaryData> allocationCountForCSP(@Nonnull ServiceProviderInfo csp) {
        return Optional.ofNullable(allocationCountByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link SummaryData} metric to collect the coverage amount per CSP as part of coverage
     * analysis. The data is qualified by the CSP through a label.
     *
     * @param csp The target cloud service provider.
     * @param coverageTypeInfo The coverage type info.
     * @return An {@link Optional}, wrapping an instance of {@link SummaryData}.
     */
    @Nonnull
    public Optional<SummaryData> allocatedCoverageAmount(@Nonnull ServiceProviderInfo csp,
                                                         @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return Optional.ofNullable(allocatedCoverageAmount)
                .map(summaryMetric -> summaryMetric.labels(csp.name(),
                        coverageTypeInfo.getCoverageType().name(),
                        Integer.toString(coverageTypeInfo.getCoverageSubtype())));
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     *
     * @return A newly created instance of {@link Builder}.
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates and returns a {@link DataMetricSummary.Builder}, configured with the correct labels
     * for metrics indexed by {@link ServiceProviderInfo}.
     *
     * @return The newly created instance of {@link DataMetricSummary.Builder}.
     */
    @Nonnull
    public static DataMetricSummary.Builder newCloudServiceProviderMetric() {
        return DataMetricSummary.builder()
                .withLabelNames(CLOUD_SERVICE_PROVIDER_LABEL);
    }

    /**
     * Builds a new {@link DataMetricSummary.Builder} instance, with labels defined to accept
     * coverage type info metrics.
     *
     * @return The configured {@link DataMetricSummary.Builder}.
     */
    public static DataMetricSummary.Builder newCoverageTypeMetric() {
        return DataMetricSummary.builder()
                .withLabelNames(CLOUD_SERVICE_PROVIDER_LABEL, COVERAGE_TYPE_LABEL, COVERAGE_SUBTYPE_LABEL);
    }

    /**
     * A builder class for {@link CoverageAllocationMetricsProvider}.
     */
    public static class Builder {

        private DataMetricSummary totalCoverageAnalysisDuration;
        private DataMetricSummary firstPassRIFilterDuration;
        private DataMetricSummary firstPassEntityFilterDuration;
        private DataMetricSummary contextCreationDuration;
        private DataMetricSummary allocationDurationByCSP;
        private DataMetricSummary coverableEntityCountByCSP;
        private DataMetricSummary cloudCommitmentCount;
        private DataMetricSummary uncoveredEntityPercentage;
        private DataMetricSummary unallocatedCommitmentCapacity;
        private DataMetricSummary allocationCountByCSP;
        private DataMetricSummary allocatedCoverageAmount;

        /**
         * Set the total coverage analysis summary (timer).
         *
         * @param totalCoverageAnalysisDuration The {@link DataMetricSummary} used as a
         *         timer for                                     analysis duration.
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#totalCoverageAnalysisDuration()
         *         CoverageAllocationMetricsProvider#totalCoverageAnalysisDuration()().
         */
        @Nonnull
        public Builder totalCoverageAnalysisDuration(@Nullable DataMetricSummary totalCoverageAnalysisDuration) {
            this.totalCoverageAnalysisDuration = totalCoverageAnalysisDuration;
            return this;
        }

        /**
         * Set the first past RI filter summary (timer).
         *
         * @param firstPassRIFilterDuration The {@link DataMetricSummary} used as a timer
         *         for                                  RI filtering.
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#firstPassRIFilterDuration()
         *         CoverageAllocationMetricsProvider#firstPassRIFilterDuration()().
         */
        @Nonnull
        public Builder firstPassRIFilterDuration(@Nullable DataMetricSummary firstPassRIFilterDuration) {
            this.firstPassRIFilterDuration = firstPassRIFilterDuration;
            return this;
        }

        /**
         * Set the first past entity filter summary (timer).
         *
         * @param firstPassEntityFilterDuration The {@link DataMetricSummary} used as a
         *         timer for                                      entity filtering.
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#firstPassEntityFilterDuration()
         *         CoverageAllocationMetricsProvider#firstPassEntityFilterDuration()().
         */
        @Nonnull
        public Builder firstPassEntityFilterDuration(@Nullable DataMetricSummary firstPassEntityFilterDuration) {
            this.firstPassEntityFilterDuration = firstPassEntityFilterDuration;
            return this;
        }

        /**
         * Set the context creation summary (timer).
         *
         * @param contextCreationDuration The {@link DataMetricSummary} used as a timer for
         *         per CSP                                context creation.
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#contextCreationDuration()
         *         CoverageAllocationMetricsProvider#contextCreationDuration()().
         */
        @Nonnull
        public Builder contextCreationDuration(@Nullable DataMetricSummary contextCreationDuration) {
            this.contextCreationDuration = contextCreationDuration;
            return this;
        }

        /**
         * Set the allocation duration by CSP summary (timer).
         *
         * @param summaryMetric The summary metric (timer).
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#allocationDurationForCSP(ServiceProviderInfo)
         *         CoverageAllocationMetricsProvider#allocationDurationForCSP(ServiceProviderInfo)
         */
        @Nonnull
        public Builder allocationDurationByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL);

            this.allocationDurationByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the number of coverable entities per CSP summary.
         *
         * @param summaryMetric The summary metric to collect the number of coverable
         *         entities.
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#coverableEntityCountForCSP(ServiceProviderInfo)
         *         CoverageAllocationMetricsProvider#coverableEntityCountForCSP(ServiceProviderInfo)
         */
        @Nonnull
        public Builder coverableEntityCountByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL);

            this.coverableEntityCountByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the uncovered entity capacity by CSP summary.
         *
         * @param summaryMetric The summary metric to collect the sum of uncovered entity
         *         capacity.
         * @return This {@link Builder} instance for method chaining.
         * @see CoverageAllocationMetricsProvider#uncoveredEntityPercentage(ServiceProviderInfo)
         *         CoverageAllocationMetricsProvider#uncoveredEntityPercentage(ServiceProviderInfo)
         */
        @Nonnull
        public Builder uncoveredEntityPercentage(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL);

            this.uncoveredEntityPercentage = summaryMetric;
            return this;
        }

        /**
         * Set the unallocated RI capacity by CSP summary.
         *
         * @param summaryMetric THe summary metric to collect the sum of unallocatoed RI
         *         capacity.
         * @return This {@link Builder} instance for method chaining.
         */
        @Nonnull
        public Builder unallocatedCommitmentCapacity(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL, COVERAGE_TYPE_LABEL, COVERAGE_SUBTYPE_LABEL);

            this.unallocatedCommitmentCapacity = summaryMetric;
            return this;
        }

        /**
         * Set the number of cloud commitments per CSP summary. This count will represent only
         * RIs that pass the first pass RI filter.
         *
         * @param summaryMetric The summary metric to collect the number of RIs
         * @return This {@link Builder} instance for method chaining
         * @see CoverageAllocationMetricsProvider#cloudCommitmentCount(ServiceProviderInfo)
         *         CoverageAllocationMetricsProvider#cloudCommitmentCount(ServiceProviderInfo)
         */
        @Nonnull
        public Builder cloudCommitmentCount(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL);

            this.cloudCommitmentCount = summaryMetric;
            return this;
        }

        /**
         * Set the number of coverage allocations per CSP.
         *
         * @param summaryMetric The summary metric to collect the number of coverage
         *         allocations
         * @return This {@link Builder} instance for method chaining
         * @see CoverageAllocationMetricsProvider#allocationCountForCSP(ServiceProviderInfo)
         *         CoverageAllocationMetricsProvider#allocationCountForCSP(ServiceProviderInfo)
         */
        @Nonnull
        public Builder allocationCountByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL);

            this.allocationCountByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the coverage amount allocated per CSP.
         *
         * @param summaryMetric The summary metric to collect the number of coverage
         *         allocations
         * @return This {@link Builder} instance for method chaining
         * @see CoverageAllocationMetricsProvider#allocatedCoverageAmount(ServiceProviderInfo,
         *         CloudCommitmentCoverageTypeInfo) CoverageAllocationMetricsProvider#allocatedCoverageAmount(ServiceProviderInfo,
         *         CloudCommitmentCoverageTypeInfo)
         */
        @Nonnull
        public Builder allocatedCoverageAmount(@Nonnull DataMetricSummary summaryMetric) {
            validateMetricLabels(summaryMetric, CLOUD_SERVICE_PROVIDER_LABEL, COVERAGE_TYPE_LABEL, COVERAGE_SUBTYPE_LABEL);

            this.allocatedCoverageAmount = summaryMetric;
            return this;
        }

        /**
         * Builds a new {@link CoverageAllocationMetricsProvider} instance.
         *
         * @return A newly built instance of {@link CoverageAllocationMetricsProvider}, based on the
         *         configuration of this builder
         */
        @Nonnull
        public CoverageAllocationMetricsProvider build() {
            return new CoverageAllocationMetricsProvider(this);
        }

        private void validateMetricLabels(@Nonnull DataMetricSummary summaryMetric, String... labelNames) {

            Preconditions.checkNotNull(summaryMetric);

            if (summaryMetric.getLabelNames().length != labelNames.length
                    || !Arrays.equals(summaryMetric.getLabelNames(), labelNames)) {
                throw new IllegalArgumentException(
                        String.format("CSP-based metric must have `%s` label", labelNames));
            }
        }
    }
}
