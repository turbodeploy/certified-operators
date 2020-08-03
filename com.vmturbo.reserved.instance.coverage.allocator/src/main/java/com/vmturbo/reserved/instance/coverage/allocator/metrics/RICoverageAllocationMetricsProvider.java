package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricSummary.SummaryData;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;

/**
 * Provides metrics to be collected during RI coverage allocation analysis. The metrics collected focus
 * on analysis duration and per-cloud service provider metrics (such as the number of coverage
 * entries/allocations). Metrics specific to a CSP are qualified by the target CSP through a metric
 * label.
 */
public class RICoverageAllocationMetricsProvider {

    /**
     * An empty metrics provider
     */
    public static final RICoverageAllocationMetricsProvider EMPTY_PROVIDER =
            RICoverageAllocationMetricsProvider.newBuilder().build();

    private static final String CLOUD_SERVICE_PROVIDER_LABEL = "cloud_service_provider";

    private final DataMetricSummary totalCoverageAnalysisDuration;

    private final DataMetricSummary firstPassRIFilterDuration;

    private final DataMetricSummary firstPassEntityFilterDuration;

    private final DataMetricSummary contextCreationDuration;

    private final DataMetricSummary allocationDurationByCSP;

    private final DataMetricSummary coverableEntityCountByCSP;

    private final DataMetricSummary reservedInstanceCountByCSP;

    private final DataMetricSummary uncoveredEntityCapacityByCSP;

    private final DataMetricSummary unallocatedRICapacityByCSP;

    private final DataMetricSummary allocationCountByCSP;

    private final DataMetricSummary allocatedCoverageAmountByCSP;


    private RICoverageAllocationMetricsProvider(@Nonnull Builder builder) {
        this.totalCoverageAnalysisDuration = builder.totalCoverageAnalysisDuration;
        this.firstPassRIFilterDuration = builder.firstPassRIFilterDuration;
        this.firstPassEntityFilterDuration = builder.firstPassEntityFilterDuration;
        this.contextCreationDuration = builder.contextCreationDuration;
        this.allocationDurationByCSP = builder.allocationDurationByCSP;
        this.coverableEntityCountByCSP = builder.coverableEntityCountByCSP;
        this.reservedInstanceCountByCSP = builder.reservedInstanceCountByCSP;
        this.uncoveredEntityCapacityByCSP = builder.uncoveredEntityCapacityByCSP;
        this.unallocatedRICapacityByCSP = builder.unallocatedRICapacityByCSP;
        this.allocationCountByCSP = builder.allocationCountByCSP;
        this.allocatedCoverageAmountByCSP = builder.allocatedCoverageAmountByCSP;
    }


    /**
     * A {@link DataMetricSummary} metric as a container for timing the total analysis time of
     * coverage allocation analysis
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> totalCoverageAnalysisDuration() {
        return Optional.ofNullable(totalCoverageAnalysisDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric as a container for timing the first-pass filtering of reserved
     * instances. The first pass filter is provider-agnostic logic for filtering out invalid or fully
     * allocated RIs.
     *
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> firstPassRIFilterDuration() {
        return Optional.ofNullable(firstPassRIFilterDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric as a container for timing the first-pass filtering of entities.
     * The first pass filter is provider-agnostic logic for filtering out entities which are fully covered
     * or have a coverage capacity of zero.
     *
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
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
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> contextCreationDuration() {
        return Optional.ofNullable(contextCreationDuration)
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link DataMetricSummary} metric to collect the number of coverable entities, indexed by the
     * cloud service provider. Coverable entities will be those entities that pass the first-pass filter.
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<SummaryData> coverableEntityCountForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(coverableEntityCountByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link DataMetricSummary} metric to collect the number of coverable entities, indexed by the
     * cloud service provider. Coverable entities will be those entities that pass the first-pass filter.
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<SummaryData> reservedInstanceCountForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(reservedInstanceCountByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link DataMetricSummary} metric to collect the uncovered entity coverage, indexed by
     * cloud service provider. The uncovered entity coverage amount will be collected prior to
     * RI coverage analysis.
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<SummaryData> uncoveredEntityCapacityForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(uncoveredEntityCapacityByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link DataMetricSummary} metric to collect the unallocated RI coverage, indexed by
     * cloud service provider. The unallocated RI coverage amount will be collected prior to
     * RI coverage analysis.
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<SummaryData> unallocatedRICapacityForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(unallocatedRICapacityByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link SummaryData} metric as a container for timing the coverage allocation analysis
     * duration per cloud service provider. It should be noted CSP analysis will happen in parallel
     * and therefore the analysis duration for any individual provider is likely to be impacted by
     * concurrent analysis of other CSPs.
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<DataMetricTimerProvider> allocationDurationForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(allocationDurationByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()))
                .map(DataMetricTimerProvider::createProvider);
    }

    /**
     * A {@link SummaryData} metric to collect the number of coverage allocations per
     * cloud service provider. The data is qualified by the CSP through a label
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link DataMetricSummary}. If the summary
     * metric is not present, no data should be collected
     */
    @Nonnull
    public Optional<SummaryData> allocationCountForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(allocationCountByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * A {@link SummaryData} metric to collect the coverage amount per CSP as part of coverage
     * analysis. The data is qualified by the CSP through a label
     *
     * @param csp The target cloud service provider
     * @return An {@link Optional}, wrapping an instance of {@link SummaryData}. If a
     */
    @Nonnull
    public Optional<SummaryData> allocatedCoverageAmountForCSP(@Nonnull CloudServiceProvider csp) {
        return Optional.ofNullable(allocatedCoverageAmountByCSP)
                .map(summaryMetric -> summaryMetric.labels(csp.name()));
    }

    /**
     * @return A newly created instance of {@link Builder}
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates and returns a {@link DataMetricSummary.Builder}, configured with the correct labels
     * for metrics indexed by {@link CloudServiceProvider}
     * @return The newly created instance of {@link DataMetricSummary.Builder}
     */
    @Nonnull
    public static DataMetricSummary.Builder newCloudServiceProviderMetric() {
        return DataMetricSummary.builder()
                .withLabelNames(CLOUD_SERVICE_PROVIDER_LABEL);
    }

    /**
     * A builder classs for {@link RICoverageAllocationMetricsProvider}
     */
    public static class Builder {

        private DataMetricSummary totalCoverageAnalysisDuration;
        private DataMetricSummary firstPassRIFilterDuration;
        private DataMetricSummary firstPassEntityFilterDuration;
        private DataMetricSummary contextCreationDuration;
        private DataMetricSummary allocationDurationByCSP;
        private DataMetricSummary coverableEntityCountByCSP;
        private DataMetricSummary reservedInstanceCountByCSP;
        private DataMetricSummary uncoveredEntityCapacityByCSP;
        private DataMetricSummary unallocatedRICapacityByCSP;
        private DataMetricSummary allocationCountByCSP;
        private DataMetricSummary allocatedCoverageAmountByCSP;

        /**
         * Set the total coverage analysis summary (timer)
         * @see RICoverageAllocationMetricsProvider#totalCoverageAnalysisDuration() ()
         * @param totalCoverageAnalysisDuration The {@link DataMetricSummary} used as a timer for
         *                                     analysis duration
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder totalCoverageAnalysisDuration(@Nullable DataMetricSummary totalCoverageAnalysisDuration) {
            this.totalCoverageAnalysisDuration = totalCoverageAnalysisDuration;
            return this;
        }

        /**
         * Set the first past RI filter summary (timer)
         * @see RICoverageAllocationMetricsProvider#firstPassRIFilterDuration() ()
         * @param firstPassRIFilterDuration The {@link DataMetricSummary} used as a timer for
         *                                  RI filtering
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder firstPassRIFilterDuration(@Nullable DataMetricSummary firstPassRIFilterDuration) {
            this.firstPassRIFilterDuration = firstPassRIFilterDuration;
            return this;
        }

        /**
         * Set the first past entity filter summary (timer)
         * @see RICoverageAllocationMetricsProvider#firstPassEntityFilterDuration() ()
         * @param firstPassEntityFilterDuration The {@link DataMetricSummary} used as a timer for
         *                                      entity filtering
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder firstPassEntityFilterDuration(@Nullable DataMetricSummary firstPassEntityFilterDuration) {
            this.firstPassEntityFilterDuration = firstPassEntityFilterDuration;
            return this;
        }

        /**
         * Set the context creation summary (timer)
         * @see RICoverageAllocationMetricsProvider#contextCreationDuration() ()
         * @param contextCreationDuration The {@link DataMetricSummary} used as a timer for per CSP
         *                                context creation
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder contextCreationDuration(@Nullable DataMetricSummary contextCreationDuration) {
            this.contextCreationDuration = contextCreationDuration;
            return this;
        }

        /**
         * Set the allocation duration by CSP summary (timer)
         * @see RICoverageAllocationMetricsProvider#allocationDurationForCSP(CloudServiceProvider)
         * @param summaryMetric The summary metric (timer)
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder allocationDurationByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.allocationDurationByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the number of coverable entities per CSP summary
         * @see RICoverageAllocationMetricsProvider#coverableEntityCountForCSP(CloudServiceProvider)
         * @param summaryMetric The summary metric to collect the number of coverable entities
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder coverableEntityCountByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.coverableEntityCountByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the uncovered entity capacity by CSP summary.
         * @see RICoverageAllocationMetricsProvider#uncoveredEntityCapacityForCSP(CloudServiceProvider)
         * @param summaryMetric The summary metric to collect the sum of uncovered entity capacity
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder uncoveredEntityCapacityByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.uncoveredEntityCapacityByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the unallocated RI capacity by CSP summary.
         * @param summaryMetric THe summary metric to collect the sum of unallocatoed RI capacity
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder unallocatedRICapacityByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.unallocatedRICapacityByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the number of reserved instances per CSP summary. This count will represent only
         * RIs that pass the first pass RI filter.
         * @see RICoverageAllocationMetricsProvider#reservedInstanceCountForCSP(CloudServiceProvider)
         * @param summaryMetric The summary metric to collect the number of RIs
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder reservedInstanceCountByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.reservedInstanceCountByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the number of coverage allocations per CSP
         * @see RICoverageAllocationMetricsProvider#allocationCountForCSP(CloudServiceProvider)
         * @param summaryMetric The summary metric to collect the number of coverage allocations
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder allocationCountByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.allocationCountByCSP = summaryMetric;
            return this;
        }

        /**
         * Set the coverage amount allocated per CSP
         * @see RICoverageAllocationMetricsProvider#allocatedCoverageAmountForCSP(CloudServiceProvider)
         * @param summaryMetric The summary metric to collect the number of coverage allocations
         * @return This {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder allocatedCoverageAmountByCSP(@Nonnull DataMetricSummary summaryMetric) {
            validateCSPMetric(summaryMetric);

            this.allocatedCoverageAmountByCSP = summaryMetric;
            return this;
        }

        /**
         * @return A newly built instance of {@link RICoverageAllocationMetricsProvider}, based on
         * the configuration of this builder
         */
        @Nonnull
        public RICoverageAllocationMetricsProvider build() {
            return new RICoverageAllocationMetricsProvider(this);
        }

        private void validateCSPMetric(@Nonnull DataMetricSummary summaryMetric) {

            Preconditions.checkNotNull(summaryMetric);

            if (summaryMetric.getLabelNames().length != 1 ||
                    summaryMetric.getLabelNames()[0] != CLOUD_SERVICE_PROVIDER_LABEL) {
                throw new IllegalArgumentException(
                        String.format("CSP-based metric must have `%s` label",
                                CLOUD_SERVICE_PROVIDER_LABEL));
            }
        }
    }
}
