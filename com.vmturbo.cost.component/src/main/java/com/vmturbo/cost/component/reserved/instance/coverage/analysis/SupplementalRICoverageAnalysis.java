package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.reserved.instance.coverage.allocator.ImmutableRICoverageAllocatorConfig;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageProvider;
import com.vmturbo.reserved.instance.coverage.allocator.metrics.RICoverageAllocationMetricsProvider;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * This class is a wrapper around {@link ReservedInstanceCoverageAllocator}, invoking it on the
 * realtime topology with RI inventory in order to fill in RI coverage & utilization due to any
 * delays in the coverage reported from cloud billing probes. This supplemental analysis is required,
 * due to billing probes generally returning coverage day with ~24 hour delay, which may cause
 * under reporting of coverage. Downstream impacts of lower RI coverage will be exhibited in both
 * the current costs reflected in CCC and market scaling decisions.
 * <p>
 * Supplemental RI coverage analysis is meant to build on top of the coverage allocations from
 * the providers billing data. Therefore this analysis accepts {@link EntityRICoverageUpload}
 * entries, representing the RI coverage extracted from the bill
 */
public class SupplementalRICoverageAnalysis {

    private final Logger logger = LogManager.getLogger();

    /**
     * A summary metric collecting the total runtime duration of coverage analysis
     */
    private static final DataMetricSummary TOTAL_COVERAGE_ANALYSIS_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_ri_cov_analysis_duration_seconds")
                    .withHelp("Total time for supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric collecting the runtime duration of first pass filtering RIs.
     */
    private static final DataMetricSummary FIRST_PASS_RI_FILTER_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_ri_cov_first_pass_ri_filter_duration_seconds")
                    .withHelp("Time for supplemental RI coverage analysis first pass filtering of RIs.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric collecting the runtime duration of first pass filtering entities
     */
    private static final DataMetricSummary FIRST_PASS_ENTITY_FILTER_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_ri_cov_first_pass_entity_filter_duration_seconds")
                    .withHelp("Time for supplemental RI coverage analysis first pass filtering of entities.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric collecting the runtime duration for creating cloud service provider-specific contexts.
     */
    private static final DataMetricSummary CONTEXT_CREATION_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_ri_cov_context_creation_duration_seconds")
                    .withHelp("Time for CSP-specific context creation during supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the runtime duration of RI coverage allocation analysis.
     */
    private static final DataMetricSummary ALLOCATION_DURATION_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_allocation_duration_seconds")
                    .withHelp("Time for supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the count of coverable entities at the start of RI coverage analysis.
     */
    private static final DataMetricSummary COVERABLE_ENTITIES_COUNT_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_coverable_entities_counts")
                    .withHelp("The number of coverable entities at the start of supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the count of RIs with unallocated coverage capacity
     * at the start of RI coverage analysis.
     */
    private static final DataMetricSummary RESERVED_INSTANCE_COUNT_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_reserved_instance_count")
                    .withHelp("The number of RIs with unallocated coverage at the start of supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the sum of uncovered coverage amount for entities
     * at the start of RI coverage analysis.
     */
    private static final DataMetricSummary UNCOVERED_ENTITY_CAPACITY_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_uncovered_entity_capacity")
                    .withHelp("The sum of uncovered entity capacity at the start of supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the sum of unallocated coverage for RIs at the start of RI
     * coverage analysis.
     */
    private static final DataMetricSummary UNALLOCATED_RI_CAPACITY_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_unallocated_ri_capacity")
                    .withHelp("The sum of unallocated RI capacity at the start of supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the count of allocations created as part of RI coverage
     * allocation analysis.
     */
    private static final DataMetricSummary ALLOCATION_COUNT_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_allocation_count")
                    .withHelp("The number of allocations for supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    /**
     * A summary metric for collecting the sum of allocated coverage for through the RI coverage
     * allocator analysis.
     */
    private static final DataMetricSummary ALLOCATED_COVERAGE_AMOUNT_SUMMARY_METRIC =
            RICoverageAllocationMetricsProvider
                    .newCloudServiceProviderMetric()
                    .withName("cost_ri_cov_allocated_cov_amount")
                    .withHelp("The total coverage amount allocated through supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private final RICoverageAllocatorFactory allocatorFactory;

    private final CoverageTopology coverageTopology;

    private final List<EntityRICoverageUpload> entityRICoverageUploads;

    private final boolean allocatorConcurrentProcessing;

    private final boolean validateCoverages;

    /**
     * Constructor for creating an instance of {@link SupplementalRICoverageAnalysis}
     * @param allocatorFactory An instance of {@link RICoverageAllocatorFactory} to create allocator
     *                         instances
     * @param coverageTopology The {@link CoverageTopology} to pass through to the
     *                         {@link ReservedInstanceCoverageAllocator}
     * @param entityRICoverageUploads The baseline RI coverage entries
     * @param allocatorConcurrentProcessing A boolean flag indicating whether concurrent coverage
     *                                      allocation should be enabled
     * @param validateCoverages A boolean flag indicating whether {@link ReservedInstanceCoverageAllocator}
     *                          validation should be enabled.
     */
    public SupplementalRICoverageAnalysis(
            @Nonnull RICoverageAllocatorFactory allocatorFactory,
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull List<EntityRICoverageUpload> entityRICoverageUploads,
            boolean allocatorConcurrentProcessing,
            boolean validateCoverages) {

        this.allocatorFactory = allocatorFactory;
        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.entityRICoverageUploads = ImmutableList.copyOf(
                Objects.requireNonNull(entityRICoverageUploads));
        this.allocatorConcurrentProcessing = allocatorConcurrentProcessing;
        this.validateCoverages = validateCoverages;
    }


    /**
     * Invokes the {@link ReservedInstanceCoverageAllocator}, converting the response and to
     * {@link EntityRICoverageUpload} records and merging them with the source records.
     *
     * @return The merged {@link EntityRICoverageUpload} records. If an entity is covered by an
     * RI through both the bill and the coverage allocator, it will have two distinct
     * {@link Coverage} records
     */
    public List<EntityRICoverageUpload> createCoverageRecordsFromSupplementalAllocation() {

        try {
            final ReservedInstanceCoverageProvider coverageProvider = createCoverageProvider();
            final ReservedInstanceCoverageAllocator coverageAllocator = allocatorFactory.createAllocator(
                    ImmutableRICoverageAllocatorConfig.builder()
                            .coverageProvider(coverageProvider)
                            .coverageTopology(coverageTopology)
                            .metricsProvider(createMetricsProvider())
                            .concurrentProcessing(allocatorConcurrentProcessing)
                            .validateCoverages(validateCoverages)
                            .build());

            final ReservedInstanceCoverageAllocation coverageAllocation = coverageAllocator.allocateCoverage();

            return ImmutableList.copyOf(createCoverageUploadsFromAllocation(coverageAllocation));
        } catch (Exception e) {
            logger.error("Error during supplemental coverage analysis", e);
            // return the input RI coverage list
            return entityRICoverageUploads;
        }
    }

    /**
     * Creates an instance of {@link ReservedInstanceCoverageProvider} based on the input
     * {@link EntityRICoverageUpload} instances. The coverage provider is used by the
     * {@link ReservedInstanceCoverageAllocator} as a starting point in adding supplemental coverage.
     *
     * @return The newly created {@link ReservedInstanceCoverageProvider} instance
     */
    private ReservedInstanceCoverageProvider createCoverageProvider() {
        final Table<Long, Long, Double> entityRICoverage = entityRICoverageUploads.stream()
                .map(entityRICoverageUpload -> entityRICoverageUpload.getCoverageList()
                        .stream()
                        .map(coverage -> ImmutablePair.of(entityRICoverageUpload.getEntityId(),
                                coverage))
                        .collect(Collectors.toSet()))
                .flatMap(Set::stream)
                .collect(ImmutableTable.toImmutableTable(
                        Pair::getLeft,
                        coveragePair -> coveragePair.getRight().getReservedInstanceId(),
                        coveragePair -> coveragePair.getRight().getCoveredCoupons(),
                        (c1, c2) -> Double.sum(c1, c2)));
        return () -> entityRICoverage;
    }

    private RICoverageAllocationMetricsProvider createMetricsProvider() {
        return RICoverageAllocationMetricsProvider.newBuilder()
                .totalCoverageAnalysisDuration(TOTAL_COVERAGE_ANALYSIS_DURATION_SUMMARY_METRIC)
                .firstPassEntityFilterDuration(FIRST_PASS_ENTITY_FILTER_DURATION_SUMMARY_METRIC)
                .firstPassRIFilterDuration(FIRST_PASS_RI_FILTER_DURATION_SUMMARY_METRIC)
                .contextCreationDuration(CONTEXT_CREATION_DURATION_SUMMARY_METRIC)
                .allocationDurationByCSP(ALLOCATION_DURATION_SUMMARY_METRIC)
                .coverableEntityCountByCSP(COVERABLE_ENTITIES_COUNT_SUMMARY_METRIC)
                .reservedInstanceCountByCSP(RESERVED_INSTANCE_COUNT_SUMMARY_METRIC)
                .uncoveredEntityCapacityByCSP(UNCOVERED_ENTITY_CAPACITY_SUMMARY_METRIC)
                .unallocatedRICapacityByCSP(UNALLOCATED_RI_CAPACITY_SUMMARY_METRIC)
                .allocationCountByCSP(ALLOCATION_COUNT_SUMMARY_METRIC)
                .allocatedCoverageAmountByCSP(ALLOCATED_COVERAGE_AMOUNT_SUMMARY_METRIC)
                .build();

    }

    /**
     * Merges the source coverage upload records with newly created records based on allocated
     * coverage from the {@link ReservedInstanceCoverageAllocator}. The RI coverage source of the newly
     * created records with be {@link RICoverageSource#SUPPLEMENTAL_COVERAGE_ALLOCATION}.
     *
     * @param coverageAllocation The allocated coverage from the {@link ReservedInstanceCoverageAllocator}
     * @return Merged {@link EntityRICoverageUpload} records
     */
    private Collection<EntityRICoverageUpload> createCoverageUploadsFromAllocation(
            @Nonnull ReservedInstanceCoverageAllocation coverageAllocation) {

        // First build a map of entity ID to EntityRICoverageUpload for faster lookup
        // in iterating coverageAllocation
        final Map<Long, EntityRICoverageUpload> coverageUploadsByEntityOid = entityRICoverageUploads.stream()
                .collect(Collectors.toMap(
                        EntityRICoverageUpload::getEntityId,
                        Function.identity()));

        coverageAllocation.allocatorCoverageTable()
                // Iterate over each entity -> RI coverage map, reducing the number of times the
                // EntityRICoverageUpload needs to be converted to a builder
                .rowMap()
                .forEach((entityOid, riCoverageMap) ->
                    coverageUploadsByEntityOid.compute(entityOid, (oid, coverageUpload) -> {
                        // Either re-use a previously existing EntityRICoverageUpload, if one exists
                        // or create a new one
                        final EntityRICoverageUpload.Builder coverageUploadBuilder =
                                coverageUpload == null ?
                                        EntityRICoverageUpload.newBuilder()
                                                .setEntityId(entityOid)
                                                .setTotalCouponsRequired(
                                                        coverageTopology.getRICoverageCapacityForEntity(entityOid)) :
                                        coverageUpload.toBuilder();
                        // Add each coverage entry, setting the source appropriately
                        riCoverageMap.entrySet()
                                .forEach(riEntry ->
                                        coverageUploadBuilder.addCoverage(
                                                Coverage.newBuilder()
                                                        .setReservedInstanceId(riEntry.getKey())
                                                        .setCoveredCoupons(riEntry.getValue())
                                                        .setRiCoverageSource(
                                                                RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)));


                        return coverageUploadBuilder.build();
                    }));

        return coverageUploadsByEntityOid.values();
    }
}
