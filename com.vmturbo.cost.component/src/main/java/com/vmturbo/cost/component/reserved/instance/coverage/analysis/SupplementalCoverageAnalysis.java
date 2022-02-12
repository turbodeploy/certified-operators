package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.commons.Units;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocationConfig;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.metrics.CoverageAllocationMetricsProvider;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * This class is a wrapper around {@link CloudCommitmentCoverageAllocator}, invoking it on the
 * realtime topology with RI inventory in order to fill in RI coverage & utilization due to any
 * delays in the coverage reported from cloud billing probes. This supplemental analysis is required,
 * due to billing probes generally returning coverage day with ~24 hour delay, which may cause
 * under reporting of coverage. Downstream impacts of lower RI coverage will be exhibited in both
 * the current costs reflected in CCC and market scaling decisions.
 *
 * <p>Supplemental RI coverage analysis is meant to build on top of the coverage allocations from
 * the providers billing data. Therefore this analysis accepts {@link EntityRICoverageUpload}
 * entries, representing the RI coverage extracted from the bill
 */
public class SupplementalCoverageAnalysis {

    private final Logger logger = LogManager.getLogger();

    /**
     * A summary metric collecting the total runtime duration of coverage analysis.
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
     * A summary metric collecting the runtime duration of first pass filtering entities.
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
            CoverageAllocationMetricsProvider
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
            CoverageAllocationMetricsProvider
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
            CoverageAllocationMetricsProvider
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
            CoverageAllocationMetricsProvider
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
            CoverageAllocationMetricsProvider
                    .newCoverageTypeMetric()
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
            CoverageAllocationMetricsProvider
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
            CoverageAllocationMetricsProvider
                    .newCoverageTypeMetric()
                    .withName("cost_ri_cov_allocated_cov_amount")
                    .withHelp("The total coverage amount allocated through supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private final CoverageAllocatorFactory allocatorFactory;

    private final CoverageTopology coverageTopology;

    private final List<EntityRICoverageUpload> entityRICoverageUploads;

    private final boolean allocatorConcurrentProcessing;

    private final boolean validateCoverages;

    private final boolean logCoverageEntries;

    /**
     * Constructor for creating an instance of {@link SupplementalCoverageAnalysis}.
     * @param allocatorFactory An instance of {@link CoverageAllocatorFactory} to create allocator
     *                         instances
     * @param coverageTopology The {@link CoverageTopology} to pass through to the
     *                         {@link CloudCommitmentCoverageAllocator}
     * @param entityRICoverageUploads The baseline RI coverage entries
     * @param allocatorConcurrentProcessing A boolean flag indicating whether concurrent coverage
     *                                      allocation should be enabled
     * @param validateCoverages A boolean flag indicating whether {@link CloudCommitmentCoverageAllocator}
     *                          validation should be enabled.
     * @param logCoverageEntries Flag indicating whether coverage entries from the coverage allocator
     *                           should be logged.
     */
    public SupplementalCoverageAnalysis(
            @Nonnull CoverageAllocatorFactory allocatorFactory,
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull List<EntityRICoverageUpload> entityRICoverageUploads,
            boolean allocatorConcurrentProcessing,
            boolean validateCoverages,
            boolean logCoverageEntries) {

        this.allocatorFactory = allocatorFactory;
        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.entityRICoverageUploads = ImmutableList.copyOf(
                Objects.requireNonNull(entityRICoverageUploads));
        this.allocatorConcurrentProcessing = allocatorConcurrentProcessing;
        this.validateCoverages = validateCoverages;
        this.logCoverageEntries = logCoverageEntries;
    }


    /**
     * Invokes the {@link CloudCommitmentCoverageAllocator}, converting the response and to
     * {@link EntityRICoverageUpload} records and merging them with the source records.
     *
     * @return The merged {@link EntityRICoverageUpload} records. If an entity is covered by an
     * RI through both the bill and the coverage allocator, it will have two distinct
     * {@link Coverage} records
     */
    public List<EntityRICoverageUpload> createCoverageRecordsFromSupplementalAllocation() {

        try {
            final Stopwatch stopwatch = Stopwatch.createStarted();
            final CloudCommitmentCoverageAllocator coverageAllocator = allocatorFactory.createAllocator(
                    CoverageAllocationConfig.builder()
                            .sourceCoverage(createSourceCoverage())
                            .coverageTopology(coverageTopology)
                            .metricsProvider(createMetricsProvider())
                            .concurrentProcessing(allocatorConcurrentProcessing)
                            .validateCoverages(validateCoverages)
                            .build());

            final CloudCommitmentCoverageAllocation coverageAllocation = coverageAllocator.allocateCoverage();

            logger.info("Supplemental analysis created {} allocations in {}",
                    coverageAllocation.allocatorCoverageTable().size(), stopwatch);
            if (logCoverageEntries || logger.isDebugEnabled()) {
                logger.log(logCoverageEntries ? Level.INFO : Level.DEBUG,
                        "Supplemental coverage entries:\n{}", Joiner.on("\n").join(coverageAllocation.coverageJournalEntries()));
            }

            return ImmutableList.copyOf(createCoverageUploadsFromAllocation(coverageAllocation));
        } catch (Exception e) {
            logger.error("Error during supplemental coverage analysis", e);
            // return the input RI coverage list
            return entityRICoverageUploads;
        }
    }

    /**
     * Creates the source coverage based on the input
     * {@link EntityRICoverageUpload} instances. The source coverage is used by the
     * {@link CloudCommitmentCoverageAllocator} as a starting point in adding supplemental coverage.
     *
     * @return The source coverage.
     */
    private Table<Long, Long, CloudCommitmentAmount> createSourceCoverage() {
        final Table<Long, Long, CloudCommitmentAmount> entityRICoverage = entityRICoverageUploads.stream()
                .map(entityRICoverageUpload -> entityRICoverageUpload.getCoverageList()
                        .stream()
                        .map(coverage -> ImmutablePair.of(entityRICoverageUpload.getEntityId(),
                                coverage))
                        .collect(Collectors.toSet()))
                .flatMap(Set::stream)
                .collect(ImmutableTable.toImmutableTable(
                        Pair::getLeft,
                        coveragePair -> coveragePair.getRight().getReservedInstanceId(),
                        coveragePair -> CloudCommitmentAmount.newBuilder()
                                .setCoupons(coveragePair.getRight().getCoveredCoupons())
                                .build(),
                        CommitmentAmountCalculator::sum));

        return entityRICoverage;
    }

    private CoverageAllocationMetricsProvider createMetricsProvider() {
        return CoverageAllocationMetricsProvider.newBuilder()
                .totalCoverageAnalysisDuration(TOTAL_COVERAGE_ANALYSIS_DURATION_SUMMARY_METRIC)
                .firstPassEntityFilterDuration(FIRST_PASS_ENTITY_FILTER_DURATION_SUMMARY_METRIC)
                .firstPassRIFilterDuration(FIRST_PASS_RI_FILTER_DURATION_SUMMARY_METRIC)
                .contextCreationDuration(CONTEXT_CREATION_DURATION_SUMMARY_METRIC)
                .allocationDurationByCSP(ALLOCATION_DURATION_SUMMARY_METRIC)
                .coverableEntityCountByCSP(COVERABLE_ENTITIES_COUNT_SUMMARY_METRIC)
                .cloudCommitmentCount(RESERVED_INSTANCE_COUNT_SUMMARY_METRIC)
                .uncoveredEntityPercentage(UNCOVERED_ENTITY_CAPACITY_SUMMARY_METRIC)
                .unallocatedCommitmentCapacity(UNALLOCATED_RI_CAPACITY_SUMMARY_METRIC)
                .allocationCountByCSP(ALLOCATION_COUNT_SUMMARY_METRIC)
                .allocatedCoverageAmount(ALLOCATED_COVERAGE_AMOUNT_SUMMARY_METRIC)
                .build();

    }

    /**
     * Merges the source coverage upload records with newly created records based on allocated
     * coverage from the {@link CloudCommitmentCoverageAllocator}. The RI coverage source of the newly
     * created records with be {@link RICoverageSource#SUPPLEMENTAL_COVERAGE_ALLOCATION}.
     *
     * @param coverageAllocation The allocated coverage from the {@link CloudCommitmentCoverageAllocator}
     * @return Merged {@link EntityRICoverageUpload} records
     */
    private Collection<EntityRICoverageUpload> createCoverageUploadsFromAllocation(
            @Nonnull CloudCommitmentCoverageAllocation coverageAllocation) {

        // First build a map of entity ID to EntityRICoverageUpload for faster lookup
        // in iterating coverageAllocation
        final Map<Long, EntityRICoverageUpload> coverageUploadsByEntityOid = entityRICoverageUploads.stream()
                .collect(Collectors.toMap(
                        EntityRICoverageUpload::getEntityId,
                        Function.identity(), (a1, a2) -> a1));


        coverageAllocation.allocatorCoverageTable()
                // Iterate over each entity -> RI coverage map, reducing the number of times the
                // EntityRICoverageUpload needs to be converted to a builder
                .rowMap()
                .forEach((entityOid, riCoverageMap) ->
                    coverageUploadsByEntityOid.compute(entityOid, (oid, coverageUpload) -> {
                        // Either re-use a previously existing EntityRICoverageUpload, if one exists
                        // or create a new one
                        final EntityRICoverageUpload.Builder coverageUploadBuilder = coverageUpload == null
                                        ? EntityRICoverageUpload.newBuilder()
                                                .setEntityId(entityOid)
                                                .setTotalCouponsRequired(
                                                        coverageTopology.getCoverageCapacityForEntity(entityOid,
                                                                CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                                        : coverageUpload.toBuilder();
                        // Add each coverage entry, setting the source appropriately
                        riCoverageMap.entrySet()
                                .stream()
                                .filter(commitmentEntry -> commitmentEntry.getValue().hasCoupons())
                                .forEach(riEntry ->
                                        coverageUploadBuilder.addCoverage(
                                                Coverage.newBuilder()
                                                        .setReservedInstanceId(riEntry.getKey())
                                                        .setCoveredCoupons(riEntry.getValue().getCoupons())
                                                        .setUsageStartTimestamp(System.currentTimeMillis())
                                                        .setUsageEndTimestamp(System.currentTimeMillis()
                                                                                    + (long)Units.HOUR_MS * 24)
                                                        .setRiCoverageSource(
                                                                RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)));


                        return coverageUploadBuilder.build();
                    }));

        return coverageUploadsByEntityOid.values();
    }
}
