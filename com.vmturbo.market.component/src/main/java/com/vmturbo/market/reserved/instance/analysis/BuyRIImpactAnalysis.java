package com.vmturbo.market.reserved.instance.analysis;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocationConfig;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageProvider;
import com.vmturbo.reserved.instance.coverage.allocator.metrics.RICoverageAllocationMetricsProvider;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * Buy RI impact analysis is invoked on the projected topology output from market analysis with RI
 * coverage from buy RI recommendations. This analysis provides an estimation of the coverage (and
 * therefore cost) of RI buy recommendations on the cloud topology, after accepting all scaling actions.
 *
 * <p>This analysis differs from including RI buy recommendations within the market (as market tiers), in
 * that it does not bias scaling actions towards the recommendations. It therefore provides a
 * quantitative measure of the accuracy of RI buy recommendations in targeting consumption demand
 * (intended to be the natural demand).
 */
public class BuyRIImpactAnalysis {

    private static final Table<Long, Long, Double> EMPTY_COVERAGE_ALLOCATION = ImmutableTable.of();

    /**
     * A summary metric collecting the total runtime duration of coverage analysis.
     */
    private static final DataMetricSummary TOTAL_COVERAGE_ANALYSIS_DURATION_METRIC = DataMetricSummary.builder()
            .withName("mkt_buy_ri_impact_cov_allocator_duration_seconds")
            .withHelp("Total time for RI coverage allocator analysis of buy RI impact.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric collecting the runtime duration of first pass filtering RIs.
     */
    private static final DataMetricSummary FIRST_PASS_RI_FILTER_DURATION_METRIC = DataMetricSummary.builder()
            .withName("mkt_buy_ri_impact_first_pass_ri_filter_duration_seconds")
            .withHelp("Time for buy RI impact analysis first pass filtering of RIs.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric collecting the runtime duration of first pass filtering entities.
     */
    private static final DataMetricSummary FIRST_PASS_ENTITY_FILTER_DURATION_METRIC = DataMetricSummary.builder()
            .withName("mkt_buy_ri_impact_first_pass_entity_filter_duration_seconds")
            .withHelp("Time for buy RI impact analysis first pass filtering of RIs.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric collecting the runtime duration for creating cloud service provider-specific contexts.
     */
    private static final DataMetricSummary CONTEXT_CREATION_DURATION_METRIC = DataMetricSummary.builder()
            .withName("mkt_buy_ri_impact_context_creation_duration_seconds")
            .withHelp("Time for CSP-specific context creation during buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the runtime duration of buy RI impact analysis.
     */
    private static final DataMetricSummary ALLOCATION_DURATION_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact_allocation_duration_seconds")
            .withHelp("Time for buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the count of coverable entities at the start of buy RI impact analysis.
     */
    private static final DataMetricSummary COVERABLE_ENTITIES_COUNT_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact_coverable_entities_counts")
            .withHelp("The number of coverable entities at the start of buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the count of RIs with unallocated coverage capacity
     * at the start of buy RI impact analysis.
     */
    private static final DataMetricSummary RESERVED_INSTANCE_COUNT_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact_reserved_instance_count")
            .withHelp("The number of RIs with unallocated coverage at the start of buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the sum of uncovered coverage amount for entities
     * at the start of buy RI impact analysis.
     */
    private static final DataMetricSummary UNCOVERED_ENTITY_CAPACITY_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact_uncovered_entity_capacity")
            .withHelp("The sum of uncovered entity capacity at the start of buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the sum of unallocated coverage for RIs at the start of buy
     * RI impact analysis.
     */
    private static final DataMetricSummary UNALLOCATED_RI_CAPACITY_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact_unallocated_ri_capacity")
            .withHelp("The sum of unallocated RI capacity at the start of buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the count of allocations created as part of buy RI impact analysis.
     */
    private static final DataMetricSummary ALLOCATION_COUNT_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact__allocation_count")
            .withHelp("The number of allocations for buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    /**
     * A summary metric for collecting the sum of allocated coverage for through the buy RI impact analysis.
     */
    private static final DataMetricSummary ALLOCATED_COVERAGE_AMOUNT_METRIC = RICoverageAllocationMetricsProvider
            .newCloudServiceProviderMetric()
            .withName("mkt_buy_ri_impact_allocated_cov_amount")
            .withHelp("The total coverage amount allocated through buy RI impact analysis.")
            .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
            .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
            .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .withMaxAgeSeconds(60 * 10) // 10 mins.
            .withAgeBuckets(5) // 5 buckets, so buckets get switched every 4 minutes.
            .build()
            .register();

    private final Logger logger = LogManager.getLogger();

    private final CoverageAllocatorFactory allocatorFactory;

    private final TopologyInfo topologyInfo;

    private final CoverageTopology coverageTopology;

    private final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid;

    private final boolean allocatorConcurrentProcessing;

    private final boolean validateCoverages;

    /**
     * Construct an instance of {@link BuyRIImpactAnalysis}.
     * @param allocatorFactory A {@link CoverageAllocatorFactory} instance
     * @param topologyInfo The {@link TopologyInfo} of the {@code coverageTopology}
     * @param coverageTopology The target {@link CoverageTopology} of the analysis
     * @param riCoverageByEntityOid The source RI coverage as a basis for analysis
     * @param allocatorConcurrentProcessing Whether the {@link ReservedInstanceCoverageAllocator}
     *                                      should use concurrent processing
     * @param validateCoverages Whether the {@link ReservedInstanceCoverageAllocator} should validate
     *                          coverage after analysis
     */
    public BuyRIImpactAnalysis(
            @Nonnull CoverageAllocatorFactory allocatorFactory,
            @Nonnull TopologyInfo topologyInfo,
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid,
            boolean allocatorConcurrentProcessing,
            boolean validateCoverages) {

        this.allocatorFactory = Objects.requireNonNull(allocatorFactory);
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.riCoverageByEntityOid = ImmutableMap.copyOf(Objects.requireNonNull(riCoverageByEntityOid));
        this.allocatorConcurrentProcessing = allocatorConcurrentProcessing;
        this.validateCoverages = validateCoverages;
    }

    /**
     * Runs buy RI impact analysis (through the {@link ReservedInstanceCoverageAllocator}), creating
     * and returning {@link EntityReservedInstanceCoverage} representing the complete set of RI coverage
     * (both RIs in inventory and buy RI recommendations for entities within scope.
     *
     * <p>Essentially, the impact of buy RI recommendations is derived from the potential coverage on
     * the projected topology, in which RI coverage from RIs in inventory takes precedence over
     * buy RI recommendations.
     *
     * @return A table, representing {@literal <Entity OID, RI ID, Coverage Amount>}. This will represent
     * only allocated coverage as part of this analysis and will not include the RI coverage input
     * in constructing the analysis.
     */
    @Nonnull
    public Table<Long, Long, Double> allocateCoverageFromBuyRIImpactAnalysis() {
        try {
            logger.info("Running BuyRIImpactAnalysis (Topology Context ID={}, Topology ID={})",
                    topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId());

            final ReservedInstanceCoverageProvider coverageProvider = createCoverageProvider();
            final ReservedInstanceCoverageAllocator coverageAllocator = allocatorFactory.createAllocator(
                    CoverageAllocationConfig.builder()
                            .coverageProvider(coverageProvider)
                            .coverageTopology(coverageTopology)
                            .metricsProvider(createMetricsProvider())
                            .concurrentProcessing(allocatorConcurrentProcessing)
                            .validateCoverages(validateCoverages)
                            .build());

            final ReservedInstanceCoverageAllocation coverageAllocation =
                    coverageAllocator.allocateCoverage();

            logger.info("Finished BuyRIImpactAnalysis (Topology ID={}, RI Count={}, Allocation Count={})",
                    topologyInfo.getTopologyId(),
                    coverageTopology.getAllRIAggregates().size(),
                    coverageAllocation.allocatorCoverageTable().size());

            return coverageAllocation.allocatorCoverageTable();
        } catch (Exception e) {
            logger.error("Error running buy RI analysis (TopologyInfo={})", topologyInfo, e);
            return EMPTY_COVERAGE_ALLOCATION;
        }
    }

    /**
     * Creates the coverage provider as a starting point for the {@link ReservedInstanceCoverageAllocator}.
     * The coverage is built from the input {@link EntityReservedInstanceCoverage} records.
     *
     * @return A {@link ReservedInstanceCoverageProvider}, with an immutable mapping for
     * {@link ReservedInstanceCoverageProvider#getAllCoverages()}
     */
    @Nonnull
    private ReservedInstanceCoverageProvider createCoverageProvider() {
        final Table<Long, Long, Double> coverage = riCoverageByEntityOid.values()
                .stream()
                .map(entityRICoverage ->
                        entityRICoverage.getCouponsCoveredByRiMap()
                                .entrySet()
                                .stream()
                                .map(riCoverage -> ImmutableTriple.of(
                                        entityRICoverage.getEntityId(),
                                        riCoverage.getKey(),
                                        riCoverage.getValue()))
                                .collect(Collectors.toSet()))
                .flatMap(Set::stream)
                .collect(ImmutableTable.toImmutableTable(
                        ImmutableTriple::getLeft,
                        ImmutableTriple::getMiddle,
                        ImmutableTriple::getRight));

        return () -> coverage;
    }

    /**
     * Creates a {@link RICoverageAllocationMetricsProvider}.
     * @return The newly created {@link RICoverageAllocationMetricsProvider} instance.
     */
    @Nonnull
    private RICoverageAllocationMetricsProvider createMetricsProvider() {
        return RICoverageAllocationMetricsProvider.newBuilder()
                .totalCoverageAnalysisDuration(TOTAL_COVERAGE_ANALYSIS_DURATION_METRIC)
                .firstPassRIFilterDuration(FIRST_PASS_RI_FILTER_DURATION_METRIC)
                .firstPassEntityFilterDuration(FIRST_PASS_ENTITY_FILTER_DURATION_METRIC)
                .contextCreationDuration(CONTEXT_CREATION_DURATION_METRIC)
                .allocationDurationByCSP(ALLOCATION_DURATION_METRIC)
                .coverableEntityCountByCSP(COVERABLE_ENTITIES_COUNT_METRIC)
                .reservedInstanceCountByCSP(RESERVED_INSTANCE_COUNT_METRIC)
                .uncoveredEntityCapacityByCSP(UNCOVERED_ENTITY_CAPACITY_METRIC)
                .unallocatedRICapacityByCSP(UNALLOCATED_RI_CAPACITY_METRIC)
                .allocationCountByCSP(ALLOCATION_COUNT_METRIC)
                .allocatedCoverageAmountByCSP(ALLOCATED_COVERAGE_AMOUNT_METRIC)
                .build();
    }

    @Nonnull
    private Map<Long, EntityReservedInstanceCoverage> convertCoverageAllocationToEntityRICoverage(
            @Nonnull ReservedInstanceCoverageAllocation coverageAllocation) {

        return coverageAllocation.totalCoverageTable()
                .rowMap()
                .entrySet()
                .stream()
                .map(entityRICoverageEntry ->
                        EntityReservedInstanceCoverage.newBuilder()
                                .setEntityId(entityRICoverageEntry.getKey())
                                .putAllCouponsCoveredByRi(entityRICoverageEntry.getValue())
                                .build())
                .collect(ImmutableMap.toImmutableMap(
                        EntityReservedInstanceCoverage::getEntityId,
                        Function.identity()));
    }
}
