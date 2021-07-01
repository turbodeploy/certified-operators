package com.vmturbo.market.reserved.instance.analysis;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudTopology;
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

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

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
     * @param cloudTopology The target {@link CloudTopology} of the analysis
     */
    public BuyRIImpactAnalysis(
            @Nonnull CoverageAllocatorFactory allocatorFactory,
            @Nonnull TopologyInfo topologyInfo,
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid,
            boolean allocatorConcurrentProcessing,
            boolean validateCoverages,
            CloudTopology<TopologyEntityDTO> cloudTopology) {

        this.allocatorFactory = Objects.requireNonNull(allocatorFactory);
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.riCoverageByEntityOid = ImmutableMap.copyOf(Objects.requireNonNull(riCoverageByEntityOid));
        this.allocatorConcurrentProcessing = allocatorConcurrentProcessing;
        this.validateCoverages = validateCoverages;
        this.cloudTopology = cloudTopology;
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
    public BuyCommitmentImpactResult allocateCoverageFromBuyRIImpactAnalysis() {
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

            final Table<Long, Long, Double> buyRIAllocatedCoverage =
                    coverageAllocation.allocatorCoverageTable();
            final List<Action.Builder> allocateActions = generateBuyRIAllocateActions(
                    buyRIAllocatedCoverage.rowKeySet());

            logger.info("Finished BuyRIImpactAnalysis (Topology ID={}, RI Count={}, Allocation Count={})",
                    topologyInfo.getTopologyId(),
                    coverageTopology.getAllRIAggregates().size(),
                    buyRIAllocatedCoverage.size());

            return BuyCommitmentImpactResult.builder()
                    .buyCommitmentCoverage(buyRIAllocatedCoverage)
                    .addAllBuyAllocationActions(allocateActions)
                    .build();
        } catch (Exception e) {
            logger.error("Error running buy RI analysis (TopologyInfo={})", topologyInfo, e);
            return BuyCommitmentImpactResult.EMPTY_RESULT;
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

    /**
     * For each of the VM that got covered by a Buy RI during the buyRIImpact analysis
     * we generate and allocate action.
     * @param ids A List of vm id that got covered by the newly bought ri's.
     * @return A list of allocate actions. one for each VM.
     */
    public List<Action.Builder> generateBuyRIAllocateActions(Set<Long> ids) {

        final ImmutableList.Builder<Action.Builder> allocateActions = ImmutableList.builder();
        for (Long id : ids) {
            Optional<Action.Builder> action = createAllocateAction(id);
            if (action.isPresent()) {
                allocateActions.add(action.get());
            }
        }
        return allocateActions.build();
    }

    /**
     * create an allocate action for the VM with the given id.
     *
     * @param id the vm oid for which we are creating an allocate action.
     * @return the allocate action for the vm of interest.
     */
    private Optional<Action.Builder> createAllocateAction(long id) {
        final Optional<TopologyEntityDTO> targetEntityO =  cloudTopology.getEntity(id);
        final Optional<TopologyEntityDTO> computeTierO = cloudTopology.getComputeTier(id);

        if (!targetEntityO.isPresent() || !computeTierO.isPresent()) {
            return Optional.empty();
        }

        TopologyEntityDTO computeTier = computeTierO.get();
        Explanation.Builder expBuilder = Explanation.newBuilder();
        AllocateExplanation explanation = AllocateExplanation.newBuilder()
                .setInstanceSizeFamily(computeTier.getTypeSpecificInfo()
                        .getComputeTier().getFamily())
                .build();
        expBuilder.setAllocate(explanation);
        final Action.Builder action;
        final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();
        action = Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0d)
                .setExplanation(expBuilder)
                .setExecutable(false);
        action.setInfo(infoBuilder);

        TopologyEntityDTO targetEntity = targetEntityO.get();
        ActionEntity targetActionEntity = ActionEntity.newBuilder()
                .setId(targetEntity.getOid())
                .setType(targetEntity.getEntityType())
                .setEnvironmentType(targetEntity.getEnvironmentType())
                .build();
        ActionEntity computeTierActionEntity = ActionEntity.newBuilder()
                .setId(computeTier.getOid())
                .setType(computeTier.getEntityType())
                .setEnvironmentType(computeTier.getEnvironmentType())
                .build();

        ActionDTO.Allocate.Builder allocateAction = ActionDTO.Allocate.newBuilder()
                .setIsBuyRecommendationCoverage(true)
                .setTarget(targetActionEntity)
                .setWorkloadTier(computeTierActionEntity);
        action.getInfoBuilder().setAllocate(allocateAction);
        return Optional.of(action);
    }

    /**
     * The result of impact analysis for buy cloud commitment recommendations (Buy RI).
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface BuyCommitmentImpactResult {

        /**
         * An empty result (no allocations or actions).
         */
        BuyCommitmentImpactResult EMPTY_RESULT = BuyCommitmentImpactResult.builder()
                .buyCommitmentCoverage(ImmutableTable.of())
                .build();

        /**
         * A table representing entity -> buy RI recommendation -> allocated coverage.
         * @return An immutable table representing buy commitment coverage.
         */
        @Nonnull
        Table<Long, Long, Double> buyCommitmentCoverage();

        /**
         * A list of allocate action builders, allocating the coverage from {@link #buyCommitmentCoverage()}.
         * @return An immutable list of allocate action builders, allocating the coverage
         * from {@link #buyCommitmentCoverage()}.
         */
        @Nonnull
        List<Action.Builder> buyAllocationActions();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link BuyCommitmentImpactResult} instances.
         */
        class Builder extends ImmutableBuyCommitmentImpactResult.Builder {}
    }
}
