package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.Builder;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A factory class for creating instances of {@link SupplementalRICoverageAnalysis}
 */
public class SupplementalRICoverageAnalysisFactory {

    private static final DataMetricSummary RI_SPEC_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_ri_cov_ri_spec_duration_seconds")
                    .withHelp("Total time for supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private static final DataMetricSummary RI_BOUGHT_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_ri_cov_ri_bought_duration_seconds")
                    .withHelp("Total time for supplemental RI coverage analysis.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private final RICoverageAllocatorFactory allocatorFactory;

    private final CoverageTopologyFactory coverageTopologyFactory;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;
    private final AccountRIMappingStore accountRIMappingStore;

    private boolean riCoverageAllocatorValidation;

    private boolean concurrentRICoverageAllocation;

    private final Logger logger = LogManager.getLogger();

    /**
     * Constructs a new instance of {@link SupplementalRICoverageAnalysis}
     * @param allocatorFactory An instance of {@link RICoverageAllocatorFactory}
     * @param coverageTopologyFactory An instance of {@link CoverageTopologyFactory}
     * @param reservedInstanceBoughtStore An instance of {@link ReservedInstanceSpecStore}
     * @param reservedInstanceSpecStore An instance of {@link ReservedInstanceSpecStore}
     * @param riCoverageAllocatorValidation A boolean flag indicating whether validation through the
*                                      RI coverage allocator should be enabled
     * @param concurrentRICoverageAllocation A boolean flag indicating whether concurrent coverage allocation
     * @param accountRIMappingStore An instance of {@link AccountRIMappingStore}
     */
    public SupplementalRICoverageAnalysisFactory(
            @Nonnull RICoverageAllocatorFactory allocatorFactory,
            @Nonnull CoverageTopologyFactory coverageTopologyFactory,
            @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
            boolean riCoverageAllocatorValidation,
            boolean concurrentRICoverageAllocation,
            final AccountRIMappingStore accountRIMappingStore) {

        this.allocatorFactory = Objects.requireNonNull(allocatorFactory);
        this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.riCoverageAllocatorValidation = riCoverageAllocatorValidation;
        this.concurrentRICoverageAllocation = concurrentRICoverageAllocation;
        this.accountRIMappingStore = accountRIMappingStore;
    }

    /**
     * First resolves all available instances of both {@link ReservedInstanceBought} and {@link ReservedInstanceSpec}
     * from the applicable store. After resolving available RIs, a new instance of {@link CoverageTopology}
     * is constructed from the {@code cloudTopology} and resolved RIs/RISpecs. Finally, a new instance of
     * {@link SupplementalRICoverageAnalysis} is constructed based on the {@link CoverageTopology} and
     * {@code entityRICoverageUploads}.
     *
     * @param cloudTopology The {@link CloudTopology}, used to create an instance of {@link CoverageTopology}
     * @param entityRICoverageUploads The source {@link EntityRICoverageUpload} records
     * @return A newly created instance of {@link SupplementalRICoverageAnalysis}
     */
    public SupplementalRICoverageAnalysis createCoverageAnalysis(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull List<EntityRICoverageUpload> entityRICoverageUploads) {

        List<ReservedInstanceBought> reservedInstances =
                reservedInstanceBoughtStore
                    .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER);

        reservedInstances = updateRIsForUndiscoveredAccounts(cloudTopology, reservedInstances);

        // Query only for RI specs referenced from reservedInstances
        final Set<Long> riSpecIds = reservedInstances.stream()
                .filter(ReservedInstanceBought::hasReservedInstanceBoughtInfo)
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .filter(ReservedInstanceBoughtInfo::hasReservedInstanceSpec)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(ImmutableSet.toImmutableSet());
        final List<ReservedInstanceSpec> riSpecs =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds);

        final CoverageTopology coverageTopology = coverageTopologyFactory.createCoverageTopology(
                cloudTopology,
                riSpecs,
                reservedInstances);

        return new SupplementalRICoverageAnalysis(
                allocatorFactory,
                coverageTopology,
                entityRICoverageUploads,
                concurrentRICoverageAllocation,
                riCoverageAllocatorValidation);
    }

    /**
     * For supplemental analysis we need to:
     * 1. Exclude undiscovered RIs in supplemental analysis .
     * 2. RI capacity of discovered RIs will be
     * (RI capacity - Current utilization in undiscovered accounts).
     *
     *
     * @param cloudTopology the cloud topology being processed.
     * @param reservedInstances the reserved instances to be filtered.
     * @return list of updated reserved instances.
     */
    private List<ReservedInstanceBought> updateRIsForUndiscoveredAccounts(
            final CloudTopology<TopologyEntityDTO> cloudTopology,
            final List<ReservedInstanceBought> reservedInstances) {

        final List<TopologyEntityDTO> allAccounts = cloudTopology
                .getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE);
        final Set<Long> discoveredAccounts = allAccounts.stream().filter(
                acc -> acc.hasTypeSpecificInfo()
                        && acc.getTypeSpecificInfo().hasBusinessAccount()
                        && acc.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId())
                .map(acc -> acc.getOid())
                .collect(toSet());

        List<ReservedInstanceBought> updatedRIs =
                reservedInstances.stream()
                        .filter(ri -> discoveredAccounts.contains(
                                ri.getReservedInstanceBoughtInfo().getBusinessAccountId()))
                        .collect(Collectors.toList());
        // for the remaining discovered RIs exclude usage in undiscovered accounts from capacity
        final List<Long> baOids = reservedInstances.stream()
                .filter(ri -> ri.hasReservedInstanceBoughtInfo()
                        && ri.getReservedInstanceBoughtInfo().hasBusinessAccountId())
                .map(ri -> ri.getReservedInstanceBoughtInfo().getBusinessAccountId())
                .distinct()
                .collect(Collectors.toList());
        final Map<Long, Double> riToUsageInUndiscoveredAccounts =
                BusinessAccountHelper.getUndiscoveredAccountUsageForRI(baOids, accountRIMappingStore);

        if (riToUsageInUndiscoveredAccounts.isEmpty()) {
            return updatedRIs;
        }
        updatedRIs = updatedRIs.stream()
                .map(ReservedInstanceBought::toBuilder)
                .peek(ri -> {
                    if (!ri.getReservedInstanceBoughtInfo().hasReservedInstanceBoughtCoupons()) {
                        logger.error("No Coupon information for RI {}", ri.getId());
                        return;
                    }
                    final Double usageFromUndiscoveredAccounts =
                            riToUsageInUndiscoveredAccounts.getOrDefault(ri.getId(), 0d);
                    Double newCapacity = ri.getReservedInstanceBoughtInfo()
                            .getReservedInstanceBoughtCoupons()
                            .getNumberOfCoupons() - usageFromUndiscoveredAccounts;
                    if (newCapacity < 0) {
                        logger.error("Resetting number of coupons ot 0, " +
                                "usage in undiscovered accounts is more than number of coupons for {}",
                                ri.getId());
                        newCapacity = 0d;
                    }
                    ri.getReservedInstanceBoughtInfoBuilder()
                            .getReservedInstanceBoughtCouponsBuilder()
                            .setNumberOfCoupons(newCapacity.intValue());
                })
                .map(Builder::build)
                .collect(Collectors.toList());
        return updatedRIs;

    }


}
