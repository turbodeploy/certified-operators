package com.vmturbo.reserved.instance.coverage.allocator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.reserved.instance.coverage.allocator.metrics.RICoverageAllocationMetricsProvider;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * A configuration for a {@link ReservedInstanceCoverageAllocator} instance.
 */
@HiddenImmutableImplementation
@Immutable
public interface CoverageAllocationConfig {

    /**
     * The {@link ReservedInstanceCoverageProvider} used to configure an {@link ReservedInstanceCoverageAllocator}
     * instance. The coverage provider provides the basis for RI coverage & utilization, which the allocator
     * will build upon.
     * @return The {@link ReservedInstanceCoverageProvider} instance.
     */
    @Default
    default ReservedInstanceCoverageProvider coverageProvider() {
        return ReservedInstanceCoverageProvider.EMPTY_COVERAGE_PROVIDER;
    }

    /**
     * The {@link CoverageTopology} instance, containing both entities and RIs to analyze in
     * allocating coverage.
     * @return The {@link CoverageTopology} instance.
     */
    @Nonnull
    CoverageTopology coverageTopology();

    /**
     * An optional {@link AccountFilter}, used to scope the analysis to a subset of the
     * {@link CoverageTopology}.
     * @return An optional {@link AccountFilter} or null if no filter is configured.
     */
    @Nullable
    Cost.AccountFilter accountFilter();

    /**
     * An optional {@link EntityFilter}, used to scope the analysis to a subset of the
     * {@link CoverageTopology}.
     * @return An optional {@link EntityFilter} or null if no filter is configured.
     */
    @Nullable
    Cost.EntityFilter entityFilter();

    /**
     * Configures how to preference cloud commitment aggregates when multiple aggregates match
     * within a coverage group.
     * @return The {@link CloudCommitmentPreference}. Will default to
     * {@link CloudCommitmentPreference#DEFAULT_PREFERENCE}, if no preference is provided.
     */
    @Nonnull
    @Default
    default CloudCommitmentPreference cloudCommitmentPreference() {
        return CloudCommitmentPreference.DEFAULT_PREFERENCE;
    }

    /**
     * Configures how to preference coverage entities  when multiple entities match
     * within a coverage group.
     * @return The {@link CoverageEntityPreference}. Will default to
     * {@link CoverageEntityPreference#DEFAULT_PREFERENCE}, if no preference is provided.
     */
    @Nonnull
    @Default
    default CoverageEntityPreference coverageEntityPreference() {
        return CoverageEntityPreference.DEFAULT_PREFERENCE;
    }

    /**
     * An optional metrics provider (a default provider will be used if none are configured), used
     * in collecting metrics. Only metrics provided by the {@link RICoverageAllocationMetricsProvider}
     * instance will be collected.
     * @return The {@link RICoverageAllocationMetricsProvider} instance.
     */
    @Nonnull
    @Value.Default
    default RICoverageAllocationMetricsProvider metricsProvider() {
        return RICoverageAllocationMetricsProvider.EMPTY_PROVIDER;
    }

    /**
     * Determines whether validation of allocated coverage should occur after analysis. The default
     * is false.
     * @return A boolean flag indicating whether to validate coverage.
     */
    @Value.Default
    default boolean validateCoverages() {
        return false;
    }

    /**
     * Determines whether concurrent analysis s enabled. If true, where possible (e.g. across
     * cloud providers, within a rule with disjoint groups), the allocator will concurrently
     * process units of work.
     * @return A boolean flag indicating whether concurrent processing is enabled. The default
     * is true.
     */
    @Value.Default
    default boolean concurrentProcessing() {
        return true;
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link CoverageAllocationConfig} instances.
     */
    class Builder extends ImmutableCoverageAllocationConfig.Builder {}
}
