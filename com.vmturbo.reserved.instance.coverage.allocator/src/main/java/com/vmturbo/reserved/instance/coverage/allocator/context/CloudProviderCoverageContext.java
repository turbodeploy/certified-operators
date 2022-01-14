package com.vmturbo.reserved.instance.coverage.allocator.context;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

/**
 * Contains the relevant contextual data for allocating coverage of a specific {@link ServiceProviderInfo},
 * which will be a cloud service provider.
 * All {@link TopologyEntityDTO} instances and {@link ReservedInstanceBought} instances contained
 * within a context will be scoped to the corresponding {@link ServiceProviderInfo}
 */
@Immutable
public class CloudProviderCoverageContext {

    private final ServiceProviderInfo serviceProviderInfo;
    private final CoverageTopology coverageTopology;
    private final Set<Long> cloudCommitmentOids;
    private final Set<Long> coverableEntityOids;


    private CloudProviderCoverageContext(@Nonnull Builder builder) {

        this.serviceProviderInfo = Objects.requireNonNull(builder.serviceProviderInfo);
        this.coverageTopology = Objects.requireNonNull(builder.coverageTopology);
        this.cloudCommitmentOids = ImmutableSet.copyOf(builder.cloudCommitmentOids);
        this.coverableEntityOids = ImmutableSet.copyOf(builder.coverableEntityOids);
    }

    /**
     * @return The {@link ServiceProviderInfo} this context is scoped to.
     */
    public ServiceProviderInfo serviceProviderInfo() {
        return serviceProviderInfo;
    }

    /**
     * @return The {@link CoverageTopology}. Note: the topology *will not* be scoped to a
     * {@link ServiceProviderInfo}. Rather, access to the {@link CoverageTopology} is meant
     * to facilitate resolving entities and reserved instances contained within this context.
     */
    @Nonnull
    public CoverageTopology coverageTopology() {
        return coverageTopology;
    }

    /**
     * @return Oids of {@link ReservedInstanceBought} instances, scoped to a {@link ServiceProviderInfo}
     * of this context
     */
    @Nonnull
    public Set<Long> cloudCommitmentOids() {
        return cloudCommitmentOids;
    }

    /**
     * @return Oids of {@link TopologyEntityDTO} instances, scoped to a {@link ServiceProviderInfo}
     * of this context
     */
    @Nonnull
    public Set<Long> coverableEntityOids() {
        return coverableEntityOids;
    }

    /**
     * Creates a set of {@link CloudProviderCoverageContext} instances, based on the {@link CoverageTopology},
     * {@link ReservedInstanceBought} instances, and {@link TopologyEntityDTO} instances. The RIs and
     * entities will be split into separate contexts based on the {@link ServiceProviderInfo}
     * associated with the origin of each entity
     *
     *
     * @param coverageTopology An instance of {@link CoverageTopology}, used to resolve oids of
     *                         both {@link ReservedInstanceBought} instances and
     *                         {@link TopologyEntityDTO} instances.
     * @param skipPartialContexts If true, any context with only {@link ReservedInstanceBought}
     *                            instances or {@link TopologyEntityDTO} instances will not be returned.
     *                            If false, all created contexts will be returned.
     * @return A set of {@link CloudProviderCoverageContext} instances, containing only oid references
     * to those {@link ReservedInstanceBought} and {@link TopologyEntityDTO} instances passed in
     * through {@code reservedInstances} and {@code entities}.
     */
    public static Set<CloudProviderCoverageContext> createContexts(
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull Stream<CloudCommitmentAggregate> commitmentAggregates,
            @Nonnull Stream<Long> coverableEntityOids,
            boolean skipPartialContexts) {

        final Map<ServiceProviderInfo, CloudProviderCoverageContext.Builder> contextBuildersByProvider =
                new HashMap<>();

        commitmentAggregates.forEach(commitmentAggregate -> {
            // For RIs, the CSP is determined through the associated tier, given the
            // RIs may not be aggregated by purchasing account (if they are scoped to accounts
            // outside of their purchasing account)
            final long serviceProviderOid = commitmentAggregate.aggregateInfo().serviceProviderOid();
            coverageTopology.getServiceProviderInfo(serviceProviderOid).ifPresent(serviceProviderInfo ->
                    contextBuildersByProvider.computeIfAbsent(serviceProviderInfo, (csp) ->
                            CloudProviderCoverageContext.newBuilder()
                                    .serviceProviderInfo(serviceProviderInfo)
                                    .coverageTopology(coverageTopology))
                            .addCloudCommitmentOid(commitmentAggregate.aggregateId()));
        });

        coverableEntityOids.forEach(entityOid -> coverageTopology.getAggregationInfo(entityOid)
                .flatMap(aggregationInfo -> coverageTopology.getServiceProviderInfo(
                        aggregationInfo.serviceProviderOid()))
                .ifPresent(serviceProviderInfo -> contextBuildersByProvider.computeIfAbsent(
                        serviceProviderInfo, (csp) -> CloudProviderCoverageContext.newBuilder()
                                .serviceProviderInfo(serviceProviderInfo)
                                .coverageTopology(coverageTopology)).addCoverableEntityOid(entityOid)));

        return contextBuildersByProvider.values().stream()
                .filter(contextBuilder -> !skipPartialContexts ||
                        (contextBuilder.hasCommitments() &&
                                contextBuilder.hasCoverableEntityOids()))
                .map(CloudProviderCoverageContext.Builder::build)
                .collect(Collectors.toSet());
    }

    /**
     * @return A new instance of {@link Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }


    /**
     * A builder class used to create an instance of {@link CloudProviderCoverageContext}
     */
    public static class Builder {
        private ServiceProviderInfo serviceProviderInfo;
        private CoverageTopology coverageTopology;
        private final Set<Long> cloudCommitmentOids = new HashSet<>();
        private final Set<Long> coverableEntityOids = new HashSet<>();

        /**
         * Set the cloud service provider (represented as a {@link ServiceProviderInfo}) of this builder.
         * @param serviceProviderInfo An instance of {@link ServiceProviderInfo}
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder serviceProviderInfo(@Nonnull ServiceProviderInfo serviceProviderInfo) {
            this.serviceProviderInfo = Objects.requireNonNull(serviceProviderInfo);
            return this;
        }

        /**
         * Set the {@link CoverageTopology} of this builder
         * @param coverageTopology An instance of {@link CoverageTopology}
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder coverageTopology(@Nonnull CoverageTopology coverageTopology) {
            this.coverageTopology = Objects.requireNonNull(coverageTopology);
            return this;
        }

        /**
         * Add an oid of a cloud commitment to add to this builder
         * @param commitmentOid A commitment oid
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder addCloudCommitmentOid(long commitmentOid) {
            this.cloudCommitmentOids.add(commitmentOid);
            return this;
        }

        /**
         * @return True, if this builder is configured with commitment oids.
         * False, otherwise.
         */
        public boolean hasCommitments() {
            return !cloudCommitmentOids.isEmpty();
        }

        /**
         * Add an oid of a {@link TopologyEntityDTO} to this builder
         * @param entityOid A {@link TopologyEntityDTO} oid
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder addCoverableEntityOid(long entityOid) {
            this.coverableEntityOids.add(entityOid);
            return this;
        }

        /**
         * @return True, if this builder is configured with {@link TopologyEntityDTO} oids.
         * False, otherwise.
         */
        public boolean hasCoverableEntityOids() {
            return !coverableEntityOids.isEmpty();
        }

        /**
         * @return A new instance of {@link CloudProviderCoverageContext}, created from this builder.
         */
        public CloudProviderCoverageContext build() {
            return new CloudProviderCoverageContext(this);
        }
    }
}
