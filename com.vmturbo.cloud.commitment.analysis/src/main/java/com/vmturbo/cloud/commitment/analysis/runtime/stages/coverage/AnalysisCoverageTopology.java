package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CloudAggregationInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ComputeTierInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageEntityInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.VirtualMachineInfo;

/**
 * A {@link CoverageTopology} implementation, built around {@link AggregateCloudTierDemand}.
 */
public class AnalysisCoverageTopology implements CoverageTopology {

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private final Map<Long, AggregateCloudTierDemand> aggregatedDemandById;

    private final Map<Long, CloudCommitmentAggregate> commitmentAggregatesMap;

    private final Map<Long, CloudCommitmentAmount> commitmentCapacityById;


    private AnalysisCoverageTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                     @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                     @Nonnull Map<Long, AggregateCloudTierDemand> aggregatedDemandById,
                                     @Nonnull Set<CloudCommitmentAggregate> commitmentAggregateSet,
                                     @Nonnull Map<Long, CloudCommitmentAmount> commitmentCapacityById) {

        this.cloudTierTopology = Objects.requireNonNull(cloudTierTopology);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.aggregatedDemandById = ImmutableMap.copyOf(Objects.requireNonNull(aggregatedDemandById));
        this.commitmentAggregatesMap = Objects.requireNonNull(commitmentAggregateSet)
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        CloudCommitmentAggregate::aggregateId,
                        Function.identity()));
        this.commitmentCapacityById = ImmutableMap.copyOf(Objects.requireNonNull(commitmentCapacityById));
    }

    /**
     * Returns an immutable map of the encapsulated {@link AggregateCloudTierDemand} within this
     * topology, indexed by their assigned IDs for coverage analysis.
     * @return An immutable map of the encapsulated {@link AggregateCloudTierDemand} within this
     * topology, indexed by their assigned IDs for coverage analysis.
     */
    @Nonnull
    public Map<Long, AggregateCloudTierDemand> getAggregatedDemandById() {
        return aggregatedDemandById;
    }

    /**
     * Returns an immutable map of {@link CloudCommitmentAggregate} instances contained within the
     * topology, indexed by the aggregate ID.
     * @return An immutable map of {@link CloudCommitmentAggregate} instances contained within the
     * topology, indexed by the aggregate ID.
     */
    @Nonnull
    public Map<Long, CloudCommitmentAggregate> getCommitmentAggregatesById() {
        return commitmentAggregatesMap;
    }

    /**
     * Converts the allocation demand as output from the coverage allocated into aggregate demand.
     * Generally, this will convert from coupons (for RIs) to hours of demand.
     * @param aggregateId The aggregate ID.
     * @param commitmentId The commitment ID.
     * @param coverageAmount The coverage amount.
     * @return The aggregate demand amount (generally in terms of hours of uptime).
     */
    @Nullable
    public Triple<Long, Long, Double> convertAllocationDemandToAggregate(
            long aggregateId,
            long commitmentId,
            @Nonnull CloudCommitmentAmount coverageAmount) {

        if (aggregatedDemandById.containsKey(aggregateId)) {
            // assumes compute tier demand
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(aggregateId);

            switch (coverageAmount.getValueCase()) {
                case COUPONS:
                    // This will need to be abstracted when multiple commitment types are supported
                    final Optional<Double> normalizationFactor = computeTierFamilyResolver.getNumCoupons(
                            aggregateDemand.cloudTierInfo().cloudTierDemand().cloudTierOid());

                    // TODO(ejf) log error
                    final double aggregateAmount = coverageAmount.getCoupons() / normalizationFactor.orElse(1D);
                    return ImmutableTriple.of(aggregateId, commitmentId, aggregateAmount);

                default:
                    throw new UnsupportedOperationException(
                            String.format("Cloud commitment type %s is not supported", coverageAmount.getValueCase()));
            }
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<Long> getEntitiesOfType(@Nonnull final int entityType) {

        if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
            // Right now we assume all aggregated demand represents VM demand
            return aggregatedDemandById.keySet();
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<CloudCommitmentAggregate> getCloudCommitment(final long commitmentAggregateOid) {
        return Optional.ofNullable(commitmentAggregatesMap.get(commitmentAggregateOid));
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Set<ReservedInstanceAggregate> getAllRIAggregates() {
        return commitmentAggregatesMap.values().stream()
                .filter(CloudCommitmentAggregate::isReservedInstance)
                .map(CloudCommitmentAggregate::asReservedInstanceAggregate)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public Set<CloudCommitmentAggregate> getAllCloudCommitmentAggregates() {
        return ImmutableSet.copyOf(commitmentAggregatesMap.values());
    }

    @Override
    public double getCommitmentCapacity(long commitmentOid,
                                        @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {

        return commitmentCapacityById.containsKey(commitmentOid)
                ? CommitmentAmountUtils.filterByCoverageKey(commitmentCapacityById.get(commitmentOid), coverageTypeInfo)
                : 0.0;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public double getCoverageCapacityForEntity(long entityOid,
                                               CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        if (aggregatedDemandById.containsKey(entityOid)) {
            // assumes compute tier demand
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(entityOid);

            switch (coverageTypeInfo.getCoverageType()) {
                case COUPONS:
                    // This will need to be abstracted when multiple commitment types are supported
                    final Optional<Double> normalizationFactor = computeTierFamilyResolver.getNumCoupons(
                            aggregateDemand.cloudTierInfo().cloudTierDemand().cloudTierOid());

                    // If the normalization factor cannot be determined, ignore this demand
                    return aggregateDemand.demandAmount() * normalizationFactor.orElse(0D);
                default:
                    throw new UnsupportedOperationException(
                            String.format("Cloud commitment type %s is not supported", coverageTypeInfo));
            }
        } else {
            return 0.0;
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<CloudAggregationInfo> getAggregationInfo(final long entityOid) {
        if (aggregatedDemandById.containsKey(entityOid)) {
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(entityOid);
            final ScopedCloudTierInfo cloudTierInfo = aggregateDemand.cloudTierInfo();
            return Optional.of(CloudAggregationInfo.builder()
                    .serviceProviderOid(cloudTierInfo.serviceProviderOid())
                    .billingFamilyId(cloudTierInfo.billingFamilyId()
                            .map(OptionalLong::of)
                            .orElse(OptionalLong.empty()))
                    .accountOid(cloudTierInfo.accountOid())
                    .regionOid(cloudTierInfo.regionOid())
                    .zoneOid(cloudTierInfo.availabilityZoneOid()
                            .map(OptionalLong::of)
                            .orElse(OptionalLong.empty()))
                    .build());
        } else {
            return Optional.empty();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<CoverageEntityInfo> getEntityInfo(final long entityOid) {
        if (aggregatedDemandById.containsKey(entityOid)) {
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(entityOid);

            if (aggregateDemand.cloudTierInfo().cloudTierType() == CloudTierType.COMPUTE_TIER) {
                final ComputeTierDemand tierDemand = (ComputeTierDemand)aggregateDemand.cloudTierInfo().cloudTierDemand();
                return Optional.of(VirtualMachineInfo.builder()
                        .entityState(EntityState.POWERED_ON)
                        .platform(tierDemand.osType())
                        .tenancy(tierDemand.tenancy())
                        .build());
            } else {
                throw new UnsupportedOperationException(
                        String.format("Cloud tier type %s not supported", aggregateDemand.cloudTierInfo().cloudTierType()));
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<ComputeTierInfo> getComputeTierInfoForEntity(final long entityOid) {
        if (aggregatedDemandById.containsKey(entityOid)) {

            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(entityOid);
            final ScopedCloudTierInfo cloudTierInfo = aggregateDemand.cloudTierInfo();
            if (cloudTierInfo.cloudTierType() == CloudTierType.COMPUTE_TIER) {

                return cloudTierTopology.getEntity(cloudTierInfo.cloudTierDemand().cloudTierOid())
                        .filter(entity -> entity.getEntityType() == EntityType.COMPUTE_TIER_VALUE)
                        .map(computeTier -> ComputeTierInfo.builder()
                                .family(computeTier.getTypeSpecificInfo().getComputeTier().getFamily())
                                .tierOid(computeTier.getOid())
                                .build());
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    @Override
    public Optional<ServiceProviderInfo> getServiceProviderInfo(long serviceProviderOid) {
        return cloudTierTopology.getEntity(serviceProviderOid)
                .map(spEntity -> ServiceProviderInfo.builder()
                        .oid(spEntity.getOid())
                        .name(spEntity.getDisplayName())
                        .build());
    }

    /**
     * A factory class for producing {@link AnalysisCoverageTopology} instances.
     */
    public static class AnalysisCoverageTopologyFactory {

        private final IdentityProvider identityProvider;

        private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

        /**
         * Constructs a new factory instance.
         * @param identityProvider The {@link IdentityProvider}, used to assign IDs to {@link AggregateCloudTierDemand}.
         * @param computeTierFamilyResolverFactory A factory class for creating {@link ComputeTierFamilyResolver}
         *                                         instances.
         */
        public AnalysisCoverageTopologyFactory(@Nonnull IdentityProvider identityProvider,
                                               @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory) {
            this.identityProvider = Objects.requireNonNull(identityProvider);
            this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
        }

        /**
         * Creates a new {@link AnalysisCoverageTopology} instance.
         * @param cloudTierTopology The cloud tier topology.
         * @param aggregatedDemandSet The {@link AggregateCloudTierDemand} set, in which each instance
         *                           will be represented as a coverage entity through this topology.
         * @param commitmentAggregateSet The set of cloud commitment aggregates to include in this topology.
         * @param commitmentCapacityById The commitment capacity by commitment ID.
         * @return The newly created {@link AnalysisCoverageTopology} instance.
         */
        @Nonnull
        public AnalysisCoverageTopology newTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                                    @Nonnull Collection<AggregateCloudTierDemand> aggregatedDemandSet,
                                                    @Nonnull Set<CloudCommitmentAggregate> commitmentAggregateSet,
                                                    @Nonnull Map<Long, CloudCommitmentAmount> commitmentCapacityById) {
            // Assign an ID to each aggregate demand instance
            final Map<Long, AggregateCloudTierDemand> aggregateDemandById = aggregatedDemandSet.stream()
                    .collect(ImmutableMap.toImmutableMap(
                            (demand) -> identityProvider.next(),
                            Function.identity()));

            return new AnalysisCoverageTopology(
                    cloudTierTopology,
                    computeTierFamilyResolverFactory.createResolver(cloudTierTopology),
                    aggregateDemandById,
                    commitmentAggregateSet,
                    commitmentCapacityById);
        }

    }
}
