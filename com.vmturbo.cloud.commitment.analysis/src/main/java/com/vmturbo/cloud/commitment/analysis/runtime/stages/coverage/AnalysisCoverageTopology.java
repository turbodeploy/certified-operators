package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CloudAggregationInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ComputeTierInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageEntityInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.VirtualMachineInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * A {@link CoverageTopology} implementation, built around {@link AggregateCloudTierDemand}.
 */
public class AnalysisCoverageTopology implements CoverageTopology {

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology;

    private final MinimalCloudTopology<MinimalEntity> cloudTopology;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private final Map<Long, AggregateCloudTierDemand> aggregatedDemandById;

    private final ThinTargetCache targetCache;

    private final Map<Long, CloudCommitmentAggregate> commitmentAggregatesMap;

    private final Map<Long, Double> commitmentCapacityById;


    private AnalysisCoverageTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                     @Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology,
                                     @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                     @Nonnull Map<Long, AggregateCloudTierDemand> aggregatedDemandById,
                                     @Nonnull ThinTargetCache targetCache,
                                     @Nonnull Set<CloudCommitmentAggregate> commitmentAggregateSet,
                                     @Nonnull Map<Long, Double> commitmentCapacityById) {

        this.cloudTierTopology = Objects.requireNonNull(cloudTierTopology);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.aggregatedDemandById = ImmutableMap.copyOf(Objects.requireNonNull(aggregatedDemandById));
        this.targetCache = Objects.requireNonNull(targetCache);
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
     * @param coverageAmount The coverage amount (generally in coupons).
     * @return The aggregate demand amount (generally in terms of hours of uptime).
     */
    public Triple<Long, Long, Double> convertAllocationDemandToAggregate(
            long aggregateId,
            long commitmentId,
            double coverageAmount) {

        if (aggregatedDemandById.containsKey(aggregateId)) {
            // assumes compute tier demand
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(aggregateId);

            // This will need to be abstracted when multiple commitment types are supported
            final Optional<Long> normalizationFactor = computeTierFamilyResolver.getNumCoupons(
                    aggregateDemand.cloudTierInfo().cloudTierDemand().cloudTierOid());

            // TODO(ejf) log error
            final double aggregateAmount = coverageAmount / normalizationFactor.orElse(1L);
            return ImmutableTriple.of(aggregateId, commitmentId, aggregateAmount);
        } else {
            // TODO(ejf) log error
            return ImmutableTriple.of(aggregateId, commitmentId, coverageAmount);
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

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Map<Long, Double> getCommitmentCapacityByOid() {
        return commitmentCapacityById;
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Set<SDKProbeType> getProbeTypesForEntity(final long entityOid) {

        final Optional<MinimalEntity> entity;
        if (aggregatedDemandById.containsKey(entityOid)) {

            // For aggregate demand, we look up the discovering target IDs for the associated
            // account.
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(entityOid);
            entity = cloudTopology.getEntity(aggregateDemand.cloudTierInfo().accountOid());
        } else {
            entity = cloudTopology.getEntity(entityOid);
        }

        return entity.map(e -> e.getDiscoveringTargetIdsList()
                .stream()
                .map(targetCache::getTargetInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(ThinTargetInfo::probeInfo)
                .map(ThinProbeInfo::type)
                .map(SDKProbeType::create)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()))
                .orElse(Collections.emptySet());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public double getCoverageCapacityForEntity(final long entityOid) {
        if (aggregatedDemandById.containsKey(entityOid)) {
            // assumes compute tier demand
            final AggregateCloudTierDemand aggregateDemand = aggregatedDemandById.get(entityOid);

            // This will need to be abstracted when multiple commitment types are supported
            final Optional<Long> normalizationFactor = computeTierFamilyResolver.getNumCoupons(
                    aggregateDemand.cloudTierInfo().cloudTierDemand().cloudTierOid());

            // TODO(ejf) log error
            return aggregateDemand.demandAmount() * normalizationFactor.orElse(1L);
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

    /**
     * A factory class for producing {@link AnalysisCoverageTopology} instances.
     */
    public static class AnalysisCoverageTopologyFactory {

        private final IdentityProvider identityProvider;

        private final ThinTargetCache thinTargetCache;

        private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

        /**
         * Constructs a new factory instance.
         * @param identityProvider The {@link IdentityProvider}, used to assign IDs to {@link AggregateCloudTierDemand}.
         * @param thinTargetCache The {@link ThinTargetCache}, used to determine the probe type of entities.
         * @param computeTierFamilyResolverFactory A factory class for creating {@link ComputeTierFamilyResolver}
         *                                         instances.
         */
        public AnalysisCoverageTopologyFactory(@Nonnull IdentityProvider identityProvider,
                                               @Nonnull ThinTargetCache thinTargetCache,
                                               @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory) {
            this.identityProvider = Objects.requireNonNull(identityProvider);
            this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
            this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
        }

        /**
         * Creates a new {@link AnalysisCoverageTopology} instance.
         * @param cloudTierTopology The cloud tier topology.
         * @param cloudTopology The cloud topology.
         * @param aggregatedDemandSet The {@link AggregateCloudTierDemand} set, in which each instance
         *                           will be represented as a coverage entity through this topology.
         * @param commitmentAggregateSet The set of cloud commitment aggregates to include in this topology.
         * @param commitmentCapacityById The commitment capacity by commitment ID.
         * @return The newly created {@link AnalysisCoverageTopology} instance.
         */
        @Nonnull
        public AnalysisCoverageTopology newTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                                    @Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology,
                                                    @Nonnull Collection<AggregateCloudTierDemand> aggregatedDemandSet,
                                                    @Nonnull Set<CloudCommitmentAggregate> commitmentAggregateSet,
                                                    @Nonnull Map<Long, Double> commitmentCapacityById) {
            // Assign an ID to each aggregate demand instance
            final Map<Long, AggregateCloudTierDemand> aggregateDemandById = aggregatedDemandSet.stream()
                    .collect(ImmutableMap.toImmutableMap(
                            (demand) -> identityProvider.next(),
                            Function.identity()));

            return new AnalysisCoverageTopology(
                    cloudTierTopology,
                    cloudTopology,
                    computeTierFamilyResolverFactory.createResolver(cloudTierTopology),
                    aggregateDemandById,
                    thinTargetCache,
                    commitmentAggregateSet,
                    commitmentCapacityById);
        }

    }
}
