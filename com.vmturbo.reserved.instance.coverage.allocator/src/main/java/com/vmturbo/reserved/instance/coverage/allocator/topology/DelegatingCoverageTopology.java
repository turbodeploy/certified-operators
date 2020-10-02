package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * A wrapper implementation around {@link CloudTopology} for all methods directly related to
 * {@link TopologyEntityDTO} instances.
 */
public class DelegatingCoverageTopology implements CoverageTopology {

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final ThinTargetCache targetCache;

    private final Map<Long, CloudCommitmentAggregate> commitmentAggregatesMap;

    private final Map<Long, CloudAggregationInfo> aggregationInfoMap = new ConcurrentHashMap<>();

    private final Map<Long, CoverageEntityInfo> entityInfoMap = new ConcurrentHashMap<>();

    private final Map<Long, ComputeTierInfo> computeTierInfoMap = new ConcurrentHashMap<>();

    /**
     * Construct a new instance of {@link CoverageTopology}.
     * @param cloudTopology The {@link CloudTopology} to wrap
     * @param targetCache The target cache, used to resolve the CSP type of business accounts
     * @param commitmentAggregates The set of cloud commitment aggregates included within the topology.
     */
    public DelegatingCoverageTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                      @Nonnull ThinTargetCache targetCache,
                                      @Nonnull Set<CloudCommitmentAggregate> commitmentAggregates) {

        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.targetCache = Objects.requireNonNull(targetCache);
        this.commitmentAggregatesMap = commitmentAggregates.stream()
                .collect(ImmutableMap.toImmutableMap(
                        CloudCommitmentAggregate::aggregateId,
                        Function.identity()));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<Long> getEntitiesOfType(@Nonnull final int entityType) {
        return cloudTopology.getAllEntitiesOfType(entityType)
                .stream()
                .map(TopologyEntityDTO::getOid)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * {@inheritDoc}.
     */
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
        return commitmentAggregatesMap.values()
                .stream()
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
        // Assumes all commitments are reserved instances
        return commitmentAggregatesMap.values()
                .stream()
                .filter(CloudCommitmentAggregate::isReservedInstance)
                .map(CloudCommitmentAggregate::asReservedInstanceAggregate)
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceAggregate::aggregateId,
                        (aggregate) -> aggregate.commitments().stream()
                                .mapToDouble(riData -> riData.commitment()
                                        .getReservedInstanceBoughtInfo()
                                        .getReservedInstanceBoughtCoupons()
                                        .getNumberOfCoupons())
                                .reduce(0.0, Double::sum)));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<SDKProbeType> getProbeTypesForEntity(final long entityOid) {
        return cloudTopology.getEntity(entityOid)
                .filter(TopologyEntityDTO::hasOrigin)
                .filter(entity -> entity.getOrigin().hasDiscoveryOrigin())
                .map(entity -> entity.getOrigin()
                        .getDiscoveryOrigin()
                        .getDiscoveredTargetDataMap()
                        .keySet()
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
        return cloudTopology.getRICoverageCapacityForEntity(entityOid);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @Nonnull
    public Optional<CloudAggregationInfo> getAggregationInfo(long entityOid) {
        return Optional.ofNullable(
                aggregationInfoMap.computeIfAbsent(entityOid, oid -> {
                    final OptionalLong billingFamilyId = getBillingFamilyForEntity(entityOid);
                    final Optional<TopologyEntityDTO> account = cloudTopology.getOwner(entityOid);
                    final Optional<TopologyEntityDTO> region = cloudTopology.getConnectedRegion(entityOid);
                    final Optional<TopologyEntityDTO> zone = cloudTopology.getConnectedAvailabilityZone(entityOid);

                    if (account.isPresent() && region.isPresent()) {
                        return CloudAggregationInfo.builder()
                                .billingFamilyId(billingFamilyId)
                                .accountOid(account.get().getOid())
                                .regionOid(region.get().getOid())
                                .zoneOid(zone.map(TopologyEntityDTO::getOid)
                                        .map(OptionalLong::of)
                                        .orElse(OptionalLong.empty()))
                                .build();
                    } else {
                        return null;
                    }
                }));
    }

    /**
     * Gets the entity type for {@code entityOid}.
     * @param entityOid The target entity OID.
     * @return The entity type for the target entity. Will be empty if the entity is not found
     * within the topology.
     */
    public Optional<Integer> getEntityType(long entityOid) {
        return cloudTopology.getEntity(entityOid)
                .map(TopologyEntityDTO::getEntityType);
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<CoverageEntityInfo> getEntityInfo(final long entityOid) {
        return Optional.ofNullable(
                entityInfoMap.computeIfAbsent(entityOid, oid ->
                    cloudTopology.getEntity(entityOid).map(entity -> {
                        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                            final TopologyDTO.TypeSpecificInfo.VirtualMachineInfo vmInfo =
                                    entity.getTypeSpecificInfo().getVirtualMachine();

                            return VirtualMachineInfo.builder()
                                    .entityState(entity.getEntityState())
                                    .platform(vmInfo.getGuestOsInfo().getGuestOsType())
                                    .tenancy(vmInfo.getTenancy())
                                    .build();
                        } else {
                            return null;
                        }
                    }).orElse(null)));
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Optional<ComputeTierInfo> getComputeTierInfoForEntity(final long entityOid) {
        return Optional.ofNullable(
                computeTierInfoMap.computeIfAbsent(entityOid, oid ->
                        cloudTopology.getComputeTier(entityOid)
                                .map(computeTier -> {
                                    final TopologyDTO.TypeSpecificInfo.ComputeTierInfo tierInfo =
                                            computeTier.getTypeSpecificInfo().getComputeTier();
                                    return ComputeTierInfo.builder()
                                            .family(tierInfo.hasFamily()
                                                    ? Optional.of(tierInfo.getFamily())
                                                    : Optional.empty())
                                            .tierOid(computeTier.getOid())
                                            .build();
                                }).orElse(null)));
    }

    /**
     * Returns the billing family ID for the entity.
     * @param entityId The target entity ID.
     * @return the the billing family ID for the entity.
     */
    public OptionalLong getBillingFamilyForEntity(final long entityId) {
        return cloudTopology.getBillingFamilyForEntity(entityId)
                .map(GroupAndMembers::group)
                .map(Grouping::getId)
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }
}
