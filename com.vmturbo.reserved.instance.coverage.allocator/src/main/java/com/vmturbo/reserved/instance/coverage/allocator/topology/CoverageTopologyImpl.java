package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
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
 * {@link TopologyEntityDTO} instances. Provides an implemenation for all analogous methods
 * relating to {@link ReservedInstanceBought} instances
 */
public class CoverageTopologyImpl implements CoverageTopology {

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final ThinTargetCache targetCache;

    private final Map<Long, ReservedInstanceSpec> reservedInstanceSpecsById;

    private final Map<Long, ReservedInstanceBought> reservedInstancesById;

    /**
     * Construct a new instance of {@link CoverageTopology}
     * @param cloudTopology The {@link CloudTopology} to wrap
     * @param targetCache The target cache, used to resolve the CSP type of business accounts
     * @param reservedInstanceSpecs The {@link ReservedInstanceSpec} instances that are part of this
     *                              topology
     * @param reservedInstances The {@link ReservedInstanceBought} instances that are part of this topology
     */
    public CoverageTopologyImpl(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                @Nonnull ThinTargetCache targetCache,
                                @Nonnull Collection<ReservedInstanceSpec> reservedInstanceSpecs,
                                @Nonnull Collection<ReservedInstanceBought> reservedInstances) {

        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.targetCache = Objects.requireNonNull(targetCache);
        this.reservedInstanceSpecsById = Objects.requireNonNull(reservedInstanceSpecs).stream()
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceSpec::getId,
                        Function.identity()));
        this.reservedInstancesById = Objects.requireNonNull(reservedInstances).stream()
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceBought::getId,
                        Function.identity()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, ReservedInstanceBought> getAllReservedInstances() {
        return reservedInstancesById;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getProviderTier(final long entityOid) {
        return getEntity(entityOid).map(entity -> {
                    switch (entity.getEntityType()) {
                        case EntityType.VIRTUAL_MACHINE_VALUE:
                            return getComputeTier(entityOid).orElse(null);
                        case EntityType.DATABASE_VALUE:
                            return getDatabaseTier(entityOid).orElse(null);
                        case EntityType.DATABASE_SERVER_TIER_VALUE:
                            return getDatabaseServerTier(entityOid).orElse(null);
                        default:
                            throw new UnsupportedOperationException();
                    }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getReservedInstanceProviderTier(long riOid) {
        return getSpecForReservedInstance(riOid)
                .flatMap(riSpec -> getEntity(riSpec.getReservedInstanceSpecInfo().getTierId()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<ReservedInstanceSpec> getSpecForReservedInstance(final long oid) {
        return getReservedInstanceBought(oid)
                .map(riBought -> reservedInstanceSpecsById.get(
                        riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<ReservedInstanceBought> getReservedInstanceBought(final long oid) {
        return Optional.ofNullable(reservedInstancesById.get(oid));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getReservedInstanceOwner(final long riOid) {
        return getReservedInstanceBought(riOid)
                .flatMap(ri -> getEntity(ri.getReservedInstanceBoughtInfo()
                        .getBusinessAccountId()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getReservedInstanceRegion(final long riOid) {
        return getSpecForReservedInstance(riOid)
                .map(ReservedInstanceSpec::getReservedInstanceSpecInfo)
                .filter(ReservedInstanceSpecInfo::hasRegionId)
                .flatMap(riSpecInfo -> cloudTopology.getEntity(riSpecInfo.getRegionId()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, TopologyEntityDTO> getEntities() {
        return cloudTopology.getEntities();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getEntity(final long entityId) {
        return cloudTopology.getEntity(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getComputeTier(final long entityId) {
        return cloudTopology.getComputeTier(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getDatabaseTier(final long entityId) {
        return cloudTopology.getDatabaseTier(entityId);
    }

    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getDatabaseServerTier(final long entityId) {
        return cloudTopology.getDatabaseServerTier(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getStorageTier(final long entityId) {
        return cloudTopology.getStorageTier(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Collection<TopologyEntityDTO> getConnectedVolumes(final long entityId) {
        return cloudTopology.getConnectedVolumes(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getConnectedRegion(final long entityId) {
        return cloudTopology.getConnectedRegion(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getPrimaryTier(final long entityId) {
        return cloudTopology.getPrimaryTier(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getReservedInstanceAvailabilityZone(final long riOid) {
        return getReservedInstanceBought(riOid)
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .filter(ReservedInstanceBoughtInfo::hasAvailabilityZoneId)
                .flatMap(riInfo -> cloudTopology
                        .getEntity(riInfo.getAvailabilityZoneId()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, Long> getReservedInstanceCapacityByOid() {
        return reservedInstancesById.values().stream()
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceBought::getId,
                        ri -> (long)ri.getReservedInstanceBoughtInfo()
                                .getReservedInstanceBoughtCoupons()
                                .getNumberOfCoupons()));
    }

    @Override
    public Set<SDKProbeType> getProbeTypesForEntity(final long entityOid) {
        return getEntity(entityOid)
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
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getConnectedAvailabilityZone(final long entityId) {
        return cloudTopology.getConnectedAvailabilityZone(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getOwner(final long entityId) {
        return cloudTopology.getOwner(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getConnectedService(final long entityId) {
        return cloudTopology.getConnectedService(entityId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return cloudTopology.size();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TopologyEntityDTO> getAllRegions() {
        return cloudTopology.getAllRegions();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TopologyEntityDTO> getAllEntitiesOfType(final int entityType) {
        return cloudTopology.getAllEntitiesOfType(entityType);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TopologyEntityDTO> getAllEntitiesOfType(final Set<Integer> entityTypes) {
        return cloudTopology.getAllEntitiesOfType(entityTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getRICoverageCapacityForEntity(final long entityId) {
        return cloudTopology.getRICoverageCapacityForEntity(entityId);
    }

    /**
     * Returns the billing family for the entity and its member.
     *
     * @return the the billing family for the entity and its member.
     */
    @Nonnull
    @Override
    public Optional<GroupAndMembers> getBillingFamilyForEntity(final long entityId) {
        return cloudTopology.getBillingFamilyForEntity(entityId);
    }

    @Nonnull
    @Override
    public Optional<TopologyEntityDTO> getServiceProvider(long entityId) {
        return cloudTopology.getServiceProvider(entityId);
    }
}
