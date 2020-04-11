package com.vmturbo.stitching.poststitching;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;

/**
 * For any PhysicalMachine with the 'cpuModel' value set, look up the
 * 'scalingFactor' by calling the CpuCapacity RPC service for that 'cpuModel'.
 * Store the 'scalingFactor' returned 'CPU' sold commodity; the 'CPUProvisioned' commodity;
 * and for each VM that buys from this PM, store the 'scalingFactor' on the 'VCPU'
 * commodity for that VM.
 * <p/>
 * In order avoid a round trip to the CpuCapacity RPC service for each PM, we build a
 * multimap of 'cpuModel' -> PMs with that CPU model, and then make a single request
 * to the CpuCapacity RPC service to fetch all of the 'scalingFactor's for all the 'cpuModel's
 * in one call. Then we insert each returned 'scalingFactor' in the appropriate PM (and VM)
 * commodities in the result.
 **/
public class CpuScalingFactorPostStitchingOperation implements PostStitchingOperation {

    /**
     * Update the 'CPU' sold
     */
    private static final Collection<Integer> COMMODITIES_TO_SCALE =
            ImmutableSet.of(CPU_VALUE, CPU_PROVISIONED_VALUE, VCPU_VALUE);
    private final CpuCapacityStore cpuCapacityStore;

    public CpuScalingFactorPostStitchingOperation(final CpuCapacityStore cpuCapacityStore) {
        this.cpuCapacityStore = cpuCapacityStore;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> pmEntities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        // the entities to operate on will all be PM's
        pmEntities.forEach(pmEntity ->
                // queue update to just this entity
                resultBuilder.queueUpdateEntityAlone(pmEntity, entityToUpdate ->
                        // check if there's a CPU Model specified
                        getCpuModelOpt(entityToUpdate).ifPresent(cpuModel ->
                                // if so, fetch the scalingFactor (if any) for that CPU Model
                                cpuCapacityStore.getScalingFactor(cpuModel)
                                        // if found, update the commodities for this entity
                                        .ifPresent(scalingFactor ->
                                        updateScalingFactorForEntity(entityToUpdate,
                                                        scalingFactor, new HashSet<Long>())))));
        return resultBuilder.build();
    }

    /**
     * Set the scalingFactor for the entity and all the consumers that buy from it.
     * The commodities to scale are sold by the entity given, and we update the
     * commodities for each entity that buys from it.
     *
     * @param entityToUpdate the TopologyEntity to update.
     * @param scalingFactor the scalingFactor to set on the entity.
     * @param updatedSet A set of oids of entities that have already been updated.
     */
    private void updateScalingFactorForEntity(@Nonnull final TopologyEntity entityToUpdate,
                                          @Nonnull final Double scalingFactor,
                                          @Nonnull HashSet<Long> updatedSet) {
        // Avoid potential for infinite recursion.
        if (updatedSet.contains(entityToUpdate.getOid())) {
            return;
        }

        updateScalingFactorForSoldCommodities(entityToUpdate, scalingFactor);
        updateScalingFactorForBoughtCommodities(entityToUpdate, scalingFactor);
        updatedSet.add(entityToUpdate.getOid());
        entityToUpdate.getConsumers().forEach(
                consumer -> updateScalingFactorForEntity(consumer, scalingFactor, updatedSet)
        );
    }

    /**
     * Update the 'scalingFactor' for any commoditiesSold of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE).
     *
     * @param entityToUpdate a TopologyEntity for which we should check for commodities to scale
     * @param scalingFactor the scaling factor to apply to any commodities that should scale
     */
    private void updateScalingFactorForSoldCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                       final double scalingFactor) {
        Objects.requireNonNull(entityToUpdate)
                .getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().stream()
                .filter(commoditySold -> commodityTypeShouldScale(commoditySold.getCommodityType()))
                .forEach(commSold -> commSold.setScalingFactor(scalingFactor));
    }

    /**
     * Update the 'scalingFactor' for any commoditiesBought of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE).
     *
     * @param entityToUpdate a TopologyEntity for which we should check for commodities to scale
     * @param scalingFactor the scaling factor to apply to any commodities that should scale
     */
    private void updateScalingFactorForBoughtCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                       final double scalingFactor) {
        Objects.requireNonNull(entityToUpdate)
                .getTopologyEntityDtoBuilder()
                .getCommoditiesBoughtFromProvidersBuilderList().stream()
                .flatMap(commoditiesBoughtFromProvider ->
                        commoditiesBoughtFromProvider.getCommodityBoughtBuilderList().stream())
                .filter(commodityBoughtFromProvider ->
                        commodityTypeShouldScale(commodityBoughtFromProvider.getCommodityType()))
                .forEach(commBought -> commBought.setScalingFactor(scalingFactor));
    }

    /**
     * Fetch the 'cpuModel' of a TopologyEntity. 'cpuModel' is a PM-only attribute, and so is
     * part of the type-specific info, PhysicalMachine oneof. If the TopologyEntity does not have a
     * PhysicalMachine TypeSpecificInfo, or the PhysicalMachine doesn't have a 'cpuModel', then
     * return Optional.empty().
     *
     * @param entityToFetchFrom the TopologyEntity from which we are going to check for the 'cpuModel'
     *                          field.
     * @return an Optional containing the 'cpuModel', or Optional.empty() if this isn't a PM, or
     * the PM doesn't have a 'cpuModel' field set.
     */
    private Optional<String> getCpuModelOpt(@Nonnull final TopologyEntity entityToFetchFrom) {
        final TypeSpecificInfo typeSpecificInfo = Objects.requireNonNull(entityToFetchFrom)
                .getTopologyEntityDtoBuilder().getTypeSpecificInfo();
        return typeSpecificInfo.hasPhysicalMachine() &&
                typeSpecificInfo.getPhysicalMachine().hasCpuModel()
                ? Optional.of(typeSpecificInfo.getPhysicalMachine().getCpuModel())
                : Optional.empty();
    }

    /**
     * Check to see if the given Commodity is of the type to which 'scaleFactor' applies. Currently
     * this includes PM CommoditySold for CPU, CPU_PROVISIONED.
     *
     * @param commodityType the commodityType to test
     * @return true iff this commodity should have a 'scaleFactor' applied, if one is found
     */
    private boolean commodityTypeShouldScale(@Nonnull TopologyDTO.CommodityType commodityType) {
        return COMMODITIES_TO_SCALE.contains(Objects.requireNonNull(commodityType).getType());
    }
}
