package com.vmturbo.stitching.poststitching;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE;

import java.util.Collection;
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
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(entity ->
                resultBuilder.queueUpdateEntityAlone(entity, entityToUpdate -> {
                    getCpuModelOpt(entityToUpdate).ifPresent(cpuModel -> {
                        cpuCapacityStore.getScalingFactor(cpuModel)
                                .ifPresent(scalingFactor -> {
                                    updateScalingFactorForPm(entityToUpdate, scalingFactor);
                                });
                    });
                }));
        return resultBuilder.build();
    }

    /**
     * Set the scalingFactor for the given PM entity and all the VMs that buy from it.
     *
     * @param pmEntityToUpdate the TopologyEntity, a PM, to update.
     * @param scalingFactor the scalingFactor to set on the PM commodities to which it should apply
     */
    private void updateScalingFactorForPm(@Nonnull final TopologyEntity pmEntityToUpdate,
                                          @Nonnull final Double scalingFactor) {
        // update the scaling factor for the PM itself
        updateScalingFactorForEntity(pmEntityToUpdate, scalingFactor);
        // update the scaling factor for an VMs which buy from this PM
        pmEntityToUpdate.getConsumers().stream()
                .filter(consumerEntity -> consumerEntity.getEntityType() ==
                        EntityType.VIRTUAL_MACHINE_VALUE)
                .forEach(vmEntity -> {
                    // add the scalingFactor to the VCPU commodity for the VM
                    updateScalingFactorForEntity(vmEntity, scalingFactor);
                });
    }

    /**
     * Update the 'scalingFactor' for any commoditiesSold of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE). Note that this applies to both PMs and
     * VMs - the items in COMMODITIES_TO_SCALE include selected commodities from either.
     * The set COMMODITIES_TO_SCALE is so small having both sets of commodities is not
     * a performance problem.
     *
     * @param entityToUpdate a TopologyEntity for which we should check for commodities to scale
     * @param scalingFactor the scaling factor to apply to any commodities that should scale
     */
    private void updateScalingFactorForEntity(@Nonnull final TopologyEntity entityToUpdate,
                                              final double scalingFactor) {
        Objects.requireNonNull(entityToUpdate);
        entityToUpdate.getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().stream()
                .filter(this::commodityTypeShouldScale)
                .forEach(commSold -> commSold.setScalingFactor(scalingFactor));
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
    private Optional<String> getCpuModelOpt(final TopologyEntity entityToFetchFrom) {
        final TypeSpecificInfo typeSpecificInfo = entityToFetchFrom.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo();
        return typeSpecificInfo.hasPhysicalMachine() &&
                typeSpecificInfo.getPhysicalMachine().hasCpuModel()
                ? Optional.of(typeSpecificInfo.getPhysicalMachine().getCpuModel())
                : Optional.empty();
    }

    /**
     * Check to see if the given Commodity is of the type to which 'scaleFactor' applies. Currently
     * this includes PM CommoditySold for CPU, CPU_PROVISIONED, and VCPU sold by VM.
     *
     * @param commodity the commodity to test
     * @return true iff this commodity should have a 'scaleFactor' applied, if one is found
     */
    private boolean commodityTypeShouldScale(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return COMMODITIES_TO_SCALE.contains(commodity.getCommodityType().getType());
    }
}
