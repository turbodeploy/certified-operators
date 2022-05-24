package com.vmturbo.stitching.poststitching;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoView;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.utilities.CPUScalingFactorUpdater;
import com.vmturbo.stitching.utilities.CPUScalingFactorUpdater.CloudNativeCPUScalingFactorUpdater;

/**
 * Post stitching operation to set CPU scaling factor to corresponding entities to reflect the
 * CPU performance of underlying infrastructure for more reasonable action results from market analysis.
 * <p/>
 * Scaling factor will be used to convert CPU commodity values from MHz/millicore to normalized MHz
 * to be analyzed by market engine. For non cloud native entities, scaling factor will convert CPU
 * values from raw MHz to normalized MHz; and for cloud native entities, scaling factor will convert
 * CPU values from millicore to normalized MHz.
 **/
public abstract class CpuScalingFactorPostStitchingOperation implements PostStitchingOperation {

    protected CPUScalingFactorUpdater cpuScalingFactorUpdater;

    @Nonnull
    protected Stream<TopologyEntity> filterEntities(@Nonnull Stream<TopologyEntity> entities) {
        return entities;
    }

    @Nonnull
    abstract Optional<Double> getCpuScalingFactor(@Nonnull TopologyEntity entity);

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        Objects.requireNonNull(cpuScalingFactorUpdater);
        filterEntities(entities).forEach(entity -> getCpuScalingFactor(entity)
                .ifPresent(sf -> resultBuilder.queueUpdateEntityAlone(
                        entity, entityToUpdate -> cpuScalingFactorUpdater
                                .update(entityToUpdate, sf, new LongOpenHashSet()))));
        return resultBuilder.build();
    }

    /**
     * Set CPU scaling factor to entities based on hosts. The scaling factor will be used to convert
     * CPU commodity values from raw MHz to normalized MHz for market analysis.
     * <p/>
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
     */
    public static class HostCpuScalingFactorPostStitchingOperation extends CpuScalingFactorPostStitchingOperation {

        private final CpuCapacityStore cpuCapacityStore;

        /**
         * Create a new HostCpuScalingFactorPostStitchingOperation.
         *
         * @param cpuCapacityStore The CPU capacity store.
         */
        public HostCpuScalingFactorPostStitchingOperation(final CpuCapacityStore cpuCapacityStore) {
            this.cpuCapacityStore = cpuCapacityStore;
            this.cpuScalingFactorUpdater = new CPUScalingFactorUpdater();
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.multiEntityTypesScope(ImmutableList.of(EntityType.PHYSICAL_MACHINE));
        }

        @Nonnull
        @Override
        Optional<Double> getCpuScalingFactor(@Nonnull TopologyEntity pmEntity) {
            return getCpuModelOpt(pmEntity).flatMap(cpuCapacityStore::getScalingFactor);
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
            final TypeSpecificInfoView typeSpecificInfo = Objects.requireNonNull(entityToFetchFrom)
                .getTopologyEntityImpl().getTypeSpecificInfo();
            return typeSpecificInfo.hasPhysicalMachine() &&
                typeSpecificInfo.getPhysicalMachine().hasCpuModel()
                ? Optional.of(typeSpecificInfo.getPhysicalMachine().getCpuModel())
                : Optional.empty();
        }
    }

    /**
     * Operation to set CPU scaling factor to cloud native entities based on VMs discovered by cloud
     * native targets.
     * <p/>
     * Cloud native entities have CPU commodities in millicores discovered from kubeturbo probe and
     * market runs analysis on CPU related commodities in MHz. The scaling factor will be used to
     * convert CPU values from millicores to normalized MHz for market analysis by
     * scalingFactor * cpuSpeed (MHz/millicore).
     * <p/>
     * Note that this operation is specifically to update scaling factor to cloud native entities
     * in cloud or standalone container platform cluster. On-prem cloud native entities have scaling
     * factor updated in {@link HostCpuScalingFactorPostStitchingOperation} based on underlying hosts.
     */
    public static class CloudNativeVMCpuScalingFactorPostStitchingOperation extends CpuScalingFactorPostStitchingOperation {

        public CloudNativeVMCpuScalingFactorPostStitchingOperation() {
            this.cpuScalingFactorUpdater = new CloudNativeCPUScalingFactorUpdater();
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.CLOUD_NATIVE,
                EntityType.VIRTUAL_MACHINE);
        }

        @Nonnull
        @Override
        protected Stream<TopologyEntity> filterEntities(@Nonnull final Stream<TopologyEntity> vmEntities) {
            // The entities to operate on will be VMs discovered by cloud native probe except
            // on-prem ones because those VMs already have scalingFactor updated in previous
            // HostCpuScalingFactorPostStitchingOperation based on underlying hosts.
            // For entities discovered from both on-prem and cloud targets, if not discovered from
            // only app or container targets, the environment type will be on-prem set up in
            // EnvironmentTypeInjector.
            return vmEntities.filter(vmEntity -> vmEntity.getEnvironmentType() != EnvironmentType.ON_PREM);
        }

        /**
         * Get scalingFactor from VM to make sure cloud native entities in cloud or standalone container
         * platform cluster will have proper normalized scalingFactor set up to CPU commodities.
         * TODO: Always return default value as 1 for now. Will update scalingFactor calculation for
         *  cloud VMs to improve container pod move recommendation in a heterogeneous cloud cluster
         *  in OM-68739.
         *
         * @return ScalingFactor from VM.
         */
        @Nonnull
        @Override
        Optional<Double> getCpuScalingFactor(@Nonnull TopologyEntity vmEntity) {
            return Optional.of(1d);
        }
    }
}
