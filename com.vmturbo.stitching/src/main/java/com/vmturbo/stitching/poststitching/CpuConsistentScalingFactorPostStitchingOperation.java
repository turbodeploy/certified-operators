package com.vmturbo.stitching.poststitching;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_REQUEST_QUOTA_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE;

import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.NamespaceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfoOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * IMPORTANT: This operation should be run AFTER the {@link CpuScalingFactorPostStitchingOperation}
 * because it relies on the regular scalingFactor to be set to its final value in order
 * to compute the correct value for the consistentScalingFactor.
 * <p/>
 * The consistent scaling factor is used in the market on entities that consistently
 * scale. It converts values on commodities from the normalized units that the market
 * uses for computations back into the unit that commodities need to be consistent in
 * in the real world. For example, on containers in a scaling group that are required
 * to have a VCPU capacity consistent in millicores, the consistent_scaling_factor
 * can be used to convert VCPU capacities, used values, etc. from the normalized MHz
 * values that the market uses into millicores. This is important to ensure the market
 * generates actions that make sense in the unit the consistent scaling group must be
 * consistent in.
 * <p/>
 * Unlike the CpuScalingFactorPostStitchingOperation, the ConsistentScalingFactor is not
 * propagated here, but it is propagated later by the EphemeralEntityEditor.
 * <p/>
 * Note that we do not set the CSF on entities like Containers here. That is done later in
 * the pipeline ({@see EphemeralEntityEditor}). We never set the CSF on pods because all
 * commodities they sell that participate in consistent scaling are actually resold from
 * other providers. Same with WorkloadControllers.
 **/
public abstract class CpuConsistentScalingFactorPostStitchingOperation implements PostStitchingOperation {

    private static final float DEFAULT_CONSISTENT_SCALING_FACTOR = AnalysisSettings.newBuilder()
        .getConsistentScalingFactor();

    private final boolean enableConsistentScalingOnHeterogeneousProviders;

    /**
     * Create a new CpuConsistentScalingFactorPostStitchingOperation.
     *
     * @param enableConsistentScalingOnHeterogeneousProviders If disabled, this operation does nothing.
     */
    protected CpuConsistentScalingFactorPostStitchingOperation(final boolean enableConsistentScalingOnHeterogeneousProviders) {
        this.enableConsistentScalingOnHeterogeneousProviders = enableConsistentScalingOnHeterogeneousProviders;
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> vms,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        // the entities to operate on will all be VM's
        vms.forEach(vm -> {
            Optional<ConsistentScalingFactorSetter> csfSetter = getCsfData(vm);
            csfSetter.ifPresent(consistentScalingFactorData -> {
                // queue update to just this entity
                resultBuilder.queueUpdateEntityAlone(vm,
                    consistentScalingFactorData::setConsistentScalingFactor);
            });
        });
        return resultBuilder.build();
    }

    /**
     * Get a setter for setting the ConsistentScalingFactor on the entity.
     * Should return {@link Optional#empty()} if it does not make sense to set the CSF
     * on the entity.
     *
     * @param entity The entity whose CSF we might want to set.
     * @return A setter for setting the CSF on the entity, or {@link Optional#empty()}
     *         if it does not make sense to set the CSF on the entity.
     */
    @Nonnull
    protected Optional<ConsistentScalingFactorSetter> getCsfData(@Nonnull final TopologyEntity entity) {
        if (!enableConsistentScalingOnHeterogeneousProviders) {
            return Optional.empty();
        }

        final TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
        return getCfsCommoditySold(entityBuilder).flatMap(commSold ->
            getMillicorePerMHz(entityBuilder, commSold).map(millicorePerMhz -> {
                return e -> e.getTopologyEntityDtoBuilder()
                    .getAnalysisSettingsBuilder()
                    .setConsistentScalingFactor(millicorePerMhz.floatValue());
        }));
    }

    /**
     * Get the commodity sold used for calculating the consistent scaling factor.
     *
     * @param entityBuilder The entity builder for the entity whose CSF should be calculated.
     * @return An optional for the commodity sold used for calculating the consistent scaling factor.
     */
    protected abstract Optional<CommoditySoldDTO> getCfsCommoditySold(
        @Nonnull TopologyEntityDTO.Builder entityBuilder);

    /**
     * Get the millicore/mHz value for the commodity sold.
     *
     * @param entityBuilder The entity builder for the entity whose CSF should be calculated.
     * @param commSold the commodity sold used for calculating the consistent scaling factor.
     * @return An optional for the millicore/mhz for the consistent scaling factor.
     */
    protected abstract Optional<Double> getMillicorePerMHz(@Nonnull TopologyEntityDTO.Builder entityBuilder,
                                                           @Nonnull CommoditySoldDTO commSold);

    /**
     * Interface for setting consistent scaling factor.
     */
    @FunctionalInterface
    private interface ConsistentScalingFactorSetter {
        /**
         * Set the consistent scaling factor on the entity.
         *
         * @param entity The entity whose CSF should be set.
         */
        void setConsistentScalingFactor(@Nonnull TopologyEntity entity);
    }

    /**
     * Set the ConsistentScalingFactor for VMs discovered by cloud native targets.
     */
    public static class VirtualMachineConsistentScalingFactorPostStitchingOperation extends
        CpuConsistentScalingFactorPostStitchingOperation {

        /**
         * Create a new VirtualMachineConsistentScalingFactorPostStitchingOperation.
         *
         * @param enableConsistentScalingOnHeterogeneousProviders If disabled, this operation does nothing.
         */
        public VirtualMachineConsistentScalingFactorPostStitchingOperation(
            final boolean enableConsistentScalingOnHeterogeneousProviders) {
            super(enableConsistentScalingOnHeterogeneousProviders);
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            // We must set the consistent scaling factor on VM's discovered by cloud native probes.
            // TODO: Support consistent scaling for groups of on-prem VM's hosted by PM's with different
            // CPU speeds.
            return stitchingScopeFactory.probeCategoryEntityTypeScope(
                ProbeCategory.CLOUD_NATIVE, EntityType.VIRTUAL_MACHINE);
        }

        @Override
        protected Optional<CommoditySoldDTO> getCfsCommoditySold(
            @Nonnull final TopologyEntityDTO.Builder entityBuilder) {
            return entityBuilder.getCommoditySoldListList().stream()
                .filter(sold -> sold.getCommodityType().getType() == VCPU_VALUE)
                .findAny();
        }

        @Override
        protected Optional<Double> getMillicorePerMHz(
            @Nonnull final TopologyEntityDTO.Builder entityBuilder,
            @Nonnull final CommoditySoldDTO commSold) {
            if (entityBuilder.hasTypeSpecificInfo()
                && entityBuilder.getTypeSpecificInfo().hasVirtualMachine()) {
                final VirtualMachineInfo vmInfo = entityBuilder.getTypeSpecificInfo().getVirtualMachine();
                if (vmInfo.hasNumCpus()) {
                    return Optional.of(computeMillicoreConsistentScalingFactor(vmInfo, commSold));
                }
            }

            return Optional.empty();
        }
    }

    /**
     * Set the ConsistentScalingFactor for namespaces.
     */
    public static class NamespaceConsistentScalingFactorPostStitchingOperation extends
        CpuConsistentScalingFactorPostStitchingOperation {

        /**
         * Create a new NamespaceConsistentScalingFactorPostStitchingOperation.
         *
         * @param enableConsistentScalingOnHeterogeneousProviders If disabled, this operation does nothing.
         */
        public NamespaceConsistentScalingFactorPostStitchingOperation(
            final boolean enableConsistentScalingOnHeterogeneousProviders) {
            super(enableConsistentScalingOnHeterogeneousProviders);
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            // We must set the consistent scaling factor on namespaces
            // (for quota commodities).
            return stitchingScopeFactory.entityTypeScope(EntityType.NAMESPACE);
        }

        @Override
        protected Optional<CommoditySoldDTO> getCfsCommoditySold(@Nonnull Builder entityBuilder) {
            return entityBuilder.getCommoditySoldListList().stream()
                .filter(sold -> sold.getCommodityType().getType() == VCPU_LIMIT_QUOTA_VALUE
                    || sold.getCommodityType().getType() == VCPU_REQUEST_QUOTA_VALUE)
                .findAny();
        }

        @Override
        protected Optional<Double> getMillicorePerMHz(@Nonnull Builder entityBuilder,
                                                      @Nonnull CommoditySoldDTO commSold) {
            if (entityBuilder.hasTypeSpecificInfo()
                && entityBuilder.getTypeSpecificInfo().hasNamespace()) {
                final NamespaceInfo nsInfo = entityBuilder.getTypeSpecificInfo().getNamespace();
                final double normMHz = commSold.getScalingFactor() * nsInfo.getAverageNodeCpuFrequency();
                return Optional.of(1000.0 / normMHz);
            }

            return Optional.empty();
        }
    }

    /**
     * Compute the consistent scaling factor value for working in millicores for a VM.
     * The ConsistentScalingFactor (CSF) should be computed such that:
     * capacity in normalized MHz * CSF => millicores
     * <p/>
     * Note that the capacity in normalized MHz should take the commodity's scalingFactor into
     * account.
     * <p/>
     * If the CSF cannot be computed because, for example, the commodity capacity or scaling
     * factor is zero, we return the default CSF of 1.0.
     *
     * @param vmInfo The TypeSpecificInfo for the VM.
     * @param vcpu The VCPU sold by the VM.
     * @return the consistent scaling factor value for a VM.
     */
    @VisibleForTesting
    static double computeMillicoreConsistentScalingFactor(
        @Nonnull final VirtualMachineInfoOrBuilder vmInfo,
        @Nonnull final CommoditySoldDTOOrBuilder vcpu) {
        double denominator = vcpu.getCapacity() * vcpu.getScalingFactor();
        return denominator == 0
            ? DEFAULT_CONSISTENT_SCALING_FACTOR
            : (vmInfo.getNumCpus() * 1000) / denominator;
    }
}
