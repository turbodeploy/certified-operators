package com.vmturbo.topology.processor.topology;

import java.util.HashMap;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A cache of consistent scaling values. It will also set the ConsistentScalingFactor on the
 * AnalysisSettings of the nodes (VMs) hosting containers.
 * <p/>
 * A new cache is created each time the editor is run (once per pipeline).
 */
public class ConsistentScalingCache {
    private final HashMap<TopologyEntity, Float> cache;

    /**
     * Create a new {@link ConsistentScalingCache}.
     */
    public ConsistentScalingCache() {
        cache = new HashMap<>();
    }

    /**
     * Lookup the consistent scaling factor to use for a particular container entity.
     * If the node (VM) entity hosting this entity does not already have a consistent scaling
     * factor,
     * set the value on the node as well.
     * <p/>
     * When multiplying (VCPU capacity * scalingFactor) on the container by the CSF should yield
     * the VCPU capacity of the container in millicores.
     *
     * @param container The container entity whose CSF was calculated.
     * @return The consistent scaling factor for the container.
     */
    public float lookupConsistentScalingFactor(@Nonnull final TopologyEntity container) {
        return getVM(container)
                .map(vm -> cache.computeIfAbsent(vm, this::getConsistentScalingFactor))
                .orElse(EphemeralEntityEditor.DEFAULT_CONSISTENT_SCALING_FACTOR);
    }

    @Nonnull
    private Float getConsistentScalingFactor(@Nonnull final TopologyEntity vm) {
        final AnalysisSettingsImpl analysisSettingsBuilder =
                vm.getTopologyEntityImpl().getOrCreateAnalysisSettings();
        // The CSF on the VM's analysis settings should have been set in PostStitching by
        // {@link VirtualMachineConsistentScalingFactorPostStitchingOperation}.
        return analysisSettingsBuilder.getConsistentScalingFactor();
    }

    @Nonnull
    private Optional<TopologyEntity> getVM(@Nonnull final TopologyEntity container) {
        return container.getProviders().stream().filter(
                containerProvider -> containerProvider.getEntityType()
                        == EntityType.CONTAINER_POD_VALUE).flatMap(pod -> pod.getProviders()
                .stream()
                .filter(podProvider -> podProvider.getEntityType()
                        == EntityType.VIRTUAL_MACHINE_VALUE)).findAny();
    }
}
