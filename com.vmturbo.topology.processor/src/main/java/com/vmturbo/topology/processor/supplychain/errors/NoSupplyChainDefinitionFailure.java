package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.stitching.TopologyEntity;

/**
 * We cannot get a supply chain definition from an existing probe.
 */
public class NoSupplyChainDefinitionFailure extends SupplyChainValidationFailure {
    /**
     * No supply chain definition found for the specified probe.
     *
     * @param entity entity during the verification of which the problem was discovered.
     * @param probe probe which failed to give a supply chain validation definition.
     */
    public NoSupplyChainDefinitionFailure(
            @Nonnull TopologyEntity entity, @Nonnull ProbeInfo probe) {
        super(probe.getProbeType(), null, entity.getDisplayName(), "Probe has no supply chain definition");
    }
}
