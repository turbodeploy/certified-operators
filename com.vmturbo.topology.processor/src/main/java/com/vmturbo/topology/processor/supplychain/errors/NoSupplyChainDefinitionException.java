package com.vmturbo.topology.processor.supplychain.errors;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * We cannot get a supply chain definition from an existing probe.
 */
public class NoSupplyChainDefinitionException extends SupplyChainValidationException {
    /**
     * No supply chain definition found for the specified probe.
     *
     * @param entity entity during the verification of which the problem was discovered.
     * @param probe probe which failed to give a supply chain validation definition.
     */
    public NoSupplyChainDefinitionException(
          @Nonnull TopologyEntity entity, @Nonnull ProbeInfo probe) {
        super(probe.getProbeType(), null, entity.getDisplayName(), "Probe has no supply chain definition");
    }
}
