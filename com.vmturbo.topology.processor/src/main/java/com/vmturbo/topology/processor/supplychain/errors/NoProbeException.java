package com.vmturbo.topology.processor.supplychain.errors;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * A probe cannot be found.
 */
public class NoProbeException extends SupplyChainValidationException {
    /**
     * A probe with a specific id cannot be found.
     *
     * @param entity  entity during the verification of which the problem was discovered.
     * @param probeId id of the missing probe.
     */
    public NoProbeException(@Nonnull TopologyEntity entity, long probeId) {
        super(Long.toString(probeId), null, entity.getDisplayName(), "Probe not found");
    }
}
