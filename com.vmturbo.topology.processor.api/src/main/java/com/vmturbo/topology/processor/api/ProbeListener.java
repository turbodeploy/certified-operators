package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

/**
 * Listener to receive target-related events.
 */
public interface ProbeListener {
    /**
     * Triggered when a probe registers with the topology processor.
     *
     * @param probe new probe registered.
     */
    default void onProbeRegistered(@Nonnull TopologyProcessorDTO.ProbeInfo probe) {}
}
