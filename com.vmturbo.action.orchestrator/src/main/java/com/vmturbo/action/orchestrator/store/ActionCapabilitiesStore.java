package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;

/**
 * Store to get action capabilities for probes
 */
public interface ActionCapabilitiesStore {

    /**
     * Returnes action capabilities of provided probes.
     *
     * @param probeIds provided probes
     * @return  action capabilities by id for all provided probes
     */
    @Nonnull
    Map<Long, List<ProbeActionCapability>> getCapabilitiesForProbes(@Nonnull Collection<Long> probeIds);
}
