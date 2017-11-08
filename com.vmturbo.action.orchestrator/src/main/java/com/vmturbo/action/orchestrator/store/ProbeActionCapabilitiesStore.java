package com.vmturbo.action.orchestrator.store;

import java.awt.font.TextHitInfo;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.Probe.ListProbeActionCapabilitiesRequest;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapabilities;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;

/**
 * Store to get action capabilities for probes
 */
public class ProbeActionCapabilitiesStore implements ActionCapabilitiesStore {

    private final Logger logger = LogManager.getLogger();

    private final ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesBlockingStub;

    /**
     * Creates Store to get action capabilities for probes.
     *
     * @param actionCapabilitiesBlockingStub action capabilities grpc service
     */
    public ProbeActionCapabilitiesStore(
            @Nonnull ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesBlockingStub) {
        this.actionCapabilitiesBlockingStub = Objects.requireNonNull(actionCapabilitiesBlockingStub);
    }

    /**
     * Returnes action capabilities of provided probes.
     *
     * @param probeIds provided probes
     * @return  action capabilities by id for all provided probes
     */
    @Nonnull
    public Map<Long, List<ProbeActionCapability>> getCapabilitiesForProbes(@Nonnull Set<Long> probeIds) {
        final Iterator<ProbeActionCapabilities> actionCapabilitiesIterator =
                actionCapabilitiesBlockingStub.listProbeActionCapabilities(
                        ListProbeActionCapabilitiesRequest.newBuilder()
                                .addAllProbeIds(probeIds)
                                .build());
        final Map<Long, List<ProbeActionCapability>> probesCapabilities = new HashMap<>();
        actionCapabilitiesIterator.forEachRemaining(capabilitiesOfProbe -> {
            if (!capabilitiesOfProbe.hasActionCapabilitiesList()) {
                logger.warn("Cannot resolve action capabilities for probe {}",
                        capabilitiesOfProbe.getProbeId());
            } else {
                probesCapabilities.put(capabilitiesOfProbe.getProbeId(),
                        capabilitiesOfProbe.getActionCapabilitiesList()
                                .getActionCapabilitiesList());
            }
        });
        return probesCapabilities;
    }
}
