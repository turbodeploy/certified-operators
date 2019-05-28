package com.vmturbo.repository.listener.realtime;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;

public class LiveTopologyStore {
    private static final Logger logger = LogManager.getLogger();

    private final Object topologyLock = new Object();

    @GuardedBy("topologyLock")
    private Optional<SourceRealtimeTopology> realtimeTopology = Optional.empty();

    @GuardedBy("topologyLock")
    private Optional<ProjectedRealtimeTopology> projectedTopology = Optional.empty();

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    public LiveTopologyStore(@Nonnull final GlobalSupplyChainCalculator globalSupplyChainCalculator) {
        this.globalSupplyChainCalculator = globalSupplyChainCalculator;
    }

    private void updateRealtimeSourceTopology(@Nonnull final SourceRealtimeTopology newSourceRealtimeTopology) {
        synchronized (topologyLock) {
            this.realtimeTopology = Optional.of(newSourceRealtimeTopology);
        }
        logger.info("Updated realtime topology. New topology has {} entities.",
            newSourceRealtimeTopology.size());
    }

    private void updateRealtimeProjectedTopology(@Nonnull final ProjectedRealtimeTopology newProjectedRealtimeTopology) {
        synchronized (topologyLock) {
            this.projectedTopology = Optional.of(newProjectedRealtimeTopology);
        }
        logger.info("Updated realtime PROJECTED topology. New topology has {} entities.",
            newProjectedRealtimeTopology.size());
    }

    @Nonnull
    public Optional<SourceRealtimeTopology> getSourceTopology() {
        return realtimeTopology;
    }

    @Nonnull
    public Optional<ProjectedRealtimeTopology> getProjectedTopology() {
        return projectedTopology;
    }

    @Nonnull
    public ProjectedTopologyBuilder newProjectedTopology(final long topologyId,
                                                         @Nonnull final TopologyInfo originalTopologyInfo) {
        return new ProjectedTopologyBuilder(this::updateRealtimeProjectedTopology, topologyId, originalTopologyInfo);
    }

    public SourceRealtimeTopologyBuilder newRealtimeTopology(@Nonnull final TopologyInfo topologyInfo) {
        return new SourceRealtimeTopologyBuilder(topologyInfo,
            globalSupplyChainCalculator,
            this::updateRealtimeSourceTopology);
    }
}
