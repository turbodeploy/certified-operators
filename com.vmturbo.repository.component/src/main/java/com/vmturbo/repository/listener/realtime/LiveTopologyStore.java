package com.vmturbo.repository.listener.realtime;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

public class LiveTopologyStore {
    private static final Logger logger = LogManager.getLogger();

    private final Object topologyLock = new Object();

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    /**
     * Create a {@link LiveTopologyStore}.
     *
     * @param globalSupplyChainCalculator a global supply chain calculator
     */
    public LiveTopologyStore(@Nonnull GlobalSupplyChainCalculator globalSupplyChainCalculator) {
        this.globalSupplyChainCalculator = globalSupplyChainCalculator;
    }

    @GuardedBy("topologyLock")
    private Optional<SourceRealtimeTopology> realtimeTopology = Optional.empty();

    @GuardedBy("topologyLock")
    private Optional<ProjectedRealtimeTopology> projectedTopology = Optional.empty();

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
                                                 this::updateRealtimeSourceTopology,
                                                 globalSupplyChainCalculator);
    }
}
