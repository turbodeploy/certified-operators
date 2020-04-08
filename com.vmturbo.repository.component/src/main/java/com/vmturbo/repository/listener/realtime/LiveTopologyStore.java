package com.vmturbo.repository.listener.realtime;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

public class LiveTopologyStore {
    private static final Logger logger = LogManager.getLogger();

    private final Object topologyLock = new Object();

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    /**
     * Map containing for each topology id, a list of entityDTOs that have changed state. The
     * keys are incrementally sorted, so that when we apply the state changes, we apply them in
     * increasing order by topologyId.
     */
    private final Map<Long, List<TopologyEntityDTO>> entitiesWithUpdatedState = new TreeMap<>();

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
        long sourceTopologyId =  newSourceRealtimeTopology.topologyInfo().getTopologyId();
        logger.info("Updated realtime topology with id {}. New topology has {} entities.",
            sourceTopologyId, newSourceRealtimeTopology.size());
        entitiesWithUpdatedState.entrySet().removeIf(entry -> (
            // The topologyId and the stateChangeId are created incrementally from the same
            // identity function in the topology processor. Here we want to remove all the cached
            // entityDTOs that were created before the current topology, and we can do that by
            // comparing the two ids.
            sourceTopologyId >= entry.getKey()));
        for (List<TopologyEntityDTO> entityDTOList : entitiesWithUpdatedState.values()) {
            entityDTOList.forEach(this::updateEntityWithNewState);
        }
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

    /**
     * Cache the EntitiesWithNewState, so that it can be reapplied to incoming topologies that were
     * created before the entity updated its state.
     *
     * @param entitiesWithNewState message containing the new state
     */
    public void setEntityWithUpdatedState(EntitiesWithNewState entitiesWithNewState) {
        for (TopologyEntityDTO entityDTO : entitiesWithNewState.getTopologyEntityList()) {
            this.entitiesWithUpdatedState.put(entitiesWithNewState.getStateChangeId(),
                entitiesWithNewState.getTopologyEntityList());
            updateEntityWithNewState(entityDTO);
        }
    }

    /**
     * Update an entity with a new state.
     *
     * @param entityDTO the new entityDTO
     */
    protected void updateEntityWithNewState(TopologyEntityDTO entityDTO) {
        getSourceTopology()
            .map(SourceRealtimeTopology::entityGraph)
            .flatMap(graph -> graph.getEntity(entityDTO.getOid()))
            .ifPresent(hostGraphEntity -> {
                hostGraphEntity.setEntityState(entityDTO.getEntityState());
            });
    }
}
