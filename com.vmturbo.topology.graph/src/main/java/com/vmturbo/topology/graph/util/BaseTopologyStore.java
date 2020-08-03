package com.vmturbo.topology.graph.util;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * A "base" class to store topologies and update them as new topologies come in.
 * The full hierarchy is:
 * {@link BaseTopologyStore} contains
 *     a {@link BaseTopology}, which is made up of
 *        {@link BaseGraphEntity}s.
 *  Every time a new topology comes in, use {@link BaseTopologyStore#newRealtimeSourceTopology(TopologyInfo)}
 *  to create a {@link com.vmturbo.topology.graph.util.BaseTopology.BaseTopologyBuilder} and
 *  feed the entities to the builder.
 *
 * @param <T> The type of topologies contained in the store.
 * @param <S> The {@link com.vmturbo.topology.graph.util.BaseTopology.BaseTopologyBuilder} for the topologies.
 * @param <E> The type of entities in the topologies.
 * @param <B> The type of builder for the entities.
 */
public abstract class BaseTopologyStore<T extends BaseTopology<E>, S extends BaseTopology.BaseTopologyBuilder<T, E, B>, E extends BaseGraphEntity<E>, B extends BaseGraphEntity.Builder<B, E>> {
    protected final Logger logger = LogManager.getLogger(getClass());

    protected final Object topologyLock = new Object();

    @GuardedBy("topologyLock")
    private Optional<T> realtimeTopology = Optional.empty();

    /**
     * Map containing for each topology id, a list of entityDTOs that have changed state. The
     * keys are incrementally sorted, so that when we apply the state changes, we apply them in
     * increasing order by topologyId.
     */
    private final Map<Long, List<TopologyEntityDTO>> entitiesWithUpdatedState = new TreeMap<>();

    private void updateRealtimeSourceTopology(@Nonnull final T newSourceRealtimeTopology) {
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

    /**
     * Get the current topology.
     *
     * @return An optional containing the topology. Empty optional if no topology has come in yet.
     */
    @Nonnull
    public Optional<T> getSourceTopology() {
        synchronized (topologyLock) {
            return realtimeTopology;
        }
    }

    protected abstract S newBuilder(TopologyInfo topologyInfo, Consumer<T> consumer);

    /**
     * Start the creation of a new topology, which, when finished, will replace the one that's
     * currently in the store.
     *
     * @param topologyInfo The {@link TopologyInfo}.
     * @return Builder for the topology.
     */
    public S newRealtimeSourceTopology(@Nonnull final TopologyInfo topologyInfo) {
        return newBuilder(topologyInfo, this::updateRealtimeSourceTopology);
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
    public void updateEntityWithNewState(TopologyEntityDTO entityDTO) {
        realtimeTopology.map(BaseTopology::entityGraph)
            .flatMap(graph -> graph.getEntity(entityDTO.getOid()))
            .ifPresent(hostGraphEntity -> {
                hostGraphEntity.setEntityState(entityDTO.getEntityState());
            });
    }

}
