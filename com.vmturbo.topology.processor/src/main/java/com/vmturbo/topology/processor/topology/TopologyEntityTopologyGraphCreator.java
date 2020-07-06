package com.vmturbo.topology.processor.topology;

import java.util.Map;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

public class TopologyEntityTopologyGraphCreator {

    /**
     * Create a new topology graph from a map of OID -> {@link TopologyEntity.Builder}.
     * Note that the builders in the map should have their lastUpdatedTime set, but the consumer and provider
     * relationships should be empty because graph construction will create these relationships.
     *
     * Note that graph construction does not validate the consistency of the input - for example,
     * if an entity in the input is buying from an entity not in the input, no error will be
     * generated. This validation should occur earlier (ie in stitching).
     *
     * @param topologyBuilderMap A map of the topology as represented by builders for TopologyEntities,
     *                           The builders in the map should have their lastUpdatedTime set but their consumer
     *                           and provider relationships will be set up during graph construction.
     *                           The keys in the map are entity OIDs.
     */
    public static TopologyGraph<TopologyEntity> newGraph(@Nonnull final Map<Long, TopologyEntity.Builder> topologyBuilderMap) {
        return new TopologyGraphCreator<>(new Long2ObjectOpenHashMap<>(topologyBuilderMap)).build();
    }
}
