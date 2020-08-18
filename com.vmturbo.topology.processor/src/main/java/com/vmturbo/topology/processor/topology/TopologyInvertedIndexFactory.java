package com.vmturbo.topology.processor.topology;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Factory class for {@link InvertedIndex} objects, mainly to allow injection for mocking/testing
 * purposes. It's quite difficult/labour-intensive to construct topologies where entities are
 * connected in the way that allows the {@link InvertedIndex} to work.
 */
public class TopologyInvertedIndexFactory {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create an {@link InvertedIndex} containing a subset of the topology.
     * The purpose of the inverted index is to allow finding "potential" placement destinations,
     * which can restrict the number of providers we need to add segmentation commodities to.
     *
     * @param topologyGraph The topology graph to use to look up entities to add to the index.
     * @param types The types to include in the index.
     * @return The {@link InvertedIndex}.
     */
    @Nonnull
    public InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> typeInvertedIndex(
            TopologyGraph<TopologyEntity> topologyGraph, Set<ApiEntityType> types) {
        Stopwatch indexCreation = Stopwatch.createStarted();
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> invertedIndex =
                new InvertedIndex<>(32, new TopologyInvertedIndexTranslator());
        types.forEach(type -> {
            topologyGraph.entitiesOfType(type.typeNumber()).forEach(invertedIndex::add);
        });
        indexCreation.stop();
        logger.debug("Creation took {}", indexCreation);
        return invertedIndex;
    }

}
