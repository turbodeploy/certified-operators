package com.vmturbo.topology.graph.util;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * A base topology class. The topology contains a particular topology graph, plus some utility
 * methods to interact with it.
 *
 * @param <T> The type of {@link BaseGraphEntity} in the topology.
 */
public class BaseTopology<T extends BaseGraphEntity<T>> {
    private final TopologyInfo topologyInfo;

    /**
     * The graph for the entities in the topology.
     */
    private final TopologyGraph<T> entityGraph;

    protected BaseTopology(TopologyInfo topologyInfo,
            TopologyGraph<T> entityGraph) {
        this.topologyInfo = topologyInfo;
        this.entityGraph = entityGraph;
    }

    /**
     * Get the topology info for this topology.
     *
     * @return The {@link TopologyInfo} for the topology this object represents.
     */
    @Nonnull
    public TopologyInfo topologyInfo() {
        return topologyInfo;
    }

    /**
     * Get the entity graph for this topology.
     *
     * @return The {@link TopologyGraph} for the entities in the topology.
     */
    @Nonnull
    public TopologyGraph<T> entityGraph() {
        return entityGraph;
    }


    /**
     * Get the number of entities in the topology.
     *
     * @return the size of the topology.
     */
    public int size() {
        return entityGraph.size();
    }

    /**
     * A builder for the {@link BaseTopology}. Takes care of adding elements to the underlying graph,
     * and calling the appropriate "completion" methods when there are no more entities to add.
     *
     * @param <T> The subtype of {@link BaseTopology} being built.
     * @param <E> The {@link BaseGraphEntity} in the topology.
     * @param <B> The {@link BaseGraphEntity.Builder} for the entities.
     */
    public abstract static class BaseTopologyBuilder<T extends BaseTopology<E>, E extends BaseGraphEntity<E>, B extends BaseGraphEntity.Builder<B, E>> {
        private final DataMetricSummary constructionTimeSummary;
        private static final Logger logger = LogManager.getLogger();

        private final TopologyInfo topologyInfo;
        private final Consumer<T> onFinish;
        private final TopologyGraphCreator<B, E> graphCreator =
                new TopologyGraphCreator<>();

        private final Stopwatch builderStopwatch = Stopwatch.createUnstarted();

        protected BaseTopologyBuilder(DataMetricSummary constructionTimeSummary,
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final Consumer<T> onFinish) {
            this.constructionTimeSummary = constructionTimeSummary;
            this.topologyInfo = topologyInfo;
            this.onFinish = onFinish;
        }

        /**
         * Add entities to the topology.
         *
         * @param entities The entity {@link TopologyEntityDTO}s.
         */
        public void addEntities(@Nonnull final Collection<TopologyEntityDTO> entities) {
            builderStopwatch.start();
            for (TopologyEntityDTO entity : entities) {
                graphCreator.addEntity(newBuilder(entity));
            }
            builderStopwatch.stop();
        }

        /**
         * Add a single entity to the topology.
         *
         * @param entity The entity {@link TopologyEntityDTO}.
         */
        public void addEntity(@Nonnull final TopologyEntityDTO entity) {
            builderStopwatch.start();
            graphCreator.addEntity(newBuilder(entity));
            builderStopwatch.stop();
        }

        protected abstract B newBuilder(TopologyEntityDTO entity);

        protected abstract T newTopology(TopologyInfo topologyInfo, TopologyGraph<E> graph);

        /**
         * Called when there are no more entities to add.
         *
         * @return The constructed topology.
         */
        @Nonnull
        public T finish() {
            builderStopwatch.start();
            final TopologyGraph<E> graph = graphCreator.build();

            final T sourceRealtimeTopology = newTopology(topologyInfo, graph);
            onFinish.accept(sourceRealtimeTopology);
            builderStopwatch.stop();

            final long elapsedSec = builderStopwatch.elapsed(TimeUnit.SECONDS);
            constructionTimeSummary.observe((double)elapsedSec);

            logger.info("Spent total of {}s to construct source realtime topology.", elapsedSec);

            return sourceRealtimeTopology;
        }

    }
}
