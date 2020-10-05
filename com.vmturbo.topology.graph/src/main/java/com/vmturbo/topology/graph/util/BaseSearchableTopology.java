package com.vmturbo.topology.graph.util;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * An extension of {@link BaseTopology} which supports {@link BaseGraphEntity}.
 *
 * <p>The main reason we use this is for the {@link BaseSearchableTopologyBuilder}</p>, which is
 * responsible for tracking tags and keeping a common compression buffer - which searchable
 * entities need. In the future we may also put methods here to streamline searches on a topology.
 *
 * @param <T> The subtype of {@link BaseGraphEntity} in the topology.
 */
public class BaseSearchableTopology<T extends BaseGraphEntity<T>> extends BaseTopology<T> {

    protected BaseSearchableTopology(TopologyInfo topologyInfo, TopologyGraph<T> entityGraph) {
        super(topologyInfo, entityGraph);
    }

    /**
     * The builder for the searchable topology.
     *
     * @param <T> The subtype of the {@link BaseSearchableTopology} built by the builder.
     * @param <E> The subtype of the {@link BaseGraphEntity} in the topology.
     * @param <B> The subtype of the {@link BaseGraphEntity.Builder} for entities in
     * the topology.
     */
    public abstract static class BaseSearchableTopologyBuilder<T extends BaseSearchableTopology<E>,
            E extends BaseGraphEntity<E>,
            B extends BaseGraphEntity.Builder<B, E>> extends BaseTopology.BaseTopologyBuilder<T, E, B> {

        /**
         * The size of the compression buffer in the most recently completed projected topology
         * builder. We assume that (in general) topologies stay roughly the same size once targets
         * are added, so we can use the last topology's buffer size to avoid unnecessary allocations
         * on the next one.
         *
         * <p/>Note - the buffer will be relavitely small - it is bounded by the largest
         * entity in the topology.
         */
        private static volatile int sharedBufferSize = 0;
        /**
         * Reuse the same intermediate compression buffer for the full topology.
         */
        private final SharedByteBuffer compressionBuffer;

        private final DefaultTagIndex defaultTagIndex = new DefaultTagIndex();

        protected BaseSearchableTopologyBuilder(DataMetricSummary constructionTimeSummary,
                @Nonnull TopologyInfo topologyInfo,
                @Nonnull Consumer<T> onFinish) {
            super(constructionTimeSummary, topologyInfo, onFinish);
            compressionBuffer = new SharedByteBuffer(sharedBufferSize);
        }

        @Override
        protected final B newBuilder(TopologyEntityDTO entity) {
            // Implementations shouldn't override this method, but should override the abstract one.
            defaultTagIndex.addTags(entity.getOid(), entity.getTags());
            return newBuilder(entity, defaultTagIndex, compressionBuffer);
        }

        protected abstract B newBuilder(TopologyEntityDTO entity, DefaultTagIndex tagIndex, SharedByteBuffer compressionBuffer);

        protected DefaultTagIndex getTagIndex() {
            return defaultTagIndex;
        }

        @Nonnull
        @Override
        public T finish() {
            // Trim the tags before calling BaseTopology::finish.
            defaultTagIndex.finish();
            T ret = super.finish();

            // Update the shared buffer size, so the next topology can use it
            // as a starting point.
            sharedBufferSize = compressionBuffer.getSize();
            return ret;
        }
    }

}
