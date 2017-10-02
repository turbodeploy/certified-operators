package com.vmturbo.repository.graph.parameter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Multimap;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.topology.TopologyDatabase;

public abstract class GraphCmd {
    public enum SupplyChainDirection {
        PROVIDER, CONSUMER
    }

    /**
     * Different search types means different search query.
     *
     * <dl>
     *     <dt>FULLTEXT</dt>
     *     <dd>Perform a FULLTEXT search on a field with a FULLTEXT index</dd>
     *
     *     <dt>STRING</dt>
     *     <dd>Simple string equality query</dd>
     *
     *     <dt>NUMERIC</dt>
     *     <dd>Simple numeric equality query</dd>
     * </dl>
     */
    public enum SearchType {
        FULLTEXT, STRING, NUMERIC;
    }

    private GraphCmd() {}

    /**
     * A command that contains all the information a {@link GraphDBExecutor} needs to compute
     * a supply chain for a given service entity.
     */
    public static final class GetSupplyChain extends GraphCmd {

        private final String startingVertex;
        private final String graphName;
        private final String vertexCollection;
        private final TopologyDatabase topologyDatabase;

        public GetSupplyChain(@Nonnull final String startingVertex,
                              final TopologyDatabase topologyDatabase,
                              final String graphName,
                              final String vertexCollection) {
            this.startingVertex = startingVertex;
            this.topologyDatabase = topologyDatabase;
            this.graphName = graphName;
            this.vertexCollection = vertexCollection;
        }

        public String getGraphName() {
            return graphName;
        }

        public String getVertexCollection() {
            return vertexCollection;
        }

        @Nonnull
        public String getStartingVertex() {
            return startingVertex;
        }

        public TopologyDatabase getTopologyDatabase() {
            return topologyDatabase;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topologyDatabase", topologyDatabase)
                    .add("startingVertex", startingVertex)
                    .add("graphName", graphName)
                    .add("vertexCollection", vertexCollection)
                    .toString();
        }
    }

    /**
     * A command for computing the global supply chain.
     */
    public static final class GetGlobalSupplyChain extends GraphCmd {
        private final String vertexCollection;

        private final TopologyDatabase topologyDatabase;
        private final Multimap<String, String> providerStructure;

        public GetGlobalSupplyChain(final TopologyDatabase topologyDatabase,
                                    final String vertexCollection,
                                    final Multimap<String, String> providerStructure) {
            this.topologyDatabase = topologyDatabase;
            this.vertexCollection = vertexCollection;
            this.providerStructure = providerStructure;
        }

        public String getVertexCollection() {
            return vertexCollection;
        }

        public Multimap<String, String> getProviderStructure() {
            return providerStructure;
        }

        public TopologyDatabase getTopologyDatabase() {
            return topologyDatabase;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topologyDatabase", topologyDatabase)
                    .add("vertexCollection", vertexCollection)
                    .add("providerStructure", providerStructure)
                    .toString();
        }
    }

    /**
     * A multi-get of service entities by OID.
     */
    public static final class ServiceEntityMultiGet extends GraphCmd {

        /**
         * The collection to search in.
         */
        private final String collection;

        /**
         * The OIDs of the entities to search for.
         */
        private final Set<Long> entityIds;

        private final TopologyDatabase topologyDatabase;

        public ServiceEntityMultiGet(@Nonnull final String collection,
                                     @Nonnull final Set<Long> entityIds,
                                     @Nonnull final TopologyDatabase topologyDatabase) {
            this.collection = Objects.requireNonNull(collection);
            this.entityIds = Collections.unmodifiableSet(Objects.requireNonNull(entityIds));
            this.topologyDatabase = Objects.requireNonNull(topologyDatabase);
        }

        public String getCollection() {
            return collection;
        }

        public Set<Long> getEntityIds() {
            return entityIds;
        }

        public TopologyDatabase getTopologyDatabase() {
            return topologyDatabase;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("collection", collection)
                    .add("entityIds", entityIds)
                    .add("topologyDatabase", topologyDatabase)
                    .toString();
        }
    }

    /**
     * A search command.
     */
    public static final class SearchServiceEntity extends GraphCmd {

        private final String collection;

        private final String field;

        private final String query;

        private final SearchType searchType;

        private final TopologyDatabase topologyDatabase;

        public SearchServiceEntity(final String collection,
                                   final String field,
                                   final String query,
                                   final SearchType searchType,
                                   final TopologyDatabase topologyDatabase) {
            this.collection = collection;
            this.field = field;
            this.query = query;
            this.searchType = searchType;
            this.topologyDatabase = topologyDatabase;
        }

        public String getCollection() {
            return collection;
        }

        public String getField() {
            return field;
        }

        public String getQuery() {
            return query;
        }

        public SearchType getSearchType() {
            return searchType;
        }

        public TopologyDatabase getTopologyDatabase() {
            return topologyDatabase;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("collection", collection)
                    .add("field", field)
                    .add("query", query)
                    .add("searchType", searchType)
                    .add("topologyDatabase", topologyDatabase)
                    .toString();
        }
    }
}
