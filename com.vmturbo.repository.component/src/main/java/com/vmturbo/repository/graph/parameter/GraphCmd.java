package com.vmturbo.repository.graph.parameter;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;

import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.EdgeParameter.EdgeType;
import com.vmturbo.repository.topology.TopologyDatabase;

public abstract class GraphCmd {
    /**
     * Currently both consumes and connected relationship are supported, but we only put PROVIDER
     * and CONSUMER here for now since we only want to show consumes relationship in supply chain.
     * We can add more types (CONNECTED_TO AND CONNECTED_FROM) in the future if needed.
     */
    public enum SupplyChainDirection {
        PROVIDER(EdgeType.CONSUMES),
        CONSUMER(EdgeType.CONSUMES);

        private EdgeType edgeType;

        SupplyChainDirection(@Nonnull EdgeType edgeType) {
            this.edgeType = edgeType;
        }

        public String getEdgeType() {
            return edgeType.name();
        }
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
        private final Optional<EnvironmentType> environmentType;
        private final String graphName;
        private final String vertexCollection;
        // the entity access scope represents the set of entities a user has access to.
        private final Optional<EntityAccessScope> entityAccessScope;
        // the inclusion entity types in the path, which means it will traverse the path
        // if and only if the entity types in this path contains the provided set
        private final Set<Integer> inclusionEntityTypes;
        // the exclusion entity types in the path, which means it will not traverse the
        // path if the path contains entities of any type within the provided set
        private final Set<Integer> exclusionEntityTypes;

        public GetSupplyChain(@Nonnull final String startingVertex,
                              @Nonnull final Optional<EnvironmentType> environmentType,
                              final String graphName,
                              final String vertexCollection,
                              final Optional<EntityAccessScope> entityAccessScope,
                              final Set<Integer> inclusionEntityTypes,
                              final Set<Integer> exclusionEntityTypes) {
            this.startingVertex = startingVertex;
            this.environmentType = environmentType;
            this.graphName = graphName;
            this.vertexCollection = vertexCollection;
            this.entityAccessScope = entityAccessScope;
            this.inclusionEntityTypes = inclusionEntityTypes;
            this.exclusionEntityTypes = exclusionEntityTypes;
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

        public Optional<EnvironmentType> getEnvironmentType() {
            return environmentType;
        }

        public Optional<EntityAccessScope> getEntityAccessScope() {
            return entityAccessScope;
        }

        public Set<Integer> getInclusionEntityTypes() {
            return inclusionEntityTypes;
        }

        public Set<Integer> getExclusionEntityTypes() {
            return exclusionEntityTypes;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("startingVertex", startingVertex)
                    .add("environmentType", environmentType)
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

        private final Optional<EnvironmentType> environmentType;

        // the entity access scope represents the set of entities a user has access to.
        private final Optional<EntityAccessScope> entityAccessScope;

        // the entity types to skip while traversing the repository
        private final Set<Integer> ignoredEntityTypes;

        public GetGlobalSupplyChain(final TopologyDatabase topologyDatabase,
                                    final String vertexCollection,
                                    final Optional<EnvironmentType> environmentType,
                                    final Optional<EntityAccessScope> entityAccessScope,
                                    final Set<Integer> ignoredEntityTypes) {
            this.topologyDatabase = topologyDatabase;
            this.vertexCollection = vertexCollection;
            this.environmentType = environmentType;
            this.entityAccessScope = entityAccessScope;
            this.ignoredEntityTypes = ignoredEntityTypes;
        }

        public String getVertexCollection() {
            return vertexCollection;
        }

        public TopologyDatabase getTopologyDatabase() {
            return topologyDatabase;
        }

        public Optional<EnvironmentType> getEnvironmentType() {
            return environmentType;
        }

        public Optional<EntityAccessScope> getEntityAccessScope() {
            return entityAccessScope;
        }

        public Set<Integer> getIgnoredEntityTypes() {
            return ignoredEntityTypes;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topologyDatabase", topologyDatabase)
                    .add("vertexCollection", vertexCollection)
                    .add("environmentType", environmentType)
                    .add("entityAccessScope",
                            (entityAccessScope.isPresent() && !entityAccessScope.get().containsAll())
                            ? "Groups: "+ entityAccessScope.get().getScopeGroupIds()
                            : "All")
                    .add("ignoredEntityTypes", ignoredEntityTypes)
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

        public ServiceEntityMultiGet(@Nonnull final String collection,
                                     @Nonnull final Set<Long> entityIds) {
            this.collection = Objects.requireNonNull(collection);
            this.entityIds = Collections.unmodifiableSet(Objects.requireNonNull(entityIds));
        }

        public String getCollection() {
            return collection;
        }

        public Set<Long> getEntityIds() {
            return entityIds;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("collection", collection)
                    .add("entityIds", entityIds)
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

        public SearchServiceEntity(final String collection,
                                   final String field,
                                   final String query,
                                   final SearchType searchType) {
            this.collection = collection;
            this.field = field;
            this.query = query;
            this.searchType = searchType;
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

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("collection", collection)
                    .add("field", field)
                    .add("query", query)
                    .add("searchType", searchType)
                    .toString();
        }
    }
}
