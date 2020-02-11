package com.vmturbo.repository.graph;

import com.google.common.base.MoreObjects;

import java.util.Objects;

import com.vmturbo.repository.topology.TopologyID;

/**
 * Metadata about the service entity graph stored in a graph database.
 */
public class GraphDefinition {

    private final String graphName;
    private final String serviceEntityVertex;
    private final String providerRelationship;
    private final String topologyProtoCollection;

    public GraphDefinition(String graphName,
                           String serviceEntityVertex,
                           String providerRelationship,
                           String topologyProtoCollection) {
        this.graphName = Objects.requireNonNull(graphName);
        this.serviceEntityVertex = Objects.requireNonNull(serviceEntityVertex);
        this.providerRelationship = Objects.requireNonNull(providerRelationship);
        this.topologyProtoCollection = Objects.requireNonNull(topologyProtoCollection);
    }

    public String getGraphName() {
        return graphName;
    }

    public String getServiceEntityVertex() {
        return serviceEntityVertex;
    }

    public String getProviderRelationship() {
        return providerRelationship;
    }

    /**
     * Get full serviceEntityVertex collection name by appending collection name suffix from the
     * given TopologyID.
     *
     * @param topologyID Given TopologyID from which collection name suffix is converted.
     * @return Constructed full serviceEntityVertex collection name by appending collection name
     * suffix from given TopologyID.
     */
    public String getSEVertexCollection(TopologyID topologyID) {
        return serviceEntityVertex + topologyID.toCollectionNameSuffix();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("graphName", graphName)
                .add("serviceEntityVertex", serviceEntityVertex)
                .add("providerRelationship", providerRelationship)
                .add("topologyProtoCollection", topologyProtoCollection)
                .toString();
    }

    public static class Builder {
        private String graphName;
        private String serviceEntityVertex;
        private String providerRelationship;
        private String topologyProtoCollection = "topology_proto";

        public Builder setGraphName(String graphName) {
            this.graphName = graphName;
            return this;
        }

        public Builder setServiceEntityVertex(String serviceEntityVertex) {
            this.serviceEntityVertex = serviceEntityVertex;
            return this;
        }

        public Builder setProviderRelationship(String providerRelationship) {
            this.providerRelationship = providerRelationship;
            return this;
        }

        public Builder setTopologyProtoCollection(final String topologyProtoCollection) {
            this.topologyProtoCollection = topologyProtoCollection;
            return this;
        }

        public GraphDefinition createGraphDefinition() {
            return new GraphDefinition(graphName, serviceEntityVertex, providerRelationship, topologyProtoCollection);
        }
    }
}
