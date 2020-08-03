package com.vmturbo.repository.graph.operator;

import com.vmturbo.repository.graph.GraphDefinition;

public class GraphCreatorFixture {
    private final GraphDefinition graphDefinition = new GraphDefinition.Builder()
            .setGraphName("seGraph")
            .setServiceEntityVertex("seVertexCollection")
            .setProviderRelationship("seProviderEdgeCol")
            .createGraphDefinition();

    public GraphDefinition getGraphDefinition() {
        return graphDefinition;
    }
}
