package com.vmturbo.repository.service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDB;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

public class GraphTopologyService {
    private final Logger logger = LoggerFactory.getLogger(GraphTopologyService.class);

    private final static String TOPOLOGY_DB_PREFIX ="topology";

    private final ArangoDatabaseFactory arangoDatabaseFactory;

    private final GraphDefinition graphDefinition;

    public GraphTopologyService(final ArangoDatabaseFactory arangoDatabaseFactory,
                                final GraphDefinition graphDefinition) {
        this.arangoDatabaseFactory = Objects.requireNonNull(arangoDatabaseFactory);
        this.graphDefinition = Objects.requireNonNull(graphDefinition);
    }

    /**
     * Delete a topology with the given topology ID.
     *
     * TODO: This is a temporary solution deal to time constraint.
     *
     * @param topologyID The topology to delete.
     */
    public void deleteTopology(final String topologyID) {
        logger.info("Deleting topology with ID {}", topologyID);
        final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();

        final List<String> databaseCandidates = arangoDB.getDatabases().stream()
                .filter(name -> topologyDatabaseFilter(name, topologyID))
                .collect(Collectors.toList());

        if (databaseCandidates.isEmpty()) {
            logger.error("Cannot find a topology with ID {}", topologyID);
            throw new IllegalArgumentException("Cannot find a topology with ID " + topologyID);
        } else if (databaseCandidates.size() > 1) {
            logger.error("There are more than one database found {}", databaseCandidates);
            throw new IllegalArgumentException("There are more than one database found " + databaseCandidates);
        } else {
            final String databaseToDelete = databaseCandidates.get(0);
            try {
                logger.info("Dropping database {}", databaseToDelete);
                arangoDB.db(databaseToDelete).drop();
            } catch (RuntimeException e) {
                logger.error("Cannot delete database", e);
                // rethrow
                throw e;
            }
        }
    }

    /**
     * Retrieve the topology protobuf from the database.
     *
     * @param topologyID The ID of the topology to retrieve.
     * @return {@link TopologyDTO.Topology} or null if not found.
     */
    public TopologyDTO.Topology retrieveTopology(final long topologyID) {
        try {
            logger.info("Retrieving topology for {}", topologyID);
            final ArangoDB arangoDB = arangoDatabaseFactory.getArangoDriver();

            final TopologyDTO.Topology topology = arangoDB.db()
                    .collection(graphDefinition.getTopologyProtoCollection())
                    .getDocument(Long.toString(topologyID), TopologyDTO.Topology.class);

            if (topology == null) {
                logger.warn("Cannot find topology with ID {}", topologyID);
            }

            return topology;
        } catch (RuntimeException e) {
            logger.error("Exception caught when retrieving topology " + topologyID, e);

            return null;
        }
    }

    /**
     * A filter for filtering out database name.
     *
     * This is used by {@link #deleteTopology(String)} to determine which topology database to delete.
     *
     * @param databaseName The database name.
     * @param topologyID The ID of the topology we want to delete.
     * @return True or False
     */
    @VisibleForTesting
    static boolean topologyDatabaseFilter(final String databaseName, final String topologyID) {
        // Make sure the database name starts with `topology`.
        if (!databaseName.startsWith(TOPOLOGY_DB_PREFIX)) {
            return false;
        } else {
            // Database name should have the format `topology-{context_id}-{topology_id}`.
            final String[] databaseNameComponents = databaseName.split("-");

            // Extract the topology id from the database name.
            if (databaseNameComponents.length == 3) {
                final String topoIDInDatabaseName = databaseNameComponents[2];
                return topoIDInDatabaseName.equals(topologyID);
            }
        }

        return false;
    }
}
