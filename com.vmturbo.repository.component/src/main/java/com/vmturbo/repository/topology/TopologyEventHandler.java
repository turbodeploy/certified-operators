package com.vmturbo.repository.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;

/**
 * Handles topology events.
 * <ul>
 *     <li> Receiving topology DTOs (from Topology Processor or Market) </li>
 *     <li> Receiving price index data (from Market) </li>
 * </ul>
 */
public class TopologyEventHandler {
    private final Logger logger = LoggerFactory.getLogger(TopologyEventHandler.class);

    /**
     * The maximal retry count for topology update finished.
     */
    static final int MAX_TOPOLOGY_UPDATE_CHECK_RETRY_COUNT = 60;

    /**
     * The retry delay for topology update finished.
     */
    static final int TOPOLOGY_UPDATED_CHECK_RETRY_DELAY_IN_MILLI_SEC = 10 * 1000;

    /**
     * Keeps only limited number of topologies in the graph database.
     * TODO Handle this more elegantly (OM-11279)
     */
    static final int MAX_TOPOLOGY_COUNT = 1;

    private final TopologyIDManager topologyIDManager;
    private final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder;
    private final GraphDefinition graphDefinition;

    private static ArrayListMultimap<Long, TopologyID> topologyUpdated = ArrayListMultimap.create();
    private static ArrayListMultimap<Long, TopologyID> projectedTopologyUpdated = ArrayListMultimap.create();

    public TopologyEventHandler(
                         GraphDatabaseDriverBuilder graphDatabaseDriverBuilder,
                         GraphDefinition graphDefinition,
                         TopologyIDManager topologyIDManager)
                         throws GraphDatabaseException {
        this.topologyIDManager = topologyIDManager;
        this.graphDatabaseDriverBuilder = graphDatabaseDriverBuilder;
        this.graphDefinition = graphDefinition;
    }

    /**
     * Initializes the topology graph for the purpose of writing {@link TopologyID}
     * to the database.
     * @param tid the topology ID of the topology being written
     * @return a db graph creator
     * @throws GraphDatabaseException if the graph database cannot be created
     */
    public TopologyGraphCreator initializeTopologyGraph(TopologyID tid)
                    throws GraphDatabaseException {
        // TODO The operations in init() should be only invoked once for same topologyId
        final String database = topologyIDManager.databaseName(tid);
        final TopologyGraphCreator topologyGraphCreator = new TopologyGraphCreator(
                       graphDatabaseDriverBuilder.build(database),
                       graphDefinition);
        topologyGraphCreator.init();
        return topologyGraphCreator;
    }

    /**
     * Registers the {@link TopologyID} and unregisters old ones.
     *
     * @param tid The {@link TopologyID} to register
     * @throws GraphDatabaseException when database creation fails
     */
    public void register(TopologyID tid)
                    throws GraphDatabaseException {

        ListMultimap<Long, TopologyID> topologyIdsByContextId = TopologyType.SOURCE.equals(tid.getType()) ?
                        topologyUpdated : projectedTopologyUpdated;

        final long contextId = tid.getContextId();
        topologyIdsByContextId.put(contextId, tid);

        // Remove topologies that are too old
        // TODO Handle this more elegantly (OM-11279)
        // TODO - karthikt : drop the database in the background
        if (topologyIdsByContextId.get(contextId).size() > MAX_TOPOLOGY_COUNT) {
            final TopologyID tidDropped = topologyIdsByContextId.get(contextId).remove(0);
            dropDatabase(tidDropped);
            topologyIDManager.deRegister(tidDropped);
        }

        // Update the current real-time topology id
        if (TopologyType.SOURCE.equals(tid.getType())) {
            topologyIDManager.setCurrentRealTimeTopologyId(tid);
        }

        topologyIDManager.register(tid);
    }

    public boolean dropDatabase(final TopologyID tid) {
        return dropDatabase(topologyIDManager.databaseName(tid));
    }

    private boolean dropDatabase(final String databaseToRemove) {
        logger.info("Dropping database : {}", databaseToRemove);
        return graphDatabaseDriverBuilder.build(databaseToRemove).dropDatabase();
    }
}
