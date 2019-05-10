package com.vmturbo.repository.graph.operator;

import java.util.Collection;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.EdgeOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.IndexOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.VertexOperationException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.parameter.EdgeDefParameter;
import com.vmturbo.repository.graph.parameter.GraphParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter;
import com.vmturbo.repository.graph.parameter.IndexParameter.GraphIndexType;

/**
 * Constructs a topology graph in an associated graph database from input SE DTOs.
 *
 * The graph contains subgraphs created by:
 * <ul>
 *     <li> {@link ServiceEntitySubGraphCreator} </li>
 * </ul>
 */
public class TopologyGraphCreator {
    private final Logger logger = LoggerFactory.getLogger(TopologyGraphCreator.class);

    private final GraphDatabaseDriver graphDatabaseDriver;
    private final GraphDefinition graphDefinition;
    private final ServiceEntitySubGraphCreator serviceEntitySubGraphCreator;
    static final int BATCH_SIZE = 100;

    public TopologyGraphCreator(final GraphDatabaseDriver graphDatabaseDriver,
                                final GraphDefinition graphDefinition) {
        this.graphDefinition = Objects.requireNonNull(graphDefinition);
        this.graphDatabaseDriver = Objects.requireNonNull(graphDatabaseDriver);
        this.serviceEntitySubGraphCreator = new ServiceEntitySubGraphCreator(
                                graphDatabaseDriver, graphDefinition, BATCH_SIZE);
    }

    /**
     * Updates the topology graph in the database. The graph will be emptied, followed by adding
     * new documents based on the input DTOs.
     *
     * @param ses The {@link ServiceEntityRepoDTO} objects of the topology
     * @throws EdgeOperationException
     * @throws VertexOperationException
     * @throws CollectionOperationException
     */
    public void updateTopologyToDb(Collection<ServiceEntityRepoDTO> ses)
            throws VertexOperationException, EdgeOperationException, CollectionOperationException {

        serviceEntitySubGraphCreator.create(ses);
    }

    /**
     * Performs the following operations:
     * <ul>
     *     <li> Creates the database if it doesn't exist. Return otherwise. </li>
     *     <li> Performs the init method of all its subgraph creators. </li>
     *     <li> Creates the graph in the database </li>
     *     <li> Creates indices for graph collections </li>
     * </ul>
     *
     * @throws GraphDatabaseException
     */
    public void init() throws GraphDatabaseException {
        final String database = graphDatabaseDriver.getDatabase();
        // Create the database if it doesn't exist
        try {
            if (graphDatabaseDriver.createDatabase()) {
                logger.info("Created database " + database);
            } else {
                logger.warn("Database " + database + " already exists");
                return;
            }
        } catch (GraphDatabaseException e) {
            throw new GraphDatabaseException(
                         "Exception encountered while creating database " + database, e);
        }


        serviceEntitySubGraphCreator.init();

        createTopologyGraph();

        createIndices();

        logger.info("Initialized topology graph creation in database " + database);
    }

    private void createIndices() throws IndexOperationException {
        final IndexParameter displayNameIndex = new IndexParameter.Builder(graphDefinition.getServiceEntityVertex(),
                                                                           IndexParameter.GraphIndexType.FULLTEXT)
                                                                  .addField("displayName")
                                                                  .build();

        final IndexParameter entityTypeIndex = new IndexParameter.Builder(graphDefinition.getServiceEntityVertex(),
                                                                          IndexParameter.GraphIndexType.HASH)
                                                                 .addField("entityType")
                                                                 .unique(false)
                                                                 .build();

        final IndexParameter uuidIndex = new IndexParameter.Builder(graphDefinition.getServiceEntityVertex(),
                                                                    IndexParameter.GraphIndexType.HASH)
                                                           .addField("uuid")
                                                           .unique(true)
                                                           .build();

        final IndexParameter oidIndex = new IndexParameter.Builder(graphDefinition.getServiceEntityVertex(),
                                                                   IndexParameter.GraphIndexType.HASH)
                                                          .addField("oid")
                                                          .unique(true)
                                                          .build();

        // entity type, state index is for facilitating supply chain queries. We are using a skip
        // list because the query will use it for sorting the results.
        final IndexParameter entityTypeAndStateIndex = new IndexParameter.Builder(graphDefinition.getServiceEntityVertex(),
                                                                                GraphIndexType.SKIPLIST)
                .addField("entityType")
                .addField("state")
                .unique(false)
                .sparse(false)
                .deduplicate(true)
                .build();

        graphDatabaseDriver.createIndex(displayNameIndex);
        graphDatabaseDriver.createIndex(entityTypeIndex);
        graphDatabaseDriver.createIndex(uuidIndex);
        graphDatabaseDriver.createIndex(oidIndex);
        graphDatabaseDriver.createIndex(entityTypeAndStateIndex);
    }

    private void createTopologyGraph() throws GraphOperationException {
        EdgeDefParameter edp = new EdgeDefParameter.Builder(
                graphDefinition.getServiceEntityVertex(),
                graphDefinition.getProviderRelationship())
                .build();
        GraphParameter p = new GraphParameter.Builder(graphDefinition.getGraphName())
                .addEdgeDef(edp)
                .build();
        graphDatabaseDriver.createGraph(p);
    }

}
