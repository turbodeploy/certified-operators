package com.vmturbo.repository.topology.protobufs;

import javax.annotation.Nonnull;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Handle writing/reading/deleting to/from the database of raw topologies in chunks. A raw
 * topology is saved as an {@link ArangoCollection} of {@link BaseDocument}s, each document
 * contains a chunk of {@link com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity}s.
 *
 */
public abstract class TopologyProtobufHandler {

    protected static final Logger logger = LogManager.getLogger();
    protected final long topologyId;
    private final ArangoDatabaseFactory arangoFactory;
    protected final ArangoDatabase database;
    protected final ArangoCollection topologyCollection;
    protected int sequenceNumber = 0;

    private static String RAW_TOPOLOGIES_DATABASE_NAME = "topology-protobufs";

    /**
     * Create a collection for the given topology ID.
     *
     * @param topologyId topology id of the raw topology
     * @param arangoDatabaseFactory the dabase factory
     */
    protected TopologyProtobufHandler(ArangoDatabaseFactory arangoDatabaseFactory, long topologyId) {
        this.topologyId = topologyId;
        this.arangoFactory = arangoDatabaseFactory;
        database = database();
        topologyCollection = collection(getTopologyCollectionName(topologyId));
    }

    private static String getTopologyCollectionName(long topologyId) {
        return "topology-dtos-" + topologyId;
    }

    /**
     * Obtain a collection where the topology chunks are/will be stored.
     *
     * @param collectionName identifying the raw topology
     * @return an ArangoDB collection that holds the chunks of this topology
     */
    protected ArangoCollection collection(@Nonnull final String collectionName) {
        database.createCollection(collectionName);
        return database.collection(collectionName);
    }

    /**
     * Verify that the raw topologies database exists, and if not then create it.
     *
     * @return the raw topologies database
     */
    private synchronized ArangoDatabase database() {
        if (!arangoFactory.getArangoDriver().getDatabases().contains(RAW_TOPOLOGIES_DATABASE_NAME)) {
            logger.info("Creating database {}", RAW_TOPOLOGIES_DATABASE_NAME);
            arangoFactory.getArangoDriver().createDatabase(RAW_TOPOLOGIES_DATABASE_NAME);
        }
        return arangoFactory.getArangoDriver().db(RAW_TOPOLOGIES_DATABASE_NAME);
    }

    /**
     * Delete the collection.
     *
     * @throws ArangoDBException when there is a problem interacting with the database
     */
    public void delete() {
        try {
            logger.debug("Dropping collection {}",  topologyCollection.getInfo().getName());
            topologyCollection.drop();
        } catch (ArangoDBException e) {
            logger.error("Attempt to drop collection {} failed", topologyCollection.getInfo().getName(), e);
            throw e;
        }
    }
}
