package com.vmturbo.repository.topology.protobufs;

import java.util.NoSuchElementException;

import org.apache.http.HttpStatus;

import com.arangodb.ArangoDBException;

import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Create raw topology readers and writers.
 *
 */
public class TopologyProtobufsManager {

    private static final int ERROR_ARANGO_COLLECTION_NOT_FOUND = 1203;

    private final ArangoDatabaseFactory arangoDatabaseFactory;

    public TopologyProtobufsManager(ArangoDatabaseFactory arangoDatabaseFactory) {
        this.arangoDatabaseFactory = arangoDatabaseFactory;
    }

    /**
     * Create a writer for chunks of raw topology DTOs. This also creates a collection in the
     * databse for the given topology ID.
     *
     * @param topologyId the topology ID identifies the collection in the database
     * @return an instance of a writer, used in writing chunks to the database
     */
    public TopologyProtobufWriter createTopologyProtobufWriter(long topologyId) {
        return new TopologyProtobufWriter(arangoDatabaseFactory, topologyId);
    }

    /**
     * Create a reader of chunks of raw topology DTOs.
     *
     * @param topologyId identifies the topology that is read from the DB
     * @return an instance of a reader, used in reading chunks from the database
     * @throws NoSuchElementException  If the database does not contain a
     * collection for the given topology ID
     */
    public TopologyProtobufReader createTopologyProtobufReader(long topologyId)
                    throws NoSuchElementException {
        try {
            return new TopologyProtobufReader(arangoDatabaseFactory, topologyId);
        } catch (ArangoDBException e) {
            if (e.getResponseCode() == HttpStatus.SC_NOT_FOUND
                            && e.getErrorNum() == ERROR_ARANGO_COLLECTION_NOT_FOUND) {
                // unknown collection
                throw new NoSuchElementException("Unknown topology " + topologyId);
            } else {
                throw e;
            }
        }
    }

}
