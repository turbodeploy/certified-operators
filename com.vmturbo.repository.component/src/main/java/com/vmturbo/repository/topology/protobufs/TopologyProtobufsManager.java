package com.vmturbo.repository.topology.protobufs;

import java.util.NoSuchElementException;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.http.HttpStatus;

import com.arangodb.ArangoDBException;
import com.arangodb.model.CollectionCreateOptions;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Create raw topology readers and writers.
 *
 */
public class TopologyProtobufsManager {

    private static final int ERROR_ARANGO_COLLECTION_NOT_FOUND = 1203;

    private final ArangoDatabaseFactory arangoDatabaseFactory;
    private final String arangoDatabaseName;
    private final CollectionCreateOptions defaultCollectionOptions;

    /**
     * TopologyProtobufsManager to create raw topology readers and writers.
     *
     * @param arangoDatabaseFactory   Functional interface to create ArangoDB driver.
     * @param arangoDatabaseName ArangoDB database name.
     * @param defaultCollectionOptions the default collection creation options to use
     */
    public TopologyProtobufsManager(ArangoDatabaseFactory arangoDatabaseFactory,
                                    String arangoDatabaseName,
                                    CollectionCreateOptions defaultCollectionOptions) {
        this.arangoDatabaseFactory = arangoDatabaseFactory;
        this.arangoDatabaseName = arangoDatabaseName;
        this.defaultCollectionOptions = defaultCollectionOptions;
    }

    /**
     * Create a writer for chunks of raw projected topology DTOs. This also creates a collection in the
     * database for the given topology ID.
     *
     * @param topologyId the topology ID identifies the collection in the database
     * @return an instance of a writer, used in writing chunks to the database
     */
    public TopologyProtobufWriter<ProjectedTopologyEntity> createProjectedTopologyProtobufWriter(
        long topologyId) {
        return new TopologyProtobufWriter<ProjectedTopologyEntity>(arangoDatabaseFactory, topologyId,
            dto -> String.valueOf(dto.getEntity().getOid()), arangoDatabaseName, defaultCollectionOptions);
    }

    /**
     * Create a writer for chunks of raw topology DTOs. This also creates a collection in the
     * database for the given topology ID.
     *
     * @param topologyId the topology ID identifies the collection in the database
     * @return an instance of a writer, used in writing chunks to the database
     */
    public TopologyProtobufWriter<TopologyEntityDTO> createSourceTopologyProtobufWriter(
        long topologyId) {
        return new TopologyProtobufWriter<TopologyEntityDTO>(arangoDatabaseFactory, topologyId,
            dto -> String.valueOf(dto.getOid()), arangoDatabaseName, defaultCollectionOptions);
    }

    /**
     * Create a reader of chunks of raw topology DTOs.
     *
     * @param topologyId identifies the topology that is read from the DB
     * @return an instance of a reader, used in reading chunks from the database
     * @throws NoSuchElementException  If the database does not contain a
     * collection for the given topology ID
     */
    public TopologyProtobufReader createTopologyProtobufReader(final long topologyId,
                   @Nonnull final Optional<TopologyEntityFilter> entityFilter)
            throws NoSuchElementException {
        try {
            return new TopologyProtobufReader(arangoDatabaseFactory, topologyId, entityFilter,
                arangoDatabaseName, defaultCollectionOptions);
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
