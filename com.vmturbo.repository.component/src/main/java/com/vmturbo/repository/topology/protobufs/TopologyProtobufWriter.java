package com.vmturbo.repository.topology.protobufs;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.CollectionCreateOptions;
import com.google.gson.Gson;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Writes raw topology DTOs to the database in chunks. It is assumed that a topology with a
 * given topology ID is written only once to the DB.
 * TODO: enforce this assumption.
 *
 * @param <E> The type of object to write to convert to json and write to the database. For example,
 *           the type could be a TopologyEntityDTO or a ProjectedTopologyEntity.
 */
public class TopologyProtobufWriter<E> extends TopologyProtobufHandler {

    private final Function<E, String> keyMappingFunction;

    protected TopologyProtobufWriter(ArangoDatabaseFactory arangoDatabaseFactory, long topologyId,
                                     Function<E, String> entityOidMappingFunction,
                                     String arangoDatabaseName, CollectionCreateOptions collectionCreateOptions) {
        super(arangoDatabaseFactory, topologyId, arangoDatabaseName, collectionCreateOptions);
        this.keyMappingFunction = entityOidMappingFunction;
    }

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    /**
     * Write a chunk to the database.
     *
     * @param chunk a collection of topology DTOs that form one chunk.
     * @throws ArangoDBException when there is a problem interacting with the database
     */
    public void storeChunk(Collection<E> chunk)
                        throws ArangoDBException {
        logger.debug("Writing chunk #{} of size {} for topology {}",
            sequenceNumber, chunk.size(), topologyId);
        final BaseDocument entityChunkDoc = new BaseDocument();
        entityChunkDoc.setKey(String.valueOf(sequenceNumber));
        final Map<String, Object> map = chunk.stream()
            .collect(Collectors.toMap(
                keyMappingFunction,
                GSON::toJson)); // TODO: use dto.toByteArray()
        entityChunkDoc.setProperties(map);
        topologyCollection.insertDocument(entityChunkDoc);
        logger.debug("Done writing chunk #{} for topology {}", sequenceNumber, topologyId);

        sequenceNumber++;
    }
}
