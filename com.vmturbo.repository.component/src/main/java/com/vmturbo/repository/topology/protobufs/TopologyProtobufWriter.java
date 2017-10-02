package com.vmturbo.repository.topology.protobufs;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Writes raw topology DTOs to the database in chunks. It is assumed that a topology with a
 * given topology ID is written only once to the DB.
 * TODO: enforce this assumption.
 *
 */
public class TopologyProtobufWriter extends TopologyProtobufHandler {

    protected TopologyProtobufWriter(ArangoDatabaseFactory arangoDatabaseFactory, long topologyId) {
        super(arangoDatabaseFactory, topologyId);
    }

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    /**
     * Write a chunk to the database.
     *
     * @param chunk a collection of topology DTOs that form one chunk.
     * @throws ArangoDBException when there is a problem interacting with the database
     */
    public void storeChunk(
                    Collection<TopologyDTO.TopologyEntityDTO> chunk)
                        throws ArangoDBException {
        logger.debug("Writing chunk #{} of size {} for topology {}",
            sequenceNumber, chunk.size(), topologyId);
        BaseDocument doc = new BaseDocument();
        doc.setKey(String.valueOf(sequenceNumber));
        Map<String, Object> map = chunk.stream()
                        .collect(Collectors.toMap(
                                dto -> String.valueOf(dto.getOid()),
                                GSON::toJson)); // TODO: use dto.toByteArray()
        doc.setProperties(map);
        topologyCollection.insertDocument(doc);
        logger.debug("Done writing chunk #{} for topology {}", sequenceNumber, topologyId);
        sequenceNumber++;
    }
}
