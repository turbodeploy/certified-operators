package com.vmturbo.repository.topology.protobufs;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;

/**
 * Reads raw topology DTOs from the database in chunks.
 *
 */
public class TopologyProtobufReader extends TopologyProtobufHandler {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final long count;

    private final Optional<TopologyEntityFilter> entityFilter;

    protected TopologyProtobufReader(@Nonnull final ArangoDatabaseFactory arangoDatabaseFactory,
                                     final long topologyId,
                                     @Nonnull final Optional<TopologyEntityFilter> entityFilter) {
        super(arangoDatabaseFactory, topologyId);
        count = topologyCollection.count().getCount();
        this.entityFilter = entityFilter;
    }

    /**
     * Get a handle to the collection.
     */
    @Override
    protected ArangoCollection collection(long topologyId) {
        ArangoDatabase database = database();
        String collectionName = collectionName(topologyId);
        return database.collection(collectionName);
    }

    public boolean hasNext() {
        return sequenceNumber < count;
    }

    public List<TopologyDTO.TopologyEntityDTO> nextChunk() {
        BaseDocument doc = topologyCollection.getDocument(String.valueOf(sequenceNumber), BaseDocument.class);
        sequenceNumber++;
        return doc.getProperties().values().stream()
            .map(TopologyProtobufReader::parseJson)
            .filter(this::entityMatchesFilter)
            .collect(Collectors.toList());
    }

    private static TopologyDTO.TopologyEntityDTO EMPTY_DTO =
            TopologyDTO.TopologyEntityDTO.newBuilder().setOid(-1).setEntityType(0).build();

    @VisibleForTesting
    protected static TopologyDTO.TopologyEntityDTO parseJson(Object json) {
        try {
            return GSON.fromJson((String)json, TopologyDTO.TopologyEntityDTO.class);
        } catch (JsonParseException e) {
            logger.error("Problem parsing DTO : " + json, e);
            return EMPTY_DTO;
        }
    }

    private boolean entityMatchesFilter(@Nonnull final TopologyEntityDTO entity) {
        return entityFilter.map(filter -> RepositoryDTOUtil.entityMatchesFilter(entity, filter))
                .orElse(false);
    }
}
