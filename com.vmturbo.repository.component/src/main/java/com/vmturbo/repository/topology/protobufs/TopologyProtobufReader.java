package com.vmturbo.repository.topology.protobufs;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.CollectionCreateOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
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
                                     @Nonnull final Optional<TopologyEntityFilter> entityFilter,
                                     @Nonnull final String arangoDatabaseName,
                                     @Nonnull final CollectionCreateOptions collectionCreateOptions) {
        super(arangoDatabaseFactory, topologyId, arangoDatabaseName, collectionCreateOptions);
        count = topologyCollection.count().getCount();
        this.entityFilter = entityFilter;
    }

    /**
     * Get a handle to the collection.
     */
    @Override
    protected ArangoCollection collection(@Nonnull final String collectionName,
                                          @Nonnull final CollectionCreateOptions collectionCreateOptions) {
        return database.collection(collectionName);
    }

    public boolean hasNext() {
        return sequenceNumber < count;
    }

    public List<ProjectedTopologyEntity> nextChunk() {
        BaseDocument doc = topologyCollection.getDocument(String.valueOf(sequenceNumber), BaseDocument.class);
        logger.debug("...fetch next chunk, doc properties size:  {}", doc.getProperties().size());
        sequenceNumber++;
        return parseJson(topologyId, doc.getProperties().values())
            .filter(this::entityMatchesFilter)
            .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected static Stream<ProjectedTopologyEntity> parseJson(final long topologyId,
                                               @Nonnull final Collection<Object> jsonObjects) {
        // We assume we're going to be reading ProjectedTopologyEntity objects. However,
        // in old saved projected topologies or in source topologies, we may actually read
        // TopologyEntityDTO objects.
        // Use this flag to detect the latter format, to avoid having exceptions on every
        // encountered entity.
        AtomicBoolean areProjectedEntities = new AtomicBoolean(true);
        return jsonObjects.stream()
            .map(jsonObj -> {
                ProjectedTopologyEntity parsedEntity = null;
                if (areProjectedEntities.get()) {
                    try {
                        parsedEntity = GSON.fromJson((String)jsonObj, ProjectedTopologyEntity.class);
                        if (parsedEntity.equals(ProjectedTopologyEntity.getDefaultInstance())) {
                            // Try parsing the old format - simple TopologyEntityDTOs.
                            areProjectedEntities.set(false);
                            logger.warn("Likely reading projected topology {} stored in older format (without price index)!", topologyId);
                        }
                    } catch (JsonParseException e) {
                        // Try parsing the old format - simple TopologyEntityDTOs.
                        areProjectedEntities.set(false);
                        logger.warn("Reading projected topology {} stored in older format (without price index)!", topologyId);
                    }
                }

                // Separate if statement so that we still try to re-parse the first entity.
                if (!areProjectedEntities.get()) {
                    try {
                        // Note: We don't re-save the old entity in the new format because the main
                        // reason this would get called is to look at old plan results, and we should
                        // clear them out eventually anyway.
                        final TopologyEntityDTO entityDTO = GSON.fromJson((String)jsonObj, TopologyEntityDTO.class);
                        parsedEntity = ProjectedTopologyEntity.newBuilder()
                                .setEntity(entityDTO)
                                .build();
                    } catch (JsonParseException e2) {
                        logger.error("Problem parsing DTO : " + jsonObj, e2);
                    }
                }
                return parsedEntity;
            })
            .filter(Objects::nonNull);
    }

    private boolean entityMatchesFilter(@Nonnull final ProjectedTopologyEntity entity) {
        // Reference comparison with EMPTY_DTO for speed.
        return entityFilter.map(filter -> RepositoryDTOUtil.entityMatchesFilter(entity.getEntity(), filter))
                .orElse(true);
    }
}
