package com.vmturbo.history.ingesters.live.writers;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;

/**
 * This class is responsible for writing new or changed entities from a topology broadcast chunk
 * to the Entities table.
 */
public class EntitiesWriter extends TopologyWriterBase {
    private static Logger logger = LogManager.getLogger(EntitiesWriter.class);

    private final TopologyInfo topologyInfo;
    private final HistorydbIO historydbIO;
    private final BulkLoader<EntitiesRecord> entitiesLoader;

    /**
     * Create a new instance, to be used for all chunks in a single topology broadcast.
     *
     * @param topologyInfo metadata for the topology
     * @param historydbIO  access to common DB features
     * @param loaders      bulk loaders
     */
    private EntitiesWriter(@Nonnull TopologyInfo topologyInfo,
                           @Nonnull HistorydbIO historydbIO,
                           @Nonnull SimpleBulkLoaderFactory loaders) {
        this.topologyInfo = topologyInfo;
        this.historydbIO = historydbIO;
        this.entitiesLoader = loaders.getLoader(Entities.ENTITIES);
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> entities,
                                            @Nonnull final String infoSummary)
        throws InterruptedException {

        final List<String> entityOids = entities.stream()
            .map(TopologyEntityDTO::getOid)
            .map(String::valueOf)
            .collect(Collectors.toList());

        // get current saved info for each of these entities
        final Map<Long, EntitiesRecord> knownChunkEntities;
        try {
            knownChunkEntities = new HashMap<>(historydbIO.getEntities(entityOids));
        } catch (VmtDbException e) {
            logger.warn("Failed to retrieve known entities from topology broadcast chunk; " +
                    "will discontinue writing entities for broadcast {}",
                infoSummary);
            return ChunkDisposition.DISCONTINUE;
        }

        // create entities table records for all those entities. These will be a mixture of update
        // records where the entity already appears in the database but have different info, and
        // new records for the rest
        final long snapshotTime = topologyInfo.getCreationTime();
        final List<EntitiesRecord> entityRecordsToPersist = entities.stream().map(entity ->
            createRecord(entity.getOid(), snapshotTime, entity, knownChunkEntities))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        // persist this chunk's entities
        entitiesLoader.insertAll(entityRecordsToPersist);
        return ChunkDisposition.SUCCESS;
    }

    /**
     * Creates db record, based on topology entity and potentially, on existing record from the DB.
     *
     * <p>This method will return db record, if some operations (modification or insertion) is required
     * for the specified entity.</p>
     *
     * <p>A {@link TopologyEntityDTO} needs to be persisted if:</p>
     * <ol>
     * <li>its OID is not found in the DB, or
     * <li>its Creation Class (EntityType) has changed, or
     * <li>its Display Name has changed.
     * </ol>
     *
     * @param oid             then entity to generate a DB record for
     * @param snapshotTime    time for this topology
     * @param entityDTO       the TopologyEntityDTO for the Service Entity to persist
     * @param existingRecords the map of known oid -> record in the DB
     * @return an Optional containing an EntitiesRecord to be persisted - either a new
     * record to be inserted or an updated copy of the prior record if modified; or Optional.empty()
     * if this particular entity type is not to be persisted to the DB
     */
    @Nonnull
    private Optional<EntitiesRecord> createRecord(
        long oid,
        long snapshotTime,
        @Nonnull TopologyEntityDTO entityDTO,
        @Nonnull Map<Long, EntitiesRecord> existingRecords) {

        final Optional<EntityType> entityDBInfo =
            historydbIO.getEntityType(entityDTO.getEntityType());
        if (!entityDBInfo.isPresent()) {
            // entity type not found - some entity types are not persisted
            return Optional.empty();
        }

        final String entityType = entityDBInfo.get().getName();
        final long entityOid = entityDTO.getOid();

        final EntitiesRecord record;
        EntitiesRecord existingRecord = existingRecords.get(oid);
        if (existingRecord == null) {
            record = new EntitiesRecord();
            record.setId(entityOid);
        } else {
            if (existingRecord.getDisplayName().equals(entityDTO.getDisplayName()) &&
                existingRecord.getCreationClass().equals(entityType)) {
                return Optional.empty();
            }
            logger.warn("Name or type for existing entity with oid {} has been changed: " +
                    "displayName >{}< -> >{}<" +
                    " creationType >{}< -> >{}<; db updated.", entityDTO.getOid(),
                existingRecord.getDisplayName(), entityDTO.getDisplayName(),
                existingRecord.getCreationClass(), entityType);
            record = existingRecord;
        }
        // in the Legacy OpsManager there is a "name" attribute different from the "displayName".
        // I doubt it is used in the UX, but for backwards compatibility we will not leave this
        // column null. This value is not provided in the input topology, so we will populate
        // the "name" column with the following:
        final String synthesizedEntityName = entityDTO.getDisplayName() + '-' + entityOid;
        record.setName(synthesizedEntityName);
        record.setDisplayName(entityDTO.getDisplayName());
        record.setUuid(Long.toString(entityOid));
        record.setCreationClass(entityType);
        record.setCreatedAt(new Timestamp(snapshotTime));
        return Optional.of(record);
    }

    /**
     * Factory to create new writer instances.
     */
    public static class Factory extends TopologyWriterBase.Factory {

        private final HistorydbIO historydbIO;

        /**
         * Create a new factory instance.
         *
         * @param historydbIO database utils
         */
        public Factory(HistorydbIO historydbIO) {
            this.historydbIO = historydbIO;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>>
        getChunkProcessor(final TopologyInfo topologyInfo,
                          final SimpleBulkLoaderFactory loaders) {
            return Optional.of(new EntitiesWriter(topologyInfo, historydbIO, loaders));
        }
    }
}
