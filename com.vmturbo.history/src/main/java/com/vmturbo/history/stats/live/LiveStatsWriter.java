package com.vmturbo.history.stats.live;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.utils.SystemLoadHelper;

/**
 * Persist information in a Live Topology to the History DB. This includes persisting the entities,
 * and for each entity write the stats, including commodities and entity attributes.
 **/
public class LiveStatsWriter {

    private final HistorydbIO historydbIO;

    // the number of entities for which stats are persisted in a single DB Insert operations
    private final int writeTopologyChunkSize;

    // commodities in this list will not be persisted to the DB; examples are Access Commodities
    // note that the list will be small so using a HashSet is not necessary.
    private final ImmutableList<String> commoditiesToExclude;

    private final SystemLoadSnapshot snapshot = new SystemLoadSnapshot();

    private static final Logger logger = LogManager.getLogger();

    public LiveStatsWriter(HistorydbIO historydbIO, int writeTopologyChunkSize,
                           ImmutableList<String> commoditiesToExclude) {
        this.historydbIO = historydbIO;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
        this.commoditiesToExclude = commoditiesToExclude;
    }

    public int processChunks(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<TopologyEntityDTO> dtosIterator,
            GroupServiceBlockingStub groupServiceClient,
            SystemLoadHelper systemLoadHelper
    ) throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        // read all the DTO chunks before any processing
        Collection<TopologyEntityDTO> allTopologyDTOs = Lists.newArrayList();
        int chunkNumber = 0;
        int numberOfEntities = 0;
        Stopwatch chunkTimer = Stopwatch.createStarted();
        // TODO (roman, Dec 19 2018) OM-41526: Process in chunks instead of collecting all DTOs.
        while (dtosIterator.hasNext()) {
            Collection<TopologyEntityDTO> chunk = dtosIterator.nextChunk();

            numberOfEntities += chunk.size();
            logger.debug("Received chunk #{} of size {} for topology {} and context {} [soFar={}]",
                ++chunkNumber, chunk.size(), topologyInfo.getTopologyId(),
                topologyInfo.getTopologyContextId(), numberOfEntities);
            allTopologyDTOs.addAll(chunk);
        }
        logger.debug("time to receive chunks & organize: {}", chunkTimer);

        // create class to buffer chunks of stats and insert in batches for efficiency
        LiveStatsAggregator aggregator = new LiveStatsAggregator(historydbIO, topologyInfo,
                commoditiesToExclude, writeTopologyChunkSize);
        // look up existing entity information
        chunkTimer.reset().start();
        // collect entity oids, and put entities into a map indexed by oid
        final List<String> chunkOIDs = new ArrayList<>();
        final Map<Long, TopologyEntityDTO> entityByOid = new HashMap<>();
        for (TopologyEntityDTO entity : allTopologyDTOs) {
            chunkOIDs.add(String.valueOf(entity.getOid()));
            entityByOid.put(entity.getOid(), entity);
        }

        Map<Long, EntitiesRecord> knownChunkEntities = historydbIO.getEntities(chunkOIDs);
        logger.debug("time to look up entities: {}", chunkTimer);

        // process all the TopologyEntityDTOs
        chunkTimer.reset().start();
        final List<EntitiesRecord> entityRecordsToPersist = new ArrayList<>();
        final long snapshotTime = topologyInfo.getCreationTime();
        for (TopologyEntityDTO entityDTO : allTopologyDTOs) {
            // persist this entity if necessary
            final Optional<EntitiesRecord> record = createRecord(entityDTO.getOid(), snapshotTime,
                    entityDTO, knownChunkEntities);
            record.ifPresent(entityRecordsToPersist::add);
            aggregator.aggregateEntity(entityDTO, entityByOid);
        }
        historydbIO.persistEntities(entityRecordsToPersist);
        logger.debug("time to persist entities: {} number of entities: {}", chunkTimer,
                allTopologyDTOs.size());
        // write the per-entity-type aggregate stats (e.g. counts), and write the partial chunks
        aggregator.writeFinalStats();

        logger.info("Done handling topology notification for realtime topology {} and context {}."
                        + " Number of entities: {}", topologyInfo.getTopologyId(),
                        topologyInfo.getTopologyContextId(), numberOfEntities);
        if (groupServiceClient != null) {
            snapshot.saveSnapshot(allTopologyDTOs, groupServiceClient, systemLoadHelper);
        }

        return numberOfEntities;
    }

    /**
     * Creates db record, based on topology entity and potentially, on existing record from the DB.
     * This method will return db record, if some operations (modification or insertion) is required
     * for the specified entity.
     *
     * A {@link TopologyEntityDTO} needs to be persisted if:
     * <ol>
     *     <li>its OID is not found in the DB, or
     *     <li>its Creation Class (EntityType) has changed, or
     *     <li>its Display Name has changed.
     * </ol>
     *
     * @param oid then entity to generate a DB record for
     * @param snapshotTime time for this topology
     * @param entityDTO the TopologyEntityDTO for the Service Entity to persist
     * @param existingRecords the map of known oid -> record in the DB
     * @return an Optional containing an EntitiesRecord to be persisted - either a new
     * record to be inserted or an updated copy of the prior record if modified; or Optional.empty()
     * if this particular entity type is not to be persisted to the DB
     */
    @Nonnull
    private Optional<EntitiesRecord> createRecord(long oid, long snapshotTime,
                                                  @Nonnull TopologyEntityDTO entityDTO,
                                                  @Nonnull Map<Long, EntitiesRecord> existingRecords) {

        final Optional<EntityType> entityDBInfo =
                historydbIO.getEntityType(entityDTO.getEntityType());
        if (!entityDBInfo.isPresent()) {
            // entity type not found - some entity types are not persisted
            return Optional.empty();
        }

        final String entityType = entityDBInfo.get().getClsName();
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
}