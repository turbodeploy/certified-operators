package com.vmturbo.history.stats;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.topology.TopologySnapshotRegistry;
import com.vmturbo.history.utils.TopologyOrganizer;

/**
 * Persist information in a Live Topology to the History DB. This includes persisting the entities,
 * and for each entity write the stats, including commodities and entity attributes.
 **/
public class LiveStatsWriter {

    private final HistorydbIO historydbIO;

    // the number of entities for which stats are persisted in a single DB Insert operations
    private final int writeTopologyChunkSize;

    // commodities in this list will not be pesisted to the DB; examples are Access Commodities
    // note that the list will be small so using a HashSet is not necessary.
    private final ImmutableList<String> commoditiesToExclude;

    private static final Logger logger = LogManager.getLogger();

    /**
     * a utility class to coordinate async receipt of Topology and PriceIndex messages for the
     * same topologyContext.
     */
    private final TopologySnapshotRegistry snapshotRegistry;

    public LiveStatsWriter(TopologySnapshotRegistry topologySnapshotRegistry,
                           HistorydbIO historydbIO, int writeTopologyChunkSize,
                           ImmutableList<String> commoditiesToExclude) {
        this.historydbIO = historydbIO;
        this.snapshotRegistry = topologySnapshotRegistry;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
        this.commoditiesToExclude = commoditiesToExclude;
    }

    /**
     * Handle receipt of an invalid topology. Record in the {@link TopologySnapshotRegistry} that
     * the corresponding PriceIndex information should be discarded.
     *
     * @param topologyContextId the topology context for the invalid topology
     * @param topologyId the unique id for the invalid topology
     */
    public void invalidTopologyReceived(long topologyContextId, long topologyId) {
        snapshotRegistry.registerInvalidTopology(topologyContextId, topologyId);
    }

    public int processChunks(
            @Nonnull TopologyOrganizer topologyOrganizer,
            @Nonnull RemoteIterator<TopologyEntityDTO> dtosIterator
    ) throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        // read all the DTO chunks before any processing
        Collection<TopologyEntityDTO> allTopologyDTOs = Lists.newArrayList();
        int chunkNumber = 0;
        int numberOfEntities = 0;
        Stopwatch chunkTimer = Stopwatch.createStarted();
        while (dtosIterator.hasNext()) {
            Collection<TopologyEntityDTO> chunk = dtosIterator.nextChunk();

            numberOfEntities += chunk.size();
            logger.debug("Received chunk #{} of size {} for topology {} and context {} [soFar={}]",
                ++chunkNumber, chunk.size(), topologyOrganizer.getTopologyId(),
                topologyOrganizer.getTopologyContextId(), numberOfEntities);
            allTopologyDTOs.addAll(chunk);
        }
        logger.debug("time to receive chunks & organize: {}", chunkTimer);

        // create class to buffer chunks of stats and insert in batches for efficiency
        LiveStatsAggregator aggregator = new LiveStatsAggregator(historydbIO, topologyOrganizer,
                commoditiesToExclude, writeTopologyChunkSize);
        // look up existing entity information
        chunkTimer.reset().start();
        List<String> chunkOIDs = allTopologyDTOs.stream()
                .map(TopologyEntityDTO::getOid)
                .map(String::valueOf)
                .collect(Collectors.toList());
        Map<Long, EntitiesRecord> knownChunkEntities = historydbIO.getEntities(chunkOIDs);
        logger.debug("time to look up entities: {}", chunkTimer);

        // process all the TopologyEntityDTOs
        chunkTimer.reset().start();
        final List<EntitiesRecord> entityRecordsToPersist = new ArrayList<>();
        final long snapshotTime = topologyOrganizer.getSnapshotTime();
        for (TopologyEntityDTO entityDTO : allTopologyDTOs) {
            // persist this entity if necessary
            final Optional<EntitiesRecord> record = createRecord(entityDTO.getOid(), snapshotTime,
                    entityDTO, knownChunkEntities);
            record.ifPresent(entityRecordsToPersist::add);
            // save the type information for processing priceIndex message
            topologyOrganizer.addEntityType(entityDTO);
            aggregator.aggregateEntity(entityDTO);
        }
        historydbIO.persistEntities(entityRecordsToPersist);
        logger.debug("time to persist entities: {} number of entities: {}", chunkTimer,
                allTopologyDTOs.size());
        // write the per-entity-type aggregate stats (e.g. counts), and write the partial chunks
        aggregator.writeFinalStats();

        // register that this topology has been processed; may trigger additional processing
        snapshotRegistry.registerTopologySnapshot(topologyOrganizer.getTopologyContextId(),
            topologyOrganizer);
        logger.info("Done handling topology notification for realtime topology {} and context {}."
                        + " Number of entities: {}", topologyOrganizer.getTopologyId(),
                        topologyOrganizer.getTopologyContextId(), numberOfEntities);

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