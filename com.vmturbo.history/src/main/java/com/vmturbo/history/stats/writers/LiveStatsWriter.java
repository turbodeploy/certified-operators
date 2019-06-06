/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.stats.live.LiveStatsAggregator;

/**
 * {@link LiveStatsWriter} writes live statistics into the database.
 */
public class LiveStatsWriter extends AbstractStatsWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    private final HistorydbIO historydbIO;
    // the number of entities for which stats are persisted in a single DB Insert operations
    private final int writeTopologyChunkSize;

    // commodities in this list will not be persisted to the DB; examples are Access Commodities
    // note that the list will be small so using a HashSet is not necessary.
    private final Set<String> commoditiesToExclude;

    /**
     * Creates {@link LiveStatsWriter} instance.
     *
     * @param historydbIO database IO component used for interaction
     * @param writeTopologyChunkSize the number of entities for which stats are
     *                 persisted in a single DB Insert operations
     * @param commoditiesToExclude commodities in this list will not be persisted to
     *                 the DB; examples are Access Commodities note that the list will be small so
     *                 using a HashSet is not necessary.1
     */
    public LiveStatsWriter(@Nonnull HistorydbIO historydbIO, int writeTopologyChunkSize,
                    @Nonnull Set<String> commoditiesToExclude) {
        this.historydbIO = historydbIO;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
        this.commoditiesToExclude = commoditiesToExclude;
    }

    @Override
    protected int process(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> objects) throws VmtDbException {
        final Stopwatch chunkTimer = Stopwatch.createStarted();
        // create class to buffer chunks of stats and insert in batches for efficiency
        final LiveStatsAggregator aggregator = new LiveStatsAggregator(historydbIO, topologyInfo,
                        ImmutableList.copyOf(commoditiesToExclude), writeTopologyChunkSize);
        // look up existing entity information
        chunkTimer.reset().start();
        // collect entity oids, and put entities into a map indexed by oid
        final List<String> chunkOIDs = new ArrayList<>();
        final Map<Long, TopologyEntityDTO> entityByOid = new HashMap<>();
        for (TopologyEntityDTO entity : objects) {
            chunkOIDs.add(String.valueOf(entity.getOid()));
            entityByOid.put(entity.getOid(), entity);
        }

        final Map<Long, EntitiesRecord> knownChunkEntities = historydbIO.getEntities(chunkOIDs);
        LOGGER.debug("time to look up entities: {}", chunkTimer);

        // process all the TopologyEntityDTOs
        chunkTimer.reset().start();
        final List<EntitiesRecord> entityRecordsToPersist = new ArrayList<>();
        final long snapshotTime = topologyInfo.getCreationTime();
        for (TopologyEntityDTO entityDTO : objects) {
            // persist this entity if necessary
            final Optional<EntitiesRecord> record =
                            createRecord(entityDTO.getOid(), snapshotTime, entityDTO,
                                            knownChunkEntities);
            record.ifPresent(entityRecordsToPersist::add);
            aggregator.aggregateEntity(entityDTO, entityByOid);
        }
        historydbIO.persistEntities(entityRecordsToPersist);
        LOGGER.debug("time to persist entities: {} number of entities: {}",
                        chunkTimer.stop()::toString, objects::size);
        // write the per-entity-type aggregate stats (e.g. counts), and write the partial chunks
        aggregator.writeFinalStats();
        return objects.size();
    }

    /**
     * Creates db record, based on topology entity and potentially, on existing record from the DB.
     * This method will return db record, if some operations (modification or insertion) is required
     * for the specified entity. A {@link TopologyEntityDTO} needs to be persisted if:
     * <ol>
     * <li>its OID is not found in the DB, or
     * <li>its Creation Class (EntityType) has changed, or
     * <li>its Display Name has changed.
     * </ol>
     *
     * @param oid then entity to generate a DB record for
     * @param snapshotTime time for this topology
     * @param entityDTO the TopologyEntityDTO for the Service Entity to persist
     * @param existingRecords the map of known oid -> record in the DB
     * @return an Optional containing an EntitiesRecord to be persisted - either a new
     *                 record to be inserted or an updated copy of the prior record if modified; or
     *                 Optional.empty() if this particular entity type is not to be persisted to the
     *                 DB
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
        final EntitiesRecord existingRecord = existingRecords.get(oid);
        if (existingRecord == null) {
            record = new EntitiesRecord();
            record.setId(entityOid);
        } else {
            if (existingRecord.getDisplayName().equals(entityDTO.getDisplayName()) && existingRecord
                            .getCreationClass().equals(entityType)) {
                return Optional.empty();
            }
            LOGGER.warn("Name or type for existing entity with oid {} has been changed: "
                                            + "displayName >{}< -> >{}<"
                                            + " creationType >{}< -> >{}<; db updated.", entityDTO.getOid(),
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
