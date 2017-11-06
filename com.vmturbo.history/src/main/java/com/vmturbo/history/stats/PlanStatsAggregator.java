package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.StringConstants.NUM_CNT_PER_HOST;
import static com.vmturbo.reports.db.StringConstants.NUM_CNT_PER_STORAGE;
import static com.vmturbo.reports.db.StringConstants.NUM_CONTAINERS;
import static com.vmturbo.reports.db.StringConstants.NUM_HOSTS;
import static com.vmturbo.reports.db.StringConstants.NUM_STORAGES;
import static com.vmturbo.reports.db.StringConstants.NUM_VMS;
import static com.vmturbo.reports.db.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.reports.db.StringConstants.NUM_VMS_PER_STORAGE;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.tables.MktSnapshotsStats;
import com.vmturbo.reports.db.abstraction.tables.records.MktSnapshotsStatsRecord;

/**
 * Aggregates plan topology stats by commodity type and by entity type.
 *
 */
public class PlanStatsAggregator {

    private final Logger logger = LogManager.getLogger();

    private Map<Integer, MktSnapshotsStatsRecord> commodityAggregate = Maps.newHashMap();
    private Map<Integer, Integer> commodityTypeCounts = Maps.newHashMap();
    private Map<Integer, Integer> entityTypeCounts = Maps.newHashMap();
    private final Timestamp snapshotTimestamp;
    private final HistorydbIO historydbIO;
    private final String dbCommodityPrefix;
    private final long topologyId;
    private final long topologyContextId;

    public PlanStatsAggregator(
                @Nonnull HistorydbIO historydbIO, @Nonnull TopologyOrganizer topologyOrganizer,
                String prefix) {
        topologyId = topologyOrganizer.getTopologyId();
        topologyContextId = topologyOrganizer.getTopologyContextId();
        snapshotTimestamp = new Timestamp(topologyOrganizer.getSnapshotTime());
        this.historydbIO = historydbIO;
        dbCommodityPrefix = prefix == null ? "" : prefix;
    }

    /**
     * An immutable view of the entity type counts map - for testing.
     * @return an immutable view of the entity counts map
     */
    protected Map<Integer, Integer> getEntityTypeCounts() {
        return Collections.unmodifiableMap(entityTypeCounts);
    }

    /**
     * An immutable view of the commodity type counts map - for testing.
     * @return an immutable view of the commodity counts map
     */
    protected Map<Integer, Integer> getCommodityTypeCounts() {
        return Collections.unmodifiableMap(commodityTypeCounts);
    }

    /**
     * Handle one chunk of topology DTOs.
     * @param chunk a collection of topology DTOs
     */
    public void handleChunk(Collection<TopologyEntityDTO> chunk) {
        aggregateCommodities(chunk);
        countTypes(chunk);
    }

    /**
     * Update the entity type counters.
     * @param chunk one chunk of topology DTOs.
     */
    private void countTypes(Collection<TopologyEntityDTO> chunk) {
        chunk.stream()
            .filter(dto -> dto.getEntityState() != EntityState.SUSPENDED)
            .map(TopologyEntityDTO::getEntityType)
            .forEach(this::increment);
    }

    /**
     * Increment by one the count of the provided entity type.
     * @param entityType numerical entity type
     */
    private void increment(int entityType ) {
        Integer count = entityTypeCounts.computeIfAbsent(entityType, j -> 0);
        entityTypeCounts.put(entityType, ++count);
    }

    /**
     * Create the DB stats records for entity counts.
     * We record number of PMs, average number of VMs per PM, average number of containers per
     * PM, number of VMs, number of storages, average number of VMs per storage, average number
     * of containers per storage and number of containers.
     * @return a collection of stats records to be written to the DB
     */
    private Collection<MktSnapshotsStatsRecord> entityCountRecords() {
        Collection<MktSnapshotsStatsRecord> entityTypeCountRecords = Lists.newArrayList();
        int numPMs = entityTypeCounts.getOrDefault(EntityType.PHYSICAL_MACHINE_VALUE, 0);
        int numVMs = entityTypeCounts.getOrDefault(EntityType.VIRTUAL_MACHINE_VALUE, 0);
        int numContainers = entityTypeCounts.getOrDefault(EntityType.CONTAINER_VALUE, 0);
        int numStorages = entityTypeCounts.getOrDefault(EntityType.STORAGE_VALUE, 0);
        logger.debug("Entity type counts for topology id {} and context id {} :"
                    + " {} PMs, {} VMs, {} containers, {} storages.",
                    topologyId, topologyContextId,
                    numPMs, numVMs, numContainers, numStorages);
        if (numPMs != 0) {
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numPMs,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_HOSTS, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numVMs / numPMs,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_VMS_PER_HOST, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numContainers / numPMs,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_CNT_PER_HOST, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
        }
        if (numVMs != 0) {
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numVMs,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_VMS, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
        }
        if (numStorages != 0) {
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numStorages,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_STORAGES, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numVMs / numStorages,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_VMS_PER_STORAGE, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numContainers / numStorages,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_CNT_PER_STORAGE, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
        }
        if (numContainers != 0) {
            entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                    snapshotTimestamp, numContainers,
                    null /*capacity*/,
                    HistoryStatsUtils.addPrefix(NUM_CONTAINERS, dbCommodityPrefix),
                    null /* propertySubtype*/,
                    topologyContextId));
        }

        return entityTypeCountRecords;
    }

    /**
     * Handle one chunk of of topology DTOs in creating aggregate stats (min, max, avg) DB records.
     * @param chunk one chunk of topology DTOs
     */
    private void aggregateCommodities(Collection<TopologyEntityDTO> chunk) {
        for (TopologyDTO.TopologyEntityDTO entityDTO : chunk) {
            for (TopologyDTO.CommoditySoldDTO commoditySoldDTO : entityDTO.getCommoditySoldListList()) {
                final int commodityType = commoditySoldDTO.getCommodityType().getType();
                double used = commoditySoldDTO.getUsed();
                double capacity = commoditySoldDTO.getCapacity();
                Integer commodityCount = commodityTypeCounts.get(commodityType);
                MktSnapshotsStatsRecord commodityRecord = commodityAggregate.get(commodityType);
                if (commodityRecord == null) { // first time encountering commodity type
                    final String propertyType = HistoryStatsUtils.formatCommodityName(
                            commodityType, dbCommodityPrefix);
                    final String propertySubtype = "used";
                    commodityRecord = buildMktSnapshotsStatsRecord(snapshotTimestamp, used,
                            capacity, propertyType, propertySubtype, topologyContextId);
                    commodityAggregate.put(commodityType, commodityRecord);
                    commodityCount = 0;
                } else {
                    commodityRecord.setMinValue(Math.min(used, commodityRecord.getMinValue()));
                    commodityRecord.setMaxValue(Math.max(used, commodityRecord.getMaxValue()));
                    // in the first phase we use the "avgValue" field to store the sum of used
                    commodityRecord.setAvgValue(used + commodityRecord.getAvgValue());
                    commodityRecord.setCapacity(capacity);
                }
                commodityTypeCounts.put(commodityType, ++commodityCount);
            }
        }
    }

    /**
     * Construct the list of records to be written to the DB. These include
     * both the topology counter records and the commodities sold aggregated
     * records.
     *
     * @return an unmodifiable list of the records
     */
    public List<MktSnapshotsStatsRecord> statsRecords() {
        // calculate averages, using the sum of used values from avgValue and counts
        commodityAggregate.forEach((commodityType, commodityRecord) ->
            commodityRecord.setAvgValue(commodityRecord.getAvgValue() /
                    commodityTypeCounts.get(commodityType)));
        List<MktSnapshotsStatsRecord> result = Lists.newArrayList(entityCountRecords());
        result.addAll(commodityAggregate.values());
        return Collections.unmodifiableList(result);
    }

    /**
     * Write aggregated data to the DB.
     * @throws VmtDbException when writing to the DB fails
     */
    public void writeAggregates() throws VmtDbException {
        // TODO: write in batch
        try (Connection conn = historydbIO.connection()) {
            for (MktSnapshotsStatsRecord snapshotStatRecord : statsRecords()) {
                historydbIO.execute(HistorydbIO.getJooqBuilder()
                    .insertInto(MktSnapshotsStats.MKT_SNAPSHOTS_STATS)
                    .set(snapshotStatRecord), conn);
            }
        } catch (SQLException | DataAccessException e) {
            throw new VmtDbException(VmtDbException.INSERT_ERR, "Error persisting" +
                            topologyContextId, e);
        }
    }

    /**
     * Construct a record to write to mkt_snapshots_stats table representing a stat value
     * for a particular snapshot. This may be a commodity, entity count, action count, etc. stat.
     *
     * @param snapshotTimestamp timestamp for the snapshot being persisted
     * @param used current value to persist
     * @param capacity capacity value to persist
     * @param propertyType general type for this stat
     * @param propertySubtype subtype for this stat, may be null
     * @param topologyContextId id for the planning context
     * @return a new MktSnapshotsStatsRecord containing the given values
     */
    private MktSnapshotsStatsRecord buildMktSnapshotsStatsRecord(
                    @Nonnull Timestamp snapshotTimestamp, double used, Double capacity,
                    @Nonnull String propertyType, @Nullable String propertySubtype,
                    long topologyContextId) {
        MktSnapshotsStatsRecord commodityRecord = new MktSnapshotsStatsRecord();
        commodityRecord.setRecordedOn(snapshotTimestamp);
        commodityRecord.setMktSnapshotId(topologyContextId);
        commodityRecord.setPropertyType(propertyType);
        commodityRecord.setPropertySubtype(propertySubtype);
        commodityRecord.setMinValue(used);
        commodityRecord.setMaxValue(used);
        commodityRecord.setAvgValue(used);
        commodityRecord.setCapacity(capacity);
        commodityRecord.setProjectionTime(snapshotTimestamp);
        return commodityRecord;
    }
}
