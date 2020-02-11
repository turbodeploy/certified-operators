package com.vmturbo.history.stats;

import static com.vmturbo.components.common.utils.StringConstants.NUM_CNT_PER_HOST;
import static com.vmturbo.components.common.utils.StringConstants.NUM_CNT_PER_STORAGE;
import static com.vmturbo.components.common.utils.StringConstants.NUM_CONTAINERPODS;
import static com.vmturbo.components.common.utils.StringConstants.NUM_CONTAINERS;
import static com.vmturbo.components.common.utils.StringConstants.NUM_CPUS;
import static com.vmturbo.components.common.utils.StringConstants.NUM_HOSTS;
import static com.vmturbo.components.common.utils.StringConstants.NUM_STORAGES;
import static com.vmturbo.components.common.utils.StringConstants.NUM_VMS;
import static com.vmturbo.components.common.utils.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.components.common.utils.StringConstants.NUM_VMS_PER_STORAGE;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.abstraction.tables.MktSnapshotsStats;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Aggregates plan topology stats by commodity type and by entity type.
 */
public class PlanStatsAggregator {

    private final Logger logger = LogManager.getLogger();

    private static final String NO_COMMODITY_PREFIX = "";
    private final SimpleBulkLoaderFactory loaders;
    private final HistorydbIO historydbIO;

    private Map<Integer, MktSnapshotsStatsRecord> commodityAggregate = Maps.newHashMap();
    private Map<Integer, Integer> commodityTypeCounts = Maps.newHashMap();
    private Map<Integer, Integer> entityTypeCounts = Maps.newHashMap();
    private Map<String, Integer> entityMetrics = Maps.newHashMap();
    private final Timestamp snapshotTimestamp;
    private final boolean isProcessingSourceTopologyStats;
    private final String dbCommodityPrefix;
    private final long topologyId;
    private final long topologyContextId;

    /**
     * Create a new instance.
     * @param loaders                         bulk loader factory
     * @param historydbIO                     DB methods
     * @param topologyInfo                    topology info
     * @param isProcessingSourceTopologyStats whether we are processing source topology stats
     */
    public PlanStatsAggregator(@Nonnull SimpleBulkLoaderFactory loaders,
                               @Nonnull HistorydbIO historydbIO,
                               @Nonnull TopologyInfo topologyInfo,
                               boolean isProcessingSourceTopologyStats) {
        topologyId = topologyInfo.getTopologyId();
        topologyContextId = topologyInfo.getTopologyContextId();
        snapshotTimestamp = new Timestamp(topologyInfo.getCreationTime());
        this.loaders = loaders;
        this.historydbIO = historydbIO;
        this.isProcessingSourceTopologyStats = isProcessingSourceTopologyStats;
        dbCommodityPrefix = isProcessingSourceTopologyStats ? StringConstants.STAT_PREFIX_CURRENT : NO_COMMODITY_PREFIX;
    }

    /**
     * An immutable view of the entity type counts map - for testing.
     * @return an immutable view of the entity counts map
     */
    @VisibleForTesting
    protected Map<Integer, Integer> getEntityTypeCounts() {
        return Collections.unmodifiableMap(entityTypeCounts);
    }

    /**
     * An immutable view of the commodity type counts map - for testing.
     * @return an immutable view of the commodity counts map
     */
    @VisibleForTesting
    protected Map<Integer, Integer> getCommodityTypeCounts() {
        return Collections.unmodifiableMap(commodityTypeCounts);
    }

    /**
     * An immutable view of the entity metrics counts map - for testing.
     * @return an immutable view of the entity metrics map
     */
    @VisibleForTesting
    protected Map<String, Integer> getEntityMetrics() {
        return Collections.unmodifiableMap(entityMetrics);
    }

    /**
     * Handle one chunk of topology DTOs.
     * @param chunk a collection of topology DTOs
     */
    public void handleChunk(Collection<TopologyEntityDTO> chunk) {
        Collection<TopologyEntityDTO> entitiesToCount = chunk.stream()
                .filter(this::shouldCountEntity)
                .collect(Collectors.toSet());
        countTypes(entitiesToCount);
        countMetrics(entitiesToCount);
        aggregateCommodities(entitiesToCount);
    }

    /**
     * Update the entity type counters.
     * @param chunk one chunk of topology DTOs.
     */
    private void countTypes(Collection<TopologyEntityDTO> chunk) {
        chunk.stream()
            // Do not filter here! All filtering should be done in shouldCountEntity
            .map(TopologyEntityDTO::getEntityType)
            .forEach(this::increment);

        logger.debug("Entity Counts:\n {}", () -> getEntityCountDump(chunk));
    }

    /**
     * Iterates collection and updates entity type-specific metrics.
     *
     * @param chunk one chunk of topology DTOs.
     */
    private void countMetrics(Collection<TopologyEntityDTO> chunk) {
        chunk.stream().forEach(countPhysicalMachineMetrics);
    }

    /**
     * Update metrics related to physicalMachine entities.
     *
     * <p>Currently only handles numCPUs</p>
     */
    private Consumer<TopologyEntityDTO> countPhysicalMachineMetrics = (topologyEntityDTO) ->  {
        if (!(topologyEntityDTO.hasTypeSpecificInfo() && topologyEntityDTO.getTypeSpecificInfo().hasPhysicalMachine())) {
            return;
        }

        if (topologyEntityDTO.getTypeSpecificInfo().getPhysicalMachine().hasNumCpus()) {
            int numCpus = topologyEntityDTO.getTypeSpecificInfo().getPhysicalMachine().getNumCpus();
            int currentValue = this.entityMetrics.computeIfAbsent(StringConstants.NUM_CPUS, j -> 0);
            this.entityMetrics.put(StringConstants.NUM_CPUS, currentValue + numCpus);
        }
    };

    // this function is only for debugging. It's an ugly function, but very useful for seeing
    // breakdowns of the contents of a topology having stats aggregated.
    // TODO: remove this after the plan results are solid.
    private String getEntityCountDump(Collection<TopologyEntityDTO> chunk) {
        if (chunk.size() <= 0) {
            return "No entities.";
        }
        // create a dump of the raw entity counts
        StringBuilder sb = new StringBuilder("Raw Entity Counts:\n");
        Map<String,Integer> entityTypeStateCounts = new HashMap<>();
        chunk.stream()
                .map(dto -> EntityType.forNumber(dto.getEntityType()).name() +":"+ dto.getEntityState().name())
                .forEach(key -> entityTypeStateCounts.merge(key,1, (i,d) -> i+d ));
        entityTypeStateCounts.entrySet().forEach(entry -> sb.append("  ")
                .append(entry.getKey()).append(":").append(entry.getValue()).append("\n"));

        // dump unplaced entity counts
        Map<String,Integer> unplacedEntityTypeStateCounts = new HashMap<>();
        chunk.stream().filter(dto -> !TopologyDTOUtil.isPlaced(dto))
                .map(dto -> EntityType.forNumber(dto.getEntityType()).name() +":"+ dto.getEntityState().name())
                .forEach(key -> unplacedEntityTypeStateCounts.merge(key,1, (i,d) -> i+d ));
        sb.append("Unplaced entity counts:\n");
        unplacedEntityTypeStateCounts.entrySet().forEach(entry -> sb.append("  ")
                .append(entry.getKey()).append(":").append(entry.getValue()).append("\n"));

        // count the entities with plan origins too
        Map<String,Integer> planEntityCounts = new HashMap<>();
        chunk.stream()
                .filter(dto -> !shouldCountEntity(dto))
                .map(dto -> EntityType.forNumber(dto.getEntityType()).name() +":"+ dto.getEntityState().name())
                .forEach(key -> planEntityCounts.merge(key,1, (i,d) -> i+d ));
        sb.append("Entity w/Plan Origin counts:\n");
        planEntityCounts.entrySet().forEach(entry -> sb.append("  ")
                .append(entry.getKey()).append(":").append(entry.getValue()).append("\n"));
        return sb.toString();
    }

    /**
     * Should we count this entity? If we are processing source topology stats, then we will skip
     * any entities that were added in plan scenarios. Since these were not part of the original
     * topology, they should not be counted in the "before plan" stats.
     *
     * @param  entity the entity to check if we should count
     * @return true, if this entity should be included in the stats.
     */
    private boolean shouldCountEntity(TopologyEntityDTO entity) {
        // Suspended entities should not appear in the counts
        final boolean entitySuspended = entity.getEntityState() == EntityState.SUSPENDED;
        // Unplaced entities (generally VMs) should not appear in the counts
        final boolean entityPlaced = TopologyDTOUtil.isPlaced(entity);
        final boolean unplacedVm = EntityType.VIRTUAL_MACHINE_VALUE == entity.getEntityType()
            && !entityPlaced;
        // Only filter scenario additions from the SOURCE topology
        final boolean scenarioAddition = isProcessingSourceTopologyStats
            && entity.hasOrigin()
            && entity.getOrigin().hasPlanScenarioOrigin();
        return !unplacedVm && !scenarioAddition && !entitySuspended;
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
     * Create DB stats records for entity Metrics.
     * <p>Currently only numCPUs metric is recorded</p>
     *
     * @return a collection of stats records to be written to the DB
     */
    private Collection<MktSnapshotsStatsRecord> entityMetricsRecords() {
        Collection<MktSnapshotsStatsRecord> entityTypeCountRecords = Lists.newArrayList();
        int numCPUs = entityMetrics.getOrDefault(StringConstants.NUM_CPUS, 0);

        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numCPUs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CPUS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));

        return entityTypeCountRecords;
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
        int numContainerPods = entityTypeCounts.getOrDefault(EntityType.CONTAINER_POD_VALUE, 0);
        logger.debug("Entity type counts for topology id {} and context id {} :"
                        + " {} PMs, {} VMs, {} containers, {} containerPods, {} storages.",
                        topologyId, topologyContextId,
                        numPMs, numVMs, numContainers, numContainerPods, numStorages);
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numPMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_HOSTS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numPMs == 0 ? 0 : numVMs / numPMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_VMS_PER_HOST, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numPMs == 0 ? 0 : numContainers / numPMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CNT_PER_HOST, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numVMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_VMS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numStorages, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_STORAGES, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numStorages == 0 ? 0 : numVMs / numStorages, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_VMS_PER_STORAGE, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numStorages == 0 ? 0 : numContainers / numStorages, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CNT_PER_STORAGE, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numContainers, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CONTAINERS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numContainerPods, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CONTAINERPODS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId));

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
                if (commodityType == CommodityType.STORAGE_AMOUNT.getNumber()
                    && EntityType.STORAGE.getNumber() != entityDTO.getEntityType()) {
                    // Storage commodity counts are expected to be the aggregation of all
                    // StorageAmount sold by all storage devices. We don't want to double count the
                    // StorageAmount sold by other entity types (e.g. DiskArrays).
                    // TODO (OM-55121): Generalize this to aggregate each commodity per entity type.
                    continue;
                }
                double used = commoditySoldDTO.getUsed();
                double capacity = commoditySoldDTO.getCapacity();
                Integer commodityCount = commodityTypeCounts.get(commodityType);
                MktSnapshotsStatsRecord commodityRecord = commodityAggregate.get(commodityType);
                if (commodityRecord == null) { // first time encountering commodity type
                    final String propertyType = HistoryStatsUtils.formatCommodityName(
                            commodityType, dbCommodityPrefix);
                    final String propertySubtype = PropertySubType.Used.getApiParameterName();
                    commodityRecord = buildMktSnapshotsStatsRecord(snapshotTimestamp, used,
                            capacity, propertyType, propertySubtype, topologyContextId);
                    commodityAggregate.put(commodityType, commodityRecord);
                    commodityCount = 0;
                } else {
                    commodityRecord.setMinValue(historydbIO.clipValue(Math.min(used, commodityRecord.getMinValue())));
                    commodityRecord.setMaxValue(historydbIO.clipValue(Math.max(used, commodityRecord.getMaxValue())));
                    // in the first phase we use the "avgValue" field to store the sum of used
                    commodityRecord.setAvgValue(historydbIO.clipValue(used + commodityRecord.getAvgValue()));
                    // Capacity is the aggregate of all the individual commodity capacities
                    commodityRecord.setCapacity(historydbIO.clipValue(capacity + commodityRecord.getCapacity()));
                }
                commodityTypeCounts.put(commodityType, ++commodityCount);
            }
        }
    }

    /**
     * Construct the list of records to be written to the DB. These include
     * both the topology counter records, entityMetrics and the commodities sold
     * aggregated records.
     *
     * @return an unmodifiable list of the records
     */
    public List<MktSnapshotsStatsRecord> statsRecords() {
        // calculate averages, using the sum of used values from avgValue and counts
        commodityAggregate.forEach((commodityType, commodityRecord) ->
            commodityRecord.setAvgValue(historydbIO.clipValue(commodityRecord.getAvgValue() /
                    commodityTypeCounts.get(commodityType))));
        List<MktSnapshotsStatsRecord> result = Lists.newArrayList(entityCountRecords());
        result.addAll(entityMetricsRecords());
        result.addAll(commodityAggregate.values());
        return Collections.unmodifiableList(result);
    }

    /**
     * Write aggregated data to the DB.
     * @throws InterruptedException if interrupted
     */
    public void writeAggregates() throws InterruptedException {
        loaders.getLoader(MktSnapshotsStats.MKT_SNAPSHOTS_STATS).insertAll(statsRecords());
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
        commodityRecord.setMinValue(historydbIO.clipValue(used));
        commodityRecord.setMaxValue(historydbIO.clipValue(used));
        commodityRecord.setAvgValue(historydbIO.clipValue(used));
        if (capacity != null) {
            commodityRecord.setCapacity(historydbIO.clipValue(capacity));
        }
        commodityRecord.setProjectionTime(snapshotTimestamp);
        return commodityRecord;
    }
}
