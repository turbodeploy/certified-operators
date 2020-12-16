package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CNT_PER_HOST;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CNT_PER_STORAGE;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CONTAINERPODS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CONTAINERS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_CPUS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_HOSTS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_STORAGES;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_STORAGE;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.abstraction.tables.MktSnapshotsStats;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Aggregates plan topology stats by commodity type and by entity type.
 */
public class PlanStatsAggregator implements MemReporter {

    private final Logger logger = LogManager.getLogger();

    private static final String NO_COMMODITY_PREFIX = "";
    private final SimpleBulkLoaderFactory loaders;
    private final HistorydbIO historydbIO;

    /**
     * A Table of entityType,commodityType -> CommodityAggregation of commodities aggregated.
     */
    private final Table<Integer, Integer, CommodityAggregation> commodityAggregationTable = HashBasedTable.create();

    private final Map<Integer, Integer> entityTypeCounts = Maps.newHashMap();
    private final Map<String, Integer> entityMetrics = Maps.newHashMap();
    private final Timestamp snapshotTimestamp;
    private final boolean isProcessingSourceTopologyStats;
    private final String dbCommodityPrefix;
    private final long topologyId;
    private final long topologyContextId;

    /**
     * Entity types which support being shown as unplaced in plan result.
     */
    private static final Set<Integer> ENTITY_TYPES_SUPPORTING_UNPLACED =
        ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.CONTAINER_POD_VALUE);

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
        chunk.forEach(countPhysicalMachineMetrics);
    }

    /**
     * Update metrics related to physicalMachine entities.
     *
     * <p>Currently only handles numCPUs</p>
     */
    private final Consumer<TopologyEntityDTO> countPhysicalMachineMetrics = (topologyEntityDTO) ->  {
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
        Map<String, Integer> entityTypeStateCounts = new HashMap<>();
        chunk.stream()
                .map(dto -> EntityType.forNumber(dto.getEntityType()).name() + ":" + dto.getEntityState().name())
                .forEach(key -> entityTypeStateCounts.merge(key, 1, Integer::sum));
        entityTypeStateCounts.forEach((key1, value) -> sb.append("  ")
                .append(key1).append(":").append(value).append("\n"));

        // dump unplaced entity counts
        Map<String, Integer> unplacedEntityTypeStateCounts = new HashMap<>();
        chunk.stream().filter(dto -> !TopologyDTOUtil.isPlaced(dto))
                .map(dto -> EntityType.forNumber(dto.getEntityType()).name() + ":" + dto.getEntityState().name())
                .forEach(key -> unplacedEntityTypeStateCounts.merge(key, 1, Integer::sum));
        sb.append("Unplaced entity counts:\n");
        unplacedEntityTypeStateCounts.forEach((key1, value) -> sb.append("  ")
                .append(key1).append(":").append(value).append("\n"));

        // count the entities with plan origins too
        Map<String, Integer> planEntityCounts = new HashMap<>();
        chunk.stream()
                .filter(dto -> !shouldCountEntity(dto))
                .map(dto -> EntityType.forNumber(dto.getEntityType()).name() + ":" + dto.getEntityState().name())
                .forEach(key -> planEntityCounts.merge(key, 1, Integer::sum));
        sb.append("Entity w/Plan Origin counts:\n");
        planEntityCounts.forEach((key, value) -> sb.append("  ")
                .append(key).append(":").append(value).append("\n"));
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
        // Unplaced entities (generally VMs and ContainerPods) should not appear in the counts
        final boolean entityPlaced = TopologyDTOUtil.isPlaced(entity);
        final boolean unplacedEntities = ENTITY_TYPES_SUPPORTING_UNPLACED.contains(entity.getEntityType())
            && !entityPlaced;
        // Only filter scenario additions from the SOURCE topology
        final boolean scenarioAddition = isProcessingSourceTopologyStats
            && entity.hasOrigin()
            && entity.getOrigin().hasPlanScenarioOrigin();
        return !unplacedEntities && !scenarioAddition && !entitySuspended;
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
     *
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
                null /* propertySubtype*/, topologyContextId, EntityType.VIRTUAL_MACHINE_VALUE));

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
                null /* propertySubtype*/, topologyContextId, EntityType.PHYSICAL_MACHINE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numPMs == 0 ? 0 : (double)numVMs / numPMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_VMS_PER_HOST, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.PHYSICAL_MACHINE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numPMs == 0 ? 0 : (double)numContainers / numPMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CNT_PER_HOST, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.PHYSICAL_MACHINE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numVMs, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_VMS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.VIRTUAL_MACHINE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numStorages, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_STORAGES, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.STORAGE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numStorages == 0 ? 0 : (double)numVMs / numStorages, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_VMS_PER_STORAGE, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.STORAGE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numStorages == 0 ? 0 : (double)numContainers / numStorages, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CNT_PER_STORAGE, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.STORAGE_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numContainers, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CONTAINERS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.CONTAINER_VALUE));
        entityTypeCountRecords.add(buildMktSnapshotsStatsRecord(
                snapshotTimestamp, numContainerPods, null /*capacity*/,
                HistoryStatsUtils.addPrefix(NUM_CONTAINERPODS, dbCommodityPrefix),
                null /* propertySubtype*/, topologyContextId, EntityType.CONTAINER_POD_VALUE));

        return entityTypeCountRecords;
    }

    /**
     * Handle one chunk of of topology DTOs in creating aggregate stats (min, max, avg) DB records.
     * @param chunk one chunk of topology DTOs
     */
    private void aggregateCommodities(Collection<TopologyEntityDTO> chunk) {
        for (TopologyDTO.TopologyEntityDTO entityDTO : chunk) {
            final int entityType = entityDTO.getEntityType();
            for (TopologyDTO.CommoditySoldDTO commoditySoldDTO : entityDTO.getCommoditySoldListList()) {
                final int commodityType = commoditySoldDTO.getCommodityType().getType();
                final double used = commoditySoldDTO.getUsed();
                final double capacity = commoditySoldDTO.getCapacity();
                if (!commoditySoldDTO.hasCapacity() || capacity <= 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Skipping plan sold commodity with unset capacity {}:{}:{}",
                                        entityDTO.getOid(), commodityType,
                                        commoditySoldDTO.getCommodityType().getKey());
                    }
                    continue;
                }
                // Get the record representing the aggregation for this entityType/commodityType combo
                final CommodityAggregation commodityAggregation = commodityAggregationTable.get(entityType, commodityType);
                if (commodityAggregation == null) {
                    // first time encountering this entityType/commodityType combo
                    final String propertyType =
                        HistoryStatsUtils.formatCommodityName(commodityType, dbCommodityPrefix);
                    final String propertySubtype = PropertySubType.Used.getApiParameterName();
                    final MktSnapshotsStatsRecord commodityRecord = buildMktSnapshotsStatsRecord(
                        snapshotTimestamp, used, capacity, propertyType, propertySubtype, topologyContextId, entityType);
                    // Insert the record representing the aggregation for this entityType/commodityType combo
                    commodityAggregationTable.put(entityType, commodityType, new CommodityAggregation(commodityRecord));
                } else {
                    // Update the record representing the aggregation for this entityType/commodityType combo
                    final MktSnapshotsStatsRecord commodityRecord = commodityAggregation.getAggregatedRecord();
                    commodityRecord.setMinValue(historydbIO.clipValue(Math.min(used, commodityRecord.getMinValue())));
                    commodityRecord.setMaxValue(historydbIO.clipValue(Math.max(used, commodityRecord.getMaxValue())));
                    // in the first phase we use the "avgValue" field to store the sum of used
                    commodityRecord.setAvgValue(historydbIO.clipValue(used + commodityRecord.getAvgValue()));
                    // Capacity is the aggregate of all the individual commodity capacities
                    commodityRecord.setCapacity(historydbIO.clipValue(capacity + commodityRecord.getCapacity()));
                    // Update the count for how many times we've encountered this entityType/commodityType combo
                    commodityAggregation.incrementEntityCount();
                }
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
        // Build the results list
        List<MktSnapshotsStatsRecord> results = Lists.newArrayList(entityCountRecords());
        results.addAll(entityMetricsRecords());
        // Iterate through the commodity aggregates
        commodityAggregationTable.values().forEach(commodityAggregation -> {
            final MktSnapshotsStatsRecord commodityRecord = commodityAggregation.getAggregatedRecord();
            // calculate averages, using the sum of used values from avgValue and counts
            commodityRecord.setAvgValue(historydbIO.clipValue(
                    commodityRecord.getAvgValue()
                            / commodityAggregation.getEntityCount()));
            // add the record to the results list
            results.add(commodityRecord);
        });
        return Collections.unmodifiableList(results);
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
     * @param entityType the type of entity for which these stats are being recorded
     * @return a new MktSnapshotsStatsRecord containing the given values
     */
    private MktSnapshotsStatsRecord buildMktSnapshotsStatsRecord(
            @Nonnull Timestamp snapshotTimestamp,
            double used,
            Double capacity,
            @Nonnull String propertyType,
            @Nullable String propertySubtype,
            long topologyContextId,
            final int entityType) {
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
        commodityRecord.setEntityType((short)entityType);
        return commodityRecord;
    }

    /**
     * A representation of the aggregation of commodities encountered.
     *
     * <p>Includes both the rolled up stats and the count of entities whose commodities have been
     * rolled up.</p>
     */
    private static class CommodityAggregation {

        private int entityCount;
        private final MktSnapshotsStatsRecord aggregatedRecord;

        /**
         * Create a representation of the aggregation of commodities from multiple entities.
         *
         * @param aggregatedRecord the rolled up commodity stats
         */
        CommodityAggregation(@Nonnull final MktSnapshotsStatsRecord aggregatedRecord) {
            this.aggregatedRecord = Objects.requireNonNull(aggregatedRecord);
            // The commodity aggregation is built one entity at a time
            this.entityCount = 1;
        }

        public int getEntityCount() {
            return entityCount;
        }

        public MktSnapshotsStatsRecord getAggregatedRecord() {
            return aggregatedRecord;
        }

        public int incrementEntityCount() {
            return ++entityCount;
        }
    }

    @Override
    public Long getMemSize() {
        return null;
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                new SimpleMemReporter("commodityAggregationTable", commodityAggregationTable),
                new SimpleMemReporter("entityMetrics", entityMetrics),
                new SimpleMemReporter("entityTypeCounts", entityTypeCounts)
        );
    }

    @Override
    public Collection<Object> getMemExclusions() {
        return Collections.singletonList(MktSnapshotsStatsRecord.class);
    }
}
