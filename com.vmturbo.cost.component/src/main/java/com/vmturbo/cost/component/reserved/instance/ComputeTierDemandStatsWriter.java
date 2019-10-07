package com.vmturbo.cost.component.reserved.instance;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

public class ComputeTierDemandStatsWriter {

    private final Logger logger = LogManager.getLogger();

    private int lastSourceTopologyProcessedHour = -1;

    private int lastProjectedTopologyProcessedHour = -1;

    private float preferredCurrentWeight;

    private final ComputeTierDemandStatsStore computeTierDemandStatsStore;

    // Storage for projected reserved instance(RI) coverage of entities.
    private final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore;

    private final Calendar calendar =
            GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    /**
     * A metric that tracks the time taken to load stats from DB.
     */
    private static final DataMetricSummary COMPUTE_TIER_STATS_LOAD_FROM_DB_TIME_SUMMARY =
            DataMetricSummary.builder()
                    .withName("cost_compute_tier_stats_load_from_db_time_seconds")
                    .withHelp("Time taken to load the compute tier stats from DB.")
                    .build();

    /**
     * A metric that tracks the time taken to upload stats to DB.
     */
    private static final DataMetricSummary COMPUTE_TIER_STATS_UPLOAD_TO_DB_TIME_SUMMARY =
            DataMetricSummary.builder()
                    .withName("cost_compute_tier_stats_upload_to_db_time_seconds")
                    .withHelp("Time taken to update/upload the compute tier stats to DB.")
                    .build();

    /**
     * Constructor.
     *
     * @param riStatsStore Provides historical data for instances.
     * @param projectedRICoverageAndUtilStore Storage for projected reserved instance(RI) coverage of entities.
     * @param preferredCurrentWeight Weight for the current demand.
     */
    public ComputeTierDemandStatsWriter(@Nonnull ComputeTierDemandStatsStore riStatsStore,
                                        @Nonnull ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore,
                                        float preferredCurrentWeight) {
        this.computeTierDemandStatsStore = Objects.requireNonNull(riStatsStore);
        this.projectedRICoverageAndUtilStore = projectedRICoverageAndUtilStore;
        this.preferredCurrentWeight = preferredCurrentWeight;
    }


    /**
     * Calculate the histogram of the various Compute Tiers consumed
     * by the VMs and persist these stats in the DB.
     *
     * @param topologyInfo Metadata of the topology.
     * @param cloudEntities Map of ID->CloudEntityDTOs in the Topology.
     * @param isProjectedTopology boolean indicating whether the topology is a
     *                            source or a projected topology.
     */
    synchronized public void calculateAndStoreRIDemandStats(TopologyInfo topologyInfo,
                                 Map<Long, TopologyEntityDTO> cloudEntities,
                                 boolean isProjectedTopology) {

        String topologyType = isProjectedTopology ? "Projected" : "Live";
        logger.info("Calculating RI demands stats from {} topology {}",
                topologyType, topologyInfo);

        final boolean isRealTimeTopology =
                (topologyInfo.getTopologyType() == TopologyType.REALTIME);
        if (!isRealTimeTopology) {
            logger.warn("Received non-realtime topology {}. Skipping");
            return;
        }
        long topologyCreationTime = topologyInfo.getCreationTime();
        calendar.setTimeInMillis(topologyCreationTime);
        int topologyForHour = calendar.get(Calendar.HOUR_OF_DAY);
        int topologyForDay = calendar.get(Calendar.DAY_OF_WEEK);
        // We just need stats from one topology per hour.
        // Skip topologies if we already have data for that hour.
        // If the process restarts, we may process additional topologies for
        // the hour and this would affect the computed weighted values.
        // Few ways to fix this case:
        //  a) Add an updated time column to the RI Demand stats table. But
        //      this is expensive.
        //  b) After every upload, persist the lastProcessedHour value to Consul
        //     and load this value during startup. This approach won't work
        //     if the component fails after persisting stats in the DB but before
        //     updating in Consul. So the persistence to the DB and write to Consul
        //     would have to be atomic for it work and this significantly increases
        //     the complexity.
        //  c) Create another table in the Cost DB which is mainly to store the
        //     lastProcessedHour value and we commit it in the same transaction
        //      as the stats upload.
        // With the Consul approach
        // Since some discrepancy in the data is acceptable, handling this
        // edge case is deemed not necessary.
        // If there is clock skew, we may compute stats for the same hour
        // more than once. This corner case is not handled.
        if ((isProjectedTopology && lastProjectedTopologyProcessedHour == topologyForHour)
                || (!isProjectedTopology && lastSourceTopologyProcessedHour == topologyForHour)) {
            logger.info("Already processed a topology for the hour: {}, day: {}." +
                    " Skipping the {} topology: {} Last updated {}",
                    topologyForHour,
                    topologyForDay,
                    topologyType,
                    topologyInfo,
                    isProjectedTopology ? lastProjectedTopologyProcessedHour : lastSourceTopologyProcessedHour);
            return;
        }

        // For now, we are getting all the records for the hour and day.
        // If it becomes a memory hog, we may have to query and
        // update in batches.
        final DataMetricTimer statsLoadFromDBDurationTimer =
                COMPUTE_TIER_STATS_LOAD_FROM_DB_TIME_SUMMARY.startTimer();

        Map<ComputeTierDemandStatsRecord, WeightedCounts> computeTierDemandStatsMap =
                retrieveExistingStats((byte)topologyForHour, (byte)topologyForDay);
        double statsLoadTime = statsLoadFromDBDurationTimer.observe();
        logger.info("Time to retrieve {} stats for day: {}, hour: {} = {} secs",
                topologyType, topologyForDay,
                topologyForHour, statsLoadTime);

        // Mapping from workload OID -> BusinessAccount OID
        // This stores the mapping of the business account associated
        // with the entities.
        final Map<Long, Long> workloadIdToBusinessAccountIdMap = new HashMap<>();
        for (TopologyEntityDTO entityDTO : cloudEntities.values()) {
            // If the entity is a business account, get all the entities connected to it.
            if (entityDTO.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                getAllConnectedEntitiesForBusinessAccount(
                        entityDTO)
                        .forEach(entityId ->
                                workloadIdToBusinessAccountIdMap.put(entityId,
                                        entityDTO.getOid()));
                continue;
            }
        }

        Map<ComputeTierDemandStatsRecord, Integer> statsRecordToCountMapping
                        = getStatsRecordToCountMapping(cloudEntities, workloadIdToBusinessAccountIdMap);

        Set<ComputeTierTypeHourlyByWeekRecord> statsRecordsToUpload
                        = getComputeTierTypeHourlyByWeekRecords(statsRecordToCountMapping,
                                                                computeTierDemandStatsMap,
                                                                topologyType,
                                                                isProjectedTopology,
                                                                topologyForHour,
                                                                topologyForDay);
        // Upload stats for this hour to the DB.
        try {
            final DataMetricTimer statsUpLoadToDBDurationTimer =
                    COMPUTE_TIER_STATS_UPLOAD_TO_DB_TIME_SUMMARY.startTimer();
            computeTierDemandStatsStore.persistComputeTierDemandStats(statsRecordsToUpload);
            double statsUploadTime = statsUpLoadToDBDurationTimer.observe();
            logger.info("Time to upload {} {} stats to DB for day: {}, hour: {} = {} secs",
                    statsRecordsToUpload.size(), isProjectedTopology ? "Projected" : "Live",
                    topologyForDay, topologyForHour, statsUploadTime);
        } catch (DataAccessException ex) {
            logger.error("Error while persisting RI Demand stats", ex);
            return;
        }

        logger.info("Successfully updated RI demand stats table with {} records"
                        + " for hour: {} and day: {}",
                statsRecordsToUpload.size(), topologyForHour, topologyForDay);

        if (isProjectedTopology) {
            lastProjectedTopologyProcessedHour = topologyForHour;
        } else {
            lastSourceTopologyProcessedHour = topologyForHour;
        }
    }

    /**
     * Returns a mapping from statsRecord to their count.
     *
     * @param cloudEntities Mapping from entity Id to the entity.
     * @return a mapping from statsRecord to their count.
     */
    protected Map<ComputeTierDemandStatsRecord, Integer> getStatsRecordToCountMapping(
            Map<Long, TopologyEntityDTO> cloudEntities,
            Map<Long, Long> workloadIdToBusinessAccountIdMap) {
        Map<ComputeTierDemandStatsRecord, Integer> statsRecordToCountMapping = new HashMap<>();
        for (Long workLoadId : cloudEntities.keySet()) {
            TopologyEntityDTO workLoadDto = cloudEntities.get(workLoadId);

            if (!workLoadDto.hasTypeSpecificInfo() &&
                    !workLoadDto.getTypeSpecificInfo().hasVirtualMachine()) {
                logger.debug("Skipping. Missing TypeSpecificInfo for workload {}", workLoadDto.getDisplayName());
                continue;
            }

            Long businessAccountId = workloadIdToBusinessAccountIdMap.get(workLoadId);
            if (businessAccountId == null) {
                logger.debug("Skipping. Missing Business Account Id for workload {}", workLoadDto.getDisplayName());
                continue;
            }

            Optional<CommoditiesBoughtFromProvider> computeTierId = getComputeTierProvider(workLoadDto);
            if (!computeTierId.isPresent()) {
                logger.debug("Skipping. Compute Tier missing for workload {}", workLoadDto.getDisplayName());
                continue;
            }

            Optional<Long> availabilityZone = getAvailabilityZoneorRegion(workLoadDto.getConnectedEntityListList());
            if (!availabilityZone.isPresent()) {
                logger.debug("Skipping. Availability zone missing for workload {}", workLoadDto.getDisplayName());
                continue;
            }

            TopologyDTO.TypeSpecificInfo.VirtualMachineInfo vmInfo =
                    workLoadDto.getTypeSpecificInfo().getVirtualMachine();
            if (vmInfo.getGuestOsInfo().getGuestOsType() == OSType.UNKNOWN_OS) {
                logger.debug("Skipping. Unknown OS for workload {}", workLoadDto.getDisplayName());
                continue;
            }

            byte platform = (byte) vmInfo.getGuestOsInfo().getGuestOsType().getNumber();
            byte tenancy = (byte) vmInfo.getTenancy().getNumber();

            final ComputeTierDemandStatsRecord statsRecord = new ComputeTierDemandStatsRecord(
                    businessAccountId, computeTierId.get().getProviderId(),
                    availabilityZone.get(),
                    platform, tenancy);
            statsRecordToCountMapping.putIfAbsent(statsRecord, 0);
            statsRecordToCountMapping.put(statsRecord, statsRecordToCountMapping.get(statsRecord) + 1);
        }

        return statsRecordToCountMapping;
    }

    /**
     * Gets ComputeTierTypeHourlyByWeekRecord's for a given hour and day.
     *
     * @param statsRecordToCountMapping mapping from statsRecord to their count.
     * @param computeTierDemandStatsMap Mapping from existing ComputeTierDemandStatsRecord to their weights.
     * @param topologyType The topology type.
     * @param isProjectedTopology flag for projected topology.
     * @param topologyForHour The hour for which records will be returned.
     * @param topologyForDay The day for which records will be returned.
     * @return computeTierTypeHourlyByWeekRecord's for a given hour and day.
     */
    protected Set<ComputeTierTypeHourlyByWeekRecord> getComputeTierTypeHourlyByWeekRecords(
            Map<ComputeTierDemandStatsRecord, Integer> statsRecordToCountMapping,
            Map<ComputeTierDemandStatsRecord, WeightedCounts> computeTierDemandStatsMap,
            String topologyType,
            boolean isProjectedTopology,
            int topologyForHour,
            int topologyForDay) {
        final Set<ComputeTierTypeHourlyByWeekRecord> statsRecordsToUpload = new HashSet<>();

        for (ComputeTierDemandStatsRecord statsRecord : statsRecordToCountMapping.keySet()) {
            WeightedCounts prevWeightedValue = computeTierDemandStatsMap.getOrDefault(statsRecord,
                    new WeightedCounts(new BigDecimal(0.0),
                            new BigDecimal(0.0)));
            BigDecimal newCountFromSourceTopology;
            BigDecimal newCountFromProjectedTopology;

            if (isProjectedTopology) {
                newCountFromProjectedTopology =
                        getNewWeightedValue(prevWeightedValue.getCountFromProjectedTopology(),
                                statsRecordToCountMapping.get(statsRecord));
                newCountFromSourceTopology =
                        prevWeightedValue.getCountFromSourceTopology();

            } else {
                newCountFromSourceTopology =
                        getNewWeightedValue(prevWeightedValue.getCountFromSourceTopology(),
                                statsRecordToCountMapping.get(statsRecord));
                newCountFromProjectedTopology =
                        prevWeightedValue.getCountFromProjectedTopology();
            }

            ComputeTierTypeHourlyByWeekRecord newComputeTierDemandStatsRecord =
                    new ComputeTierTypeHourlyByWeekRecord(
                            (byte)topologyForHour,
                            (byte)topologyForDay,
                            statsRecord.targetId,
                            statsRecord.instanceType,
                            statsRecord.availabilityZone,
                            statsRecord.platform,
                            statsRecord.tenancy,
                            newCountFromSourceTopology,
                            newCountFromProjectedTopology);

            logger.trace("Persisted {} demand topology {} {} {} {} {} {} {} {} {}",
                    topologyType,
                    (byte)topologyForHour,
                    (byte)topologyForDay,
                    statsRecord.targetId,
                    statsRecord.instanceType,
                    statsRecord.availabilityZone,
                    statsRecord.platform,
                    statsRecord.tenancy,
                    newCountFromSourceTopology,
                    newCountFromProjectedTopology);
            statsRecordsToUpload.add(newComputeTierDemandStatsRecord);
        }
        return statsRecordsToUpload;
    }

    /**
     * Check if there are stats in the database for the source topology.
     *
     * @param sourceTopologyInfo  Source TopologyInfo for which databased is checked
     * @return true if there are stats in the database for the hour and day, false otherwise
     */
    public boolean hasExistingStats(TopologyInfo sourceTopologyInfo) {
        long topologyCreationTime = sourceTopologyInfo.getCreationTime();
        calendar.setTimeInMillis(topologyCreationTime);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int day = calendar.get(Calendar.DAY_OF_WEEK);
        try (Stream<ComputeTierTypeHourlyByWeekRecord> riStatsRecordStream =
                     computeTierDemandStatsStore.getStats((byte)hour, (byte)day)) {
            return riStatsRecordStream.findAny().isPresent();
        }
    }

    /**
     * Query all the stats from the database for the hour and day this
     * topology was created on.
     *
     * @param hour The hour for which the stats are requested.
     * @param day  The day for which the stats are requested.
     * @return The stats for the day and hour.
     * @throws DataAccessException
     */
    public Map<ComputeTierDemandStatsRecord, WeightedCounts> retrieveExistingStats(byte hour, byte day)
            throws DataAccessException {

        // map for storing the existing stats retrieved from the DB.
        final Map<ComputeTierDemandStatsRecord, WeightedCounts> riDemandStatsMap
                = new HashMap();

        try (Stream<ComputeTierTypeHourlyByWeekRecord> riStatsRecordStream =
                     computeTierDemandStatsStore.getStats(hour, day)) {
            riStatsRecordStream.forEach(riStatRecord -> {
                riDemandStatsMap.put(
                        new ComputeTierDemandStatsRecord(riStatRecord.getAccountId(),
                                riStatRecord.getComputeTierId(),
                                riStatRecord.getAvailabilityZone(),
                                riStatRecord.getPlatform(),
                                riStatRecord.getTenancy()),
                        new WeightedCounts(
                                riStatRecord.getCountFromSourceTopology(),
                                riStatRecord.getCountFromProjectedTopology()));

            });
        }
        return riDemandStatsMap;
    }

    /**
     *
     * @param entityDTO Entity DTO.
     * @return Optional if the entity buys from a Compute Tier
     */
    private Optional<CommoditiesBoughtFromProvider> getComputeTierProvider(
            TopologyEntityDTO entityDTO) {

        // Entities can buy from only one COMPUTE_TIER. If there are many, then ignore
        // the remaining ones.
        return entityDTO.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(provider -> (provider.hasProviderEntityType()
                       && provider.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE))
                .findFirst();
    }

    /**
      Return the Availability zone or the region for an entity given it's connected entity list.

      @param connectedEntityList
      @return Optional availability zone or the region id.
     */
    private Optional<Long> getAvailabilityZoneorRegion(
            @Nonnull List<ConnectedEntity> connectedEntityList) {

        return connectedEntityList.stream()
                    .filter(ce -> (ce.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                                || ce.getConnectedEntityType() == EntityType.REGION_VALUE)
                    .findFirst()
                    .map(e -> e.getConnectedEntityId());
    }

    /**
     *  Get the OIDs of the VMs which are connected to the
     *  BusinessAccount.
     *
     * @param entityDTO of the BusinessAccount.
     * @return VM Oids which are connected to the given business
     *          account.
     */
    private List<Long> getAllConnectedEntitiesForBusinessAccount(
            @Nonnull TopologyEntityDTO entityDTO) {

        if (entityDTO.getEntityType() != EntityType.BUSINESS_ACCOUNT_VALUE) {
            return Collections.emptyList();
        }
        return entityDTO.getConnectedEntityListList().stream()
                .filter(entity -> (entity.getConnectionType() ==
                        ConnectionType.OWNS_CONNECTION)
                        && (entity.getConnectedEntityType() ==
                        EntityType.VIRTUAL_MACHINE_VALUE))
                .map(e -> e.getConnectedEntityId())
                .collect(Collectors.toList());
    }

    /**
     * Return the new weighted value.
     *
     * @param prevWeightedValue The previous weighted value.
     * @param newValue The new value.
     * @return The computed new weighted value.
     */
    private BigDecimal getNewWeightedValue(BigDecimal prevWeightedValue,
                                           long newValue) {

        // weighted_value = (previousWeight * prevValue) + (currentWeight * newValue)
        BigDecimal newWeight = new BigDecimal(newValue * preferredCurrentWeight);
        return prevWeightedValue.multiply(new BigDecimal(1 - preferredCurrentWeight))
                .add(newWeight);
    }

    /**
     *  Helper class used as a key in a Map to identify existing stats for
     *  a given RI instance class.
     */
    protected static class ComputeTierDemandStatsRecord {

        private final long targetId;

        private final long availabilityZone;

        private final long instanceType;

        private final byte platform;

        private final byte tenancy;

        ComputeTierDemandStatsRecord(long targetId,
                            long instanceType,
                            long availabilityZone,
                            byte platform,
                            byte tenancy) {
            this.targetId = targetId;
            this.instanceType = instanceType;
            this.availabilityZone = availabilityZone;
            this.platform = platform;
            this.tenancy = tenancy;
        }


        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }

            if (!(obj instanceof ComputeTierDemandStatsRecord)) {
               return false;
            }

            ComputeTierDemandStatsRecord otherRec = (ComputeTierDemandStatsRecord) obj;

            return (otherRec.targetId == this.targetId &&
                    otherRec.instanceType == this.instanceType &&
                    otherRec.availabilityZone == this.availabilityZone &&
                    otherRec.platform == this.platform &&
                    otherRec.tenancy == this.tenancy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetId, instanceType,
                    availabilityZone, platform, tenancy);
        }
    }

    /**
     *  Helper class used to store the weighted average histogram
     *  values retrieved from the DB.
     *
     */
    private class WeightedCounts {

        private final BigDecimal countFromSourceTopology;
        private final BigDecimal countFromProjectedTopology;

        public WeightedCounts(@Nonnull BigDecimal countFromSourceTopology,
                                    @Nonnull BigDecimal countFromProjectedTopology) {
            this.countFromSourceTopology= Objects.requireNonNull(countFromSourceTopology);
            this.countFromProjectedTopology = Objects.requireNonNull(countFromProjectedTopology);
        }

        public BigDecimal getCountFromSourceTopology() {
            return countFromSourceTopology;
        }

        public BigDecimal getCountFromProjectedTopology() {
            return countFromProjectedTopology;
        }

    }
}
