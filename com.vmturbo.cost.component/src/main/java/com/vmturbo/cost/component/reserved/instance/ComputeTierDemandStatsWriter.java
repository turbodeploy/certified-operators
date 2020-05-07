package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
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

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
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
     * @param cloudTopology The cloud topology.
     * @param isProjectedTopology boolean indicating whether the topology is a
     *                            source or a projected topology.
     */
    synchronized public void calculateAndStoreRIDemandStats(TopologyInfo topologyInfo,
                                 CloudTopology cloudTopology,
                                 boolean isProjectedTopology) {

        String topologyType = isProjectedTopology ? "Projected" : "Live";
        long topologyCreationTime = topologyInfo.getCreationTime();
        String creationDateStr = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss")
                .format(new Date(topologyCreationTime));
        logger.info("Calculating RI demands stats from {} topology {} created at {}",
                topologyType, topologyInfo, creationDateStr);

        final boolean isRealTimeTopology =
                (topologyInfo.getTopologyType() == TopologyType.REALTIME);
        if (!isRealTimeTopology) {
            logger.warn("Received non-realtime topology {}. Skipping", topologyInfo);
            return;
        }

        calendar.setTimeInMillis(topologyCreationTime);

        int topologyForHour = calendar.get(Calendar.HOUR_OF_DAY);
        int topologyForDay = calendar.get(Calendar.DAY_OF_WEEK);

        if ((isProjectedTopology && lastProjectedTopologyProcessedHour == topologyForHour)
                || (!isProjectedTopology && lastSourceTopologyProcessedHour == topologyForHour)) {
            logger.info("Already processed a topology for the hour: {}, day: {}." +
                    " Skipping the {} topology: {} created at {}. Last updated {}",
                    topologyForHour,
                    topologyForDay,
                    topologyType,
                    topologyInfo,
                    creationDateStr,
                    isProjectedTopology ? lastProjectedTopologyProcessedHour : lastSourceTopologyProcessedHour);
            return;
        }

        // Checks whether an entry is present in Cost.LAST_UPDATED table corresponding to the table
        // and the column which are about to be updated. If found then they are not updated. This is
        // necessary to avoid double counting for that hour in case the component restarts after a
        // crash.
        boolean isUpdatedInLastHour = computeTierDemandStatsStore
                .isUpdatedInLastHour(Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.getName(),
                isProjectedTopology ?
                COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COUNT_FROM_SOURCE_TOPOLOGY.getName() :
                COMPUTE_TIER_TYPE_HOURLY_BY_WEEK.COUNT_FROM_PROJECTED_TOPOLOGY.getName(),
                        calendar);

        if (isUpdatedInLastHour) {
            logger.info("Table has already been updated for this hour for topology {}"
                    + " created at {} . Skip Updating.", topologyInfo, creationDateStr);
            if (isProjectedTopology) {
                lastProjectedTopologyProcessedHour = topologyForHour;
            } else {
                lastSourceTopologyProcessedHour = topologyForHour;
            }
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

        Map<ComputeTierDemandStatsRecord, Integer> statsRecordToCountMapping
                        = getStatsRecordToCountMapping(cloudTopology,
                                                        isProjectedTopology);

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
            computeTierDemandStatsStore.persistComputeTierDemandStats(statsRecordsToUpload, isProjectedTopology);
            double statsUploadTime = statsUpLoadToDBDurationTimer.observe();
            logger.info("Time to upload {} {} stats to DB for day: {}, hour: {} = {} secs",
                    statsRecordsToUpload.size(), isProjectedTopology ? "Projected" : "Live",
                    topologyForDay, topologyForHour, statsUploadTime);
        } catch (DataAccessException ex) {
            logger.error("Error while persisting RI Demand stats", ex);
            return;
        }

        logger.info("Successfully updated RI demand stats table with {} records"
                        + " for hour: {} and day: {} for topology created at {} ",
                statsRecordsToUpload.size(), topologyForHour, topologyForDay, creationDateStr);

        if (isProjectedTopology) {
            lastProjectedTopologyProcessedHour = topologyForHour;
        } else {
            lastSourceTopologyProcessedHour = topologyForHour;
        }
    }

    /**
     * Returns a mapping from statsRecord to their count.
     *
     * @param cloudTopology The Cloud Topology.
     * @return a mapping from statsRecord to their count.
     */
    protected Map<ComputeTierDemandStatsRecord, Integer> getStatsRecordToCountMapping(
                                                CloudTopology cloudTopology,
                                                boolean isProjectedTopology) {
        Map<ComputeTierDemandStatsRecord, Integer> statsRecordToCountMapping = new HashMap<>();
        List<TopologyEntityDTO> cloudVms = cloudTopology
                .getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);

        // A map of entity oid -> EntityReservedInstanceCoverage.
        // Used to determine whether an entity has an RI coverage.
        final Map<Long, EntityReservedInstanceCoverage> allProjectedEntitiesRICoverages =
                projectedRICoverageAndUtilStore.getAllProjectedEntitiesRICoverages();

        for (TopologyEntityDTO workLoadDto : cloudVms) {
            final Long workLoadId = workLoadDto.getOid();
            final String workLoadDisplayName = workLoadDto.getDisplayName();

            if (workLoadDto.getEntityState() != EntityState.POWERED_ON) {
                logger.debug("Skipping. Workload {} is not powered on. Current state is : {}",
                        workLoadDisplayName, workLoadDto.getEntityState());
                continue;
            }

            if (!workLoadDto.hasTypeSpecificInfo() &&
                    !workLoadDto.getTypeSpecificInfo().hasVirtualMachine()) {
                logger.debug("Skipping. Missing TypeSpecificInfo for workload {}", workLoadDisplayName);
                continue;
            }

            Optional<TopologyEntityDTO> businessAccount = cloudTopology.getOwner(workLoadId);
            if (!businessAccount.isPresent()) {
                logger.debug("Skipping. Missing Business Account Id for workload {}", workLoadDisplayName);
                continue;
            }
            final Long businessAccountId = businessAccount.get().getOid();

            Optional<TopologyEntityDTO> computeTierId = cloudTopology.getComputeTier(workLoadId);
            if (!computeTierId.isPresent()) {
                logger.debug("Skipping. Compute Tier missing for workload {}", workLoadDisplayName);
                continue;
            }

            Optional<TopologyEntityDTO> connectedAZorRegion = cloudTopology.getConnectedAvailabilityZone(workLoadId);
            if (!connectedAZorRegion.isPresent()) {
                connectedAZorRegion = cloudTopology.getConnectedRegion(workLoadId);
                if (!connectedAZorRegion.isPresent()) {
                    logger.info("Skipping. Availability zone/ Region missing for workload {}",
                            workLoadDisplayName);
                    continue;
                }
            }

            TopologyDTO.TypeSpecificInfo.VirtualMachineInfo vmInfo =
                    workLoadDto.getTypeSpecificInfo().getVirtualMachine();
            final OSType guestOsType = vmInfo.getGuestOsInfo().getGuestOsType();
            if (guestOsType == OSType.UNKNOWN_OS) {
                logger.debug("Skipping. Unknown OS for workload {}", workLoadDisplayName);
                continue;
            }

            // If the billing type of a VM is bidding the demand should not be recorded.
            if (vmInfo.getBillingType() == VMBillingType.BIDDING) {
                logger.debug("Skipping. The billing type is bidding for workload {}", workLoadDisplayName);
                continue;
            }

            // If its a projected topology we don't want to count a VM if its covered.
            if (isProjectedTopology) {
                final EntityReservedInstanceCoverage riCoverage = allProjectedEntitiesRICoverages.get(workLoadId);
                if (riCoverage != null) {
                    Map<Long, Double> riMapping = riCoverage.getCouponsCoveredByRiMap();
                    if (riMapping != null && riMapping.size() > 0) {
                        logger.debug("Skipping. Workload {} is covered by an RI.", workLoadDisplayName);
                        continue;
                    }
                }
            }

            byte platform = (byte) guestOsType.getNumber();
            byte tenancy = (byte) vmInfo.getTenancy().getNumber();

            final ComputeTierDemandStatsRecord statsRecord = new ComputeTierDemandStatsRecord(
                    businessAccountId, computeTierId.get().getOid(),
                    connectedAZorRegion.get().getOid(),
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
                            statsRecord.regionOrZone,
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
                    statsRecord.regionOrZone,
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
                                riStatRecord.getRegionOrZoneId(),
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

        private final long regionOrZone;

        private final long instanceType;

        private final byte platform;

        private final byte tenancy;

        ComputeTierDemandStatsRecord(long targetId,
                            long instanceType,
                            long regionOrZone,
                            byte platform,
                            byte tenancy) {
            this.targetId = targetId;
            this.instanceType = instanceType;
            this.regionOrZone = regionOrZone;
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
                    otherRec.regionOrZone == this.regionOrZone &&
                    otherRec.platform == this.platform &&
                    otherRec.tenancy == this.tenancy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetId, instanceType,
                    regionOrZone, platform, tenancy);
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
