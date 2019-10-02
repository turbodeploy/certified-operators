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

    public ComputeTierDemandStatsWriter(@Nonnull ComputeTierDemandStatsStore riStatsStore,
                                        float preferredCurrentWeight) {
        this.computeTierDemandStatsStore = Objects.requireNonNull(riStatsStore);
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

        // Mapping from RI_ID -> VM_ID
        // Stores the mapping from Reserve Instance Id to VM Id.
        final Map<Long, Set<Long>> computeTierIdToVmIdMap = new HashMap<>();
        // Mapping from workload OID -> BusinessAccount OID
        // This stores the mapping of the business account associated
        // with the entities.
        final Map<Long, Long> workloadIdToBusinessAccountIdMap = new HashMap<>();

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

            // We are only interested in entities which are buying from ComputeTier.
            Optional<CommoditiesBoughtFromProvider> computeTierProvider =
                    getComputeTierProvider(entityDTO);
            if (computeTierProvider.isPresent()) {
                computeTierIdToVmIdMap.computeIfAbsent(
                        computeTierProvider.get().getProviderId(),
                        k -> new HashSet<>()).add(entityDTO.getOid());
            }
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

        Set<ComputeTierTypeHourlyByWeekRecord> statsRecordsToUpload =
                new HashSet<>();

        computeTierIdToVmIdMap.forEach((instanceId, vmIdSet) -> {
            int computeTierIdCount = vmIdSet.size();
            vmIdSet.forEach(vmId -> {
                TopologyEntityDTO vmDTO = cloudEntities.get(vmId);
                Optional<Long> availabilityZone =
                        getAvailabilityZone(
                                vmDTO.getConnectedEntityListList());
                if (!availabilityZone.isPresent()) {
                    logger.warn("Skipping. Availability zone missing for VM {} {} ",
                            vmId, vmDTO.getDisplayName());
                    return;
                }
                if (!vmDTO.hasTypeSpecificInfo() &&
                        !vmDTO.getTypeSpecificInfo().hasVirtualMachine()) {
                    logger.warn("Skipping. Missing TypeSpecificInfo for VM {} {} ",
                            vmId, vmDTO.getDisplayName());
                    return;
                }
                TopologyDTO.TypeSpecificInfo.VirtualMachineInfo vmInfo =
                        vmDTO.getTypeSpecificInfo().getVirtualMachine();
                if (vmInfo.getGuestOsInfo().getGuestOsType() == OSType.UNKNOWN_OS) {
                    logger.warn("Skipping. Unknown OS for  VM {} {} {}",
                            vmId, vmDTO.getDisplayName(), vmInfo.getGuestOsInfo().getGuestOsName());
                    return;
                }
                byte platform = (byte) vmInfo.getGuestOsInfo().getGuestOsType().getNumber();
                byte tenancy = (byte) vmInfo.getTenancy().getNumber();
                Long targetId = workloadIdToBusinessAccountIdMap.get(vmId);
                if (targetId == null) {
                    logger.warn("Skipping. Missing Business Account Id for VM {} {} ",
                            vmId, vmDTO.getDisplayName());
                    return;
                }

                WeightedCounts prevWeightedValue =
                        computeTierDemandStatsMap.getOrDefault(
                                new ComputeTierDemandStatsRecord(
                                        targetId, instanceId,
                                        availabilityZone.get(),
                                        platform, tenancy),
                                new WeightedCounts(new BigDecimal(0.0),
                                        new BigDecimal(0.0)));

                BigDecimal newCountFromSourceTopology;
                BigDecimal newCountFromProjectedTopology;

                if (isProjectedTopology) {
                    newCountFromProjectedTopology =
                            getNewWeightedValue(prevWeightedValue.getCountFromProjectedTopology(),
                                    computeTierIdCount);
                    newCountFromSourceTopology =
                            prevWeightedValue.getCountFromSourceTopology();

                } else {
                    newCountFromSourceTopology =
                            getNewWeightedValue(prevWeightedValue.getCountFromSourceTopology(),
                                    computeTierIdCount);
                    newCountFromProjectedTopology =
                            prevWeightedValue.getCountFromProjectedTopology();
                }

                ComputeTierTypeHourlyByWeekRecord newComputeTierDemandStatsRecord =
                        new ComputeTierTypeHourlyByWeekRecord(
                                (byte)topologyForHour,
                                (byte)topologyForDay,
                                targetId,
                                instanceId,
                                availabilityZone.get(),
                                platform,
                                tenancy,
                                newCountFromSourceTopology,
                                newCountFromProjectedTopology);
                logger.trace("Persisted {} demand topology {} {} {} {} {} {} {} {} {}",
                        topologyType,
                        (byte)topologyForHour,
                        (byte)topologyForDay,
                        targetId,
                        instanceId,
                        availabilityZone.get(),
                        platform,
                        tenancy,
                        newCountFromSourceTopology,
                        newCountFromProjectedTopology);

                statsRecordsToUpload.add(newComputeTierDemandStatsRecord);
            });
        });

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
      Return the Availability zone for an entity given
      it's connected entity list.
      @param connectedEntityList
      @return Optional availability zone id.

     */
    private Optional<Long> getAvailabilityZone(
            @Nonnull List<ConnectedEntity> connectedEntityList) {

        return connectedEntityList.stream()
                    .filter(ce -> (ce.getConnectedEntityType() ==
                                EntityType.AVAILABILITY_ZONE_VALUE))
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
     * Get the Ids of the Targets that discovered the entity.
     *
     * @param entityDTO DTO of the entity.
     * @return The list of TargetIds.
     *
     */
    private Set<Long> getTargetIds(TopologyEntityDTO entityDTO) {
            if (!entityDTO.hasOrigin() &&
                    !entityDTO.getOrigin().hasDiscoveryOrigin()) {
                return Collections.emptySet();
            }
            return entityDTO.getOrigin().getDiscoveryOrigin()
                        .getDiscoveringTargetIdsList()
                        .stream()
                        .collect(Collectors.toSet());
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
    private  class ComputeTierDemandStatsRecord {

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
