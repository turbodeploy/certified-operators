package com.vmturbo.stitching.poststitching;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.DoubleMath;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.stats.Stats.EntityCommoditiesMaxValues;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;

/**
 * Post-stitching operation for setting maxQuantity values of the commodities for each entity.
 *
 */
public class SetCommodityMaxQuantityPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A metric that tracks the time taken to load max values.
     */
    private static final DataMetricSummary COMMODITY_MAX_VALUES_LOAD_TIME_SUMMARY =
        DataMetricSummary.builder()
            .withName("tp_max_values_load_time_seconds")
            .withHelp("Time taken to load the max values from history.")
            .withLabelNames("loading_phase")
            .build();

    /**
     * A metric that tracks the % of redundant entries in the max values map.
     */
    private static final DataMetricGauge MAX_VALUES_REDUNDANT_ENTRIES_PERCENTAGE_GAUGE =
        DataMetricGauge.builder()
            .withName("tp_max_values_map_redundant_entries_percentage")
            .withHelp("The percentage of redundant entries in the max values map")
            .build();

    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    /**
     * ConcurrentMap which stores the mapping from {EntityId,CommidtyType} -> MaxValue.
     */
    private ConcurrentMap<EntityCommodityReference, CommodityMaxValue> entityCommodityToMaxQuantitiesMap =
        new ConcurrentHashMap<>();

    /**
     * Map which stores the mapping from entity type -> set of oids.
     */
    private Map<Integer, Set<Long>> entityOidsByType = new HashMap<>();

    /**
     * In the broadcast thread we take the entities from the topology, and store their oids by type in entityOidsByType.
     * The broadcast thread holds this lock when swapping out the old entityOidsByType with the new one.
     * In the background processing thread, we access the oids from entityOidsByType and use them in the query to
     * history component. The background thread holds the lock when accessing entityOidsByType.
     */
    private final Object entityOidsLock = new Object();

    /**
     * This configuration value controls how often the commodity max values are
     * loaded from history component.
     */
    private long maxValuesBackgroundLoadFrequencyMinutes;

    /**
     * Background task to fetch max values is scheduled in the first broadcast. Once scheduled, this will be set to true.
     */
    private boolean maxValuesBackgroundTaskScheduled;

    /**
     * Used to age out any max values in TP. The max retention settings are
     * stored in history. It will be loaded from history when we load the max values.
     */
    private long statsDaysRetentionSettingInSeconds = TimeUnit.DAYS.toSeconds(Math.round(
            GlobalSettingSpecs.StatsRetentionDays.createSettingSpec()
                .getNumericSettingValueType().getDefault()));

    private static final ImmutableMap<Integer, ImmutableSet<Integer>> QUERY_MAP =
        ImmutableMap.<Integer, ImmutableSet<Integer>>builder()
            .put(EntityType.APPLICATION_SERVER_VALUE, ImmutableSet.of(CommodityType.HEAP_VALUE))
            .put(EntityType.CONTAINER_SPEC_VALUE, ImmutableSet.of(CommodityType.VCPU_VALUE, CommodityType.VMEM_VALUE))
            .put(EntityType.DATABASE_VALUE, ImmutableSet.of(CommodityType.VMEM_VALUE))
            .put(EntityType.DATABASE_SERVER_VALUE, ImmutableSet.of(CommodityType.DB_MEM_VALUE))
            .put(EntityType.DISK_ARRAY_VALUE, ImmutableSet.of(CommodityType.STORAGE_AMOUNT_VALUE))
            .put(EntityType.IO_MODULE_VALUE, ImmutableSet.of(CommodityType.NET_THROUGHPUT_VALUE))
            .put(EntityType.LOGICAL_POOL_VALUE, ImmutableSet.of(CommodityType.STORAGE_AMOUNT_VALUE))
            .put(EntityType.STORAGE_VALUE, ImmutableSet.of(CommodityType.STORAGE_AMOUNT_VALUE))
            .put(EntityType.STORAGE_CONTROLLER_VALUE, ImmutableSet.of(CommodityType.STORAGE_AMOUNT_VALUE))
            .put(EntityType.SWITCH_VALUE, ImmutableSet.of(CommodityType.NET_THROUGHPUT_VALUE))
            .put(EntityType.VIRTUAL_MACHINE_VALUE, ImmutableSet.of(CommodityType.VCPU_VALUE, CommodityType.VMEM_VALUE))
            .build();


    /**
     * We exclude all the access commodities.
     */
    private ImmutableSet<Integer> excludedCommodities =
        ImmutableSet.of(
            CommodityType.APPLICATION_VALUE,
            CommodityType.CLUSTER_VALUE,
            CommodityType.DATACENTER_VALUE,
            CommodityType.DATASTORE_VALUE,
            CommodityType.DRS_SEGMENTATION_VALUE,
            CommodityType.DSPM_ACCESS_VALUE,
            CommodityType.NETWORK_VALUE,
            CommodityType.SEGMENTATION_VALUE,
            CommodityType.STORAGE_CLUSTER_VALUE,
            CommodityType.VAPP_ACCESS_VALUE,
            CommodityType.VDC_VALUE,
            CommodityType.VMPM_ACCESS_VALUE,
            CommodityType.LICENSE_ACCESS_VALUE);

    // In legacy, maxQuantity value is being set only for VM entities.
    // Here, we are setting maxQuantity value for all entities whose
    // max values stats are stored by DB.
    private static final List<EntityType> interestedEntityTypes =
        ImmutableList.of(
            EntityType.CHASSIS,
            EntityType.CONTAINER_SPEC,
            EntityType.CONTAINER,
            EntityType.DISK_ARRAY,
            EntityType.DPOD,
            EntityType.IO_MODULE,
            EntityType.LOGICAL_POOL,
            EntityType.PHYSICAL_MACHINE,
            EntityType.RESERVED_INSTANCE,
            EntityType.STORAGE,
            EntityType.STORAGE_CONTROLLER,
            EntityType.SWITCH,
            EntityType.VIRTUAL_DATACENTER,
            EntityType.VIRTUAL_MACHINE,
            EntityType.VPOD,
            EntityType.DATABASE_SERVER,
            EntityType.DATABASE
            );

    private ScheduledExecutorService backgroundStatsLoadingExecutor =
            Executors.newSingleThreadScheduledExecutor(threadFactory());

    /**
     * This constrcut is only used for getOperationName().
     */
    public SetCommodityMaxQuantityPostStitchingOperation() {
        this.statsHistoryClient = null;
    }

    /**
     *  We try to initalize the internal max value map during object
     *  initialization.  We also start a background task to periodically
     *  pull the stats from history.We are only interested in the max values
     *  within the stats retention window. So periodically we need to pull
     *  the data from history to reflect the current
     *  max=max(max_value_from_tp, max_value_from_db).
     *  If the load fails during initilization, we start the background task
     *  with a smaller delay. Otherwise we start the next load after a delay
     *  of maxValuesBackgroundLoadFrequencyMinutes.
     */
    public SetCommodityMaxQuantityPostStitchingOperation(
        com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig setMaxValuesConfig) {
        this.statsHistoryClient = setMaxValuesConfig.getStatsClient();
        this.maxValuesBackgroundLoadFrequencyMinutes =
            setMaxValuesConfig.getMaxValuesBackgroundLoadFrequencyMinutes();
    }

    /**
     * Task to load the max values map in the background. It can be mutating the
     * max value map concurrently(in a safe manner). If any exception happens
     * while fetching the values from history, we ignore it as the task
     * will be executed on the next schedule.
     */
    @VisibleForTesting
    class LoadMaxValuesFromDBTask implements Runnable {
        @Override
        public void run() {
            try {
                if (runBackgroundTask()) {
                    setStatsDaysRetentionSettingInSeconds();
                    final DataMetricTimer loadDurationTimer =
                            COMMODITY_MAX_VALUES_LOAD_TIME_SUMMARY.labels("background").startTimer();
                    QUERY_MAP.forEach((entityType, comms) -> {
                        Set<Long> oids = fetchEntityOidsOfType(entityType);
                        if (oids.isEmpty()) {
                            return;
                        }
                        Stopwatch watch = Stopwatch.createStarted();
                        GetEntityCommoditiesMaxValuesRequest request =
                                GetEntityCommoditiesMaxValuesRequest.newBuilder()
                                        .setEntityType(entityType)
                                        .addAllCommodityTypes(comms)
                                        .addAllUuids(oids)
                                        .build();
                        Iterator<EntityCommoditiesMaxValues> response = statsHistoryClient.getEntityCommoditiesMaxValues(request);
                        updateEntityCommodityToMaxQuantitiesMap(response);
                        watch.stop();
                        logger.info("Received and processed max for {} {}s in {} ms", oids.size(),
                                EntityType.forNumber(entityType).toString(), watch.elapsed(TimeUnit.MILLISECONDS));
                    });
                    double loadTime = loadDurationTimer.observe();
                    logger.info("Size of maxValues map after background load: {}. Load time: {} seconds",
                                entityCommodityToMaxQuantitiesMap.size(), loadTime);
                }
            } catch (Throwable t) {
                    logger.error("Error while fetching max values", t);
            }
        }

        private boolean runBackgroundTask() {
            synchronized (entityOidsLock) {
                return !entityOidsByType.isEmpty();
            }
        }
    }

    @VisibleForTesting
    void setBackgroundStatsLoadingExecutor(ScheduledExecutorService backgroundStatsLoadingExecutor) {
        this.backgroundStatsLoadingExecutor = backgroundStatsLoadingExecutor;
    }

    private void setStatsDaysRetentionSettingInSeconds() {
        Iterator<SettingProto.Setting> settingIterator = statsHistoryClient
                .getStatsDataRetentionSettings(GetStatsDataRetentionSettingsRequest.getDefaultInstance());
        while (settingIterator.hasNext()) {
            SettingProto.Setting setting = settingIterator.next();
            if (setting.getSettingSpecName().equals(
                    GlobalSettingSpecs.StatsRetentionDays.getSettingName())
                    && setting.hasNumericSettingValue()) {
                statsDaysRetentionSettingInSeconds = TimeUnit.DAYS.toSeconds(
                        Math.round(setting.getNumericSettingValue().getValue()));
            }
        }
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("max-stats-background-loader-thread-%d").build();
    }

    @Nonnull
    private Set<Long> fetchEntityOidsOfType(Integer entityType) {
        Set<Long> resultSet;
        synchronized (entityOidsLock) {
            resultSet = entityOidsByType.get(entityType);
            return resultSet == null ? new HashSet<>() : resultSet;
        }
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.multiEntityTypesScope(interestedEntityTypes);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity>
    performOperation(@Nonnull final Stream<TopologyEntity> entities,
                     @Nonnull final EntitySettingsCollection settingsCollection,
                     @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        Set<TopologyEntity> entitySet = entities.collect(Collectors.toSet());
        updateEntityOids(entitySet);
        if (!maxValuesBackgroundTaskScheduled) {
            backgroundStatsLoadingExecutor
                    .scheduleWithFixedDelay(
                            new LoadMaxValuesFromDBTask(),
                            0,
                            maxValuesBackgroundLoadFrequencyMinutes,
                            TimeUnit.MINUTES);
            logger.info("Scheduled max values background load with frequency of {} minutes",
                    maxValuesBackgroundLoadFrequencyMinutes);
            maxValuesBackgroundTaskScheduled = true;
        }

        long commoditiesCount = 0;
        for (TopologyEntity entity : entitySet) {
            final TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();

            final long entityOid = entity.getOid();
            List<CommoditySoldDTO.Builder> commoditySoldBuilderList = entityBuilder.getCommoditySoldListBuilderList();
            // For now we only set max quantity values for CommoditySoldDTO as
            // that's the behaviour in legacy.
            for (CommoditySoldDTO.Builder commoditySoldDTO : commoditySoldBuilderList) {
                // skip access commodities
                if (excludedCommodities.contains(commoditySoldDTO.getCommodityType().getType())) {
                    continue;
                }
                EntityCommodityReference key = createEntityCommodityKey(entityOid, commoditySoldDTO.getCommodityType());
                if (commoditySoldDTO.getUsed() < 0)  {
                    logger.warn(
                            "Commodity with type {} and key {} for entity oid {} has -ve used value : {}",
                            commoditySoldDTO.getCommodityType().getType(),
                            commoditySoldDTO.getCommodityType().getKey(),
                            commoditySoldDTO.getUsed(), entity.getOid());
                }
                CommodityMaxValue newMax = createCommodityMaxValue(ValueSource.TP, commoditySoldDTO.getUsed());
                // Atomically set the values as the background thread might also be mutating the map concurrently.
                newMax = entityCommodityToMaxQuantitiesMap
                            .merge(key, newMax,
                                    (oldValue, newValue) ->
                                        // equality check is there to keep the newer value when the values are equal.
                                        // This way max value age gets updated.
                                        ((Double.compare(oldValue.getMaxValue(), newValue.getMaxValue()) <= 0)
                                            ? newValue : oldValue));

                final double newMaxValue = newMax.getMaxValue();
                resultBuilder.queueUpdateEntityAlone(entity, toUpdate -> commoditySoldDTO
                                .getHistoricalUsedBuilder().setMaxQuantity(newMaxValue));
                commoditiesCount++;
            }
        }

        // We now query the history component for the max values only for commodities of entities which are in the
        // current topology. So this percentage should be quite high.
        // This is a rough estimate. To get the correct amount, a set_difference
        // has to be calculated which is a more expensive operation.
        Float redundantPct = calculateMapOccupancyPercentage(commoditiesCount);
        logger.info("Redundant entries stats. Total size: {}, CommoditiesLookedAt: {}, Pct_Redundant: {}%",
                    entityCommodityToMaxQuantitiesMap.size(), commoditiesCount,
                    redundantPct);

        MAX_VALUES_REDUNDANT_ENTRIES_PERCENTAGE_GAUGE.setData(redundantPct.doubleValue());

        // Don't track the changes as it could lead to memory bloat.
        // Just return empty builder.
        return resultBuilder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public FormatRecommendation getFormatRecommendation() {
        // Because this operation makes the same change to lots of entities, recommend that no
        // context be added to changes in the journal.
        return FormatRecommendation.COMPACT;
    }

    private void updateEntityOids(Set<TopologyEntity> entitySet) {
        Map<Integer, Set<Long>> tempEntityOidsByType = new HashMap<>();
        for (TopologyEntity entity : entitySet) {
            if (QUERY_MAP.containsKey(entity.getEntityType())) {
                tempEntityOidsByType.computeIfAbsent(entity.getEntityType(), k -> new HashSet<>()).add(entity.getOid());
            }
        }
        synchronized (entityOidsLock) {
            entityOidsByType = tempEntityOidsByType;
        }
    }

    private void updateEntityCommodityToMaxQuantitiesMap(Iterator<EntityCommoditiesMaxValues> commMaxValues) {
        commMaxValues.forEachRemaining(entityCommodityMaxValues -> {
            entityCommodityMaxValues.getCommodityMaxValuesList()
                .forEach(dbMaxValue -> {
                    EntityCommodityReference key =
                        createEntityCommodityKey(entityCommodityMaxValues.getOid(), dbMaxValue.getCommodityType());
                    // Atomically set the values as the background thread may be concurrently mutating the map.
                    CommodityMaxValue currentMax =
                        entityCommodityToMaxQuantitiesMap
                            .compute(key, (k, currValue) -> {
                                if (currValue == null) {
                                    return createCommodityMaxValue(ValueSource.DB, dbMaxValue.getMaxValue());
                                } else {
                                    // If the existing value source is from DB, then overwrite it with the currently
                                    // fetched value. This is to age out old maxes as we are only interested in the
                                    // max value within the stats retention period.
                                    // If the existing value source is from TP, replace it with the one from DB, if
                                    //  (a) the value has been in the cache for too long i.e. beyond the retention period
                                    //          OR
                                    //  (b) tp_value < db_value.
                                    if (currValue.getValueSource() == ValueSource.DB
                                        || currValue.getAge() > statsDaysRetentionSettingInSeconds
                                        || (Double.compare(currValue.getMaxValue(), dbMaxValue.getMaxValue()) < 0)) {
                                        return createCommodityMaxValue(ValueSource.DB, dbMaxValue.getMaxValue());
                                    } else {
                                        return currValue;
                                    }
                                }
                            });
                });
        });
    }

    /**
     * Recreates commodity type and always sets the key even if it does not have a key.
     * @param commodityType commodityType to recreate with key
     * @return CommodityType with key
     */
    private TopologyDTO.CommodityType commodityWithKey(TopologyDTO.CommodityType commodityType) {
        return TopologyDTO.CommodityType.newBuilder().setType(commodityType.getType()).setKey(commodityType.getKey()).build();
    }

    private float calculateMapOccupancyPercentage(long commoditiesCount) {
        if (entityCommodityToMaxQuantitiesMap.size() == 0) return 0f;

        return ((((Math.abs(entityCommodityToMaxQuantitiesMap.size() - commoditiesCount)) * 1f)
                    / entityCommodityToMaxQuantitiesMap.size()) * 100);
    }

    private EntityCommodityReference createEntityCommodityKey(long entityOid, TopologyDTO.CommodityType commodityType) {
        // CommodityType returned from history component will always have a key - even for commodities like VCPU the key
        // is an empty string. But the commodityType of commodities like VCPU in the broadcast will not have a key
        // (hasKey() will evaluate to false). Because of this difference, the 2 commodityTypes are
        // not treated equal according to the protobuf definition of equals and hence will become 2 separate entries in
        // entityCommodityToMaxQuantitiesMap. To normalize this, we recreate the commodityType and always set the key.
        return new EntityCommodityReference(entityOid, commodityWithKey(commodityType), null);
    }

    private CommodityMaxValue createCommodityMaxValue(ValueSource valSource, double maxValue) {
        // OM-33482 : Set the value to 0 until we understand where the -ve
        // value is coming from.
        if (maxValue < 0) {
            logger.warn("Max value: {} less than 0 from valSource: {}", maxValue, valSource);
            maxValue = 0;
        }
        return new CommodityMaxValue(valSource, maxValue);
    }

    private enum ValueSource {
        DB, // max value from DB.
        TP  // max value set by TP. TP value takes precedendce over DB as it is more current.
    }

    /**
     * Don't compare objects of this type for equality.
     */
    class CommodityMaxValue {

        private final ValueSource valueSource;

        private final double maxValue;

        private final long insertTime;

        private final Double DELTA = 0.0001;

        CommodityMaxValue(ValueSource valSource, double maxValue) {
            this.valueSource = valSource;
            this.maxValue = maxValue;
            this.insertTime = Instant.now().getEpochSecond();
        }

        public double getMaxValue() {
            return maxValue;
        }

        public ValueSource getValueSource() {
            return valueSource;
        }

        /**
         * Returns the age of the max value.
         * If the system clock goes back in time, this method can return
         * negative age.
         *
         */
        public long getAge() {
            long age = Instant.now().getEpochSecond() - insertTime;
            if (age < 0) {
                logger.warn("Negative age. Clock must have gone back.");
            }
            return age;
        }

        @Override
        public int hashCode() {
            return Objects.hash(valueSource, maxValue, insertTime);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof CommodityMaxValue) {
                final CommodityMaxValue other = (CommodityMaxValue) obj;
                return (valueSource == other.getValueSource()
                            && (DoubleMath.fuzzyEquals(maxValue, other.maxValue, DELTA))
                            && insertTime == other.insertTime);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append("valSource=")
                    .append(valueSource)
                    .append(", maxValue=")
                    .append(maxValue)
                    .append(", insertTime=")
                    .append(insertTime)
                    .toString();
        }
    }
}
