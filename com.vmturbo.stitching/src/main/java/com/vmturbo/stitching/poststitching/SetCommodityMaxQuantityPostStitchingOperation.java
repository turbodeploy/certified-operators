package com.vmturbo.stitching.poststitching;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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
     * This configuration value controls how often the commodity max values are
     * loaded from history component.
     */
    private long maxValuesBackgroundLoadFrequencyMinutes;

    /**
     *  This configuration value controls the initial delay before triggering
     *  the background load task when the initial load during startup fails.
     #  For subsequent fetches from the DB or when the initial load succeeds,
     *  the delay will be based on the maxValuesBackgroundLoadFrequencyMinutes
     *  config value.
     */
    private long maxValuesBackgroundLoadDelayOnInitFailureMinutes;

    /**
     * Used to age out any max values in TP. The max retention settings are
     * stored in history. It will be loaded from history when we load the max values.
     * Here we initialize it to the most conservative value which is the
     * max value for the setting.
     */
    private long statsDaysRetentionSettingInSeconds = Math.round(
            GlobalSettingSpecs.StatsRetentionMonths.createSettingSpec()
                .getNumericSettingValueType().getMax()) * (31 * 24 * 60 * 60);

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
            EntityType.CONTAINER,
            EntityType.CONTAINER_POD,
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

    private static final List<Integer> interestedEntityTypesNumbers =
        interestedEntityTypes
            .stream()
            .map(EntityType::getNumber)
            .collect(Collectors.toList());

    private static final GetEntityCommoditiesMaxValuesRequest request =
        GetEntityCommoditiesMaxValuesRequest.newBuilder()
            .addAllEntityTypes(interestedEntityTypesNumbers)
            .build();

    private ScheduledExecutorService backgroundStatsLoadingExecutor =
            Executors.newSingleThreadScheduledExecutor(threadFactory());

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
        this.maxValuesBackgroundLoadDelayOnInitFailureMinutes =
            setMaxValuesConfig.getMaxValuesBackgroundLoadDelayOnInitFailureMinutes();
        // Initialize the maxQuantities map by fetching from history component.
        boolean wasInitialized = initializeMaxQuantityMap();
        long initialDelay = ( wasInitialized ?
                maxValuesBackgroundLoadFrequencyMinutes : maxValuesBackgroundLoadDelayOnInitFailureMinutes);

        logger.info("Max values map initilization status: {}. Init delay={} minutes",
                        wasInitialized, initialDelay);
        logger.info("Setting max values background load frequency to {} minutes",
            maxValuesBackgroundLoadFrequencyMinutes);

        backgroundStatsLoadingExecutor
            .scheduleWithFixedDelay(
                new LoadMaxValuesFromDBTask(statsHistoryClient, entityCommodityToMaxQuantitiesMap),
                initialDelay,
                maxValuesBackgroundLoadFrequencyMinutes,
                TimeUnit.MINUTES);
    }

    /**
     * Fetch the max values from history component and add it to the max values
     * concurrent map. If there is an exception while fetching from history,
     * return false, else return true.
     */
    private boolean initializeMaxQuantityMap() {
        try {

            final DataMetricTimer loadDurationTimer =
                COMMODITY_MAX_VALUES_LOAD_TIME_SUMMARY.labels("initial").startTimer();

            statsHistoryClient
                .getEntityCommoditiesMaxValues(request)
                    .forEachRemaining(entityCommodityMaxValues -> {
                        entityCommodityMaxValues.getCommodityMaxValuesList()
                            .forEach(commodityMaxValue -> {
                                // There is not need for atomic put as no other
                                // thread is mutating the map during initialization.
                                entityCommodityToMaxQuantitiesMap.put(
                                    createEntityCommodityKey(entityCommodityMaxValues.getOid(),
                                        commodityMaxValue.getCommodityType()),
                                        createCommodityMaxValue(ValueSource.DB, commodityMaxValue.getMaxValue()));
                            });
                    });
            double loadTime = loadDurationTimer.observe();
            logger.info("Stats retention period: {} secs. Size of maxValues map after initial load {}. Load time {} secs",
                statsDaysRetentionSettingInSeconds,
                entityCommodityToMaxQuantitiesMap.size(),
                loadTime);
        } catch (StatusRuntimeException e) {
            if (e.getCause() != null &&
                    e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                logger.error("Failed initializing max value map as history component is unavailable");
            }
            else {
                logger.error("Failed initializing max value map", e);
            }
            return false;
        }

        return true;
    }

    /**
     * Task to load the max values map in the background. It can be mutating the
     * max value map concurrently(in a safe manner). If any exception happens
     * while fetching the values from history, we ignore it as the task
     * will be executed on the next schedule.
     */
    private class LoadMaxValuesFromDBTask implements Runnable {

        private final StatsHistoryServiceBlockingStub statsClient;
        private ConcurrentMap<EntityCommodityReference, CommodityMaxValue> maxValuesMap;

        LoadMaxValuesFromDBTask(@Nonnull StatsHistoryServiceBlockingStub statsClient,
                                @Nonnull ConcurrentMap<EntityCommodityReference, CommodityMaxValue> maxValuesMap) {
            this.statsClient = statsClient;
            this.maxValuesMap = maxValuesMap;
        }

        @Override
        public void run() {
            try {

                // first fetch stats rentention setting value.
                statsHistoryClient
                    .getStatsDataRetentionSettings(
                        GetStatsDataRetentionSettingsRequest
                            .getDefaultInstance())
                    .forEachRemaining(setting -> {
                        if (setting.getSettingSpecName().equals(
                                GlobalSettingSpecs.StatsRetentionDays.getSettingName())
                            && setting.hasNumericSettingValue()) {
                            statsDaysRetentionSettingInSeconds = Math.round(
                                setting.getNumericSettingValue().getValue());
                        }
                    });

                final DataMetricTimer loadDurationTimer =
                    COMMODITY_MAX_VALUES_LOAD_TIME_SUMMARY.labels("background").startTimer();

                statsClient
                    .getEntityCommoditiesMaxValues(request)
                        .forEachRemaining(entityCommodityMaxValues -> {
                            entityCommodityMaxValues.getCommodityMaxValuesList()
                                .forEach(dbMaxValue -> {
                                    EntityCommodityReference key =
                                        createEntityCommodityKey(entityCommodityMaxValues.getOid(),
                                            dbMaxValue.getCommodityType());
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

                double loadTime = loadDurationTimer.observe();
                logger.info("Size of maxValues map after background load: {}. Load time: {}",
                    entityCommodityToMaxQuantitiesMap.size(), loadTime);
            } catch (Throwable t) {
                logger.error("Error while fetching max values", t);
            }
        }
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("max-stats-background-loader-thread-%d").build();
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

        long commoditiesCount = 0;
        Iterable<TopologyEntity> entitiesIterable = entities::iterator;
        for (TopologyEntity entity : entitiesIterable) {
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
                    logger.warn("Commodity has -ve used value : {}", commoditySoldDTO);
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

        // History returns the max values for all the entities it has info on.
        // But in the current run, we may not be using many entities. This log
        // message will give an idea on the useless info we are storing in the
        // map. If the % of redundant info becomes too high, we would have to
        // start deleting entries from the map(a cache won't be useful here).
        // This is a rough estimate. Over time it's assumed that the number of
        // entries in the DB will be equal to or more than the
        // entities(and hence the commodities) discovered by TP.
        // To get the correct amount, a set_difference has to be calculated which
        // is a more expensive operation.
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

    private float calculateMapOccupancyPercentage(long commoditiesCount) {
        if (entityCommodityToMaxQuantitiesMap.size() == 0) return 0f;

        return ((((Math.abs(entityCommodityToMaxQuantitiesMap.size() - commoditiesCount)) * 1f)
                    / entityCommodityToMaxQuantitiesMap.size()) * 100);
    }

    private EntityCommodityReference createEntityCommodityKey(long entityOid, TopologyDTO.CommodityType commodityType) {
        return new EntityCommodityReference(entityOid, commodityType, null);
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
    private class CommodityMaxValue {

        private final ValueSource valueSource;

        private final double maxValue;

        private final long insertTime;

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
            if (obj instanceof CommodityMaxValue) {
                final CommodityMaxValue other = (CommodityMaxValue) obj;
                return (valueSource == other.getValueSource()
                            // should add an epsilon bounds check instead of
                            // checking for pure equality.
                            && (Double.compare(maxValue, other.maxValue) == 0)
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
