package com.vmturbo.topology.processor.history.percentile;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.forecasting.TimeInMillisConstants;
import com.vmturbo.commons.utils.ThrowingFunction;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractCachingHistoricalEditor;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord.Builder;

/**
 * Calculate and provide percentile historical value for topology commodities.
 */
public class PercentileEditor extends
                AbstractCachingHistoricalEditor<PercentileCommodityData,
                    PercentilePersistenceTask,
                    PercentileHistoricalEditorConfig,
                    PercentileRecord,
                    StatsHistoryServiceStub,
                    PercentileRecord.Builder> {
    private static final Logger logger = LogManager.getLogger();
    // certain sold commodities should have percentile calculated from real-time points
    // even if dedicated percentile utilizations are absent in the mediation
    private static final Set<CommodityType> REQUIRED_SOLD_COMMODITY_TYPES = Sets.immutableEnumSet(
                        CommodityDTO.CommodityType.VCPU,
                        CommodityDTO.CommodityType.VMEM);
    // percentile on a bought commodity will not add up unless there is no more than one
    // consumer per provider, so only certain commodity types are applicable
    private static final Set<CommodityType> ENABLED_BOUGHT_COMMODITY_TYPES = Sets.immutableEnumSet(
                    CommodityDTO.CommodityType.IMAGE_CPU,
                    CommodityDTO.CommodityType.IMAGE_MEM,
                    CommodityDTO.CommodityType.IMAGE_STORAGE);

    /**
     * Entity types for which percentile calculation is supported.
     * These entities trade commodities with the types listed in {@link
     * PercentileEditor#REQUIRED_SOLD_COMMODITY_TYPES} and
     * {@link PercentileEditor#ENABLED_BOUGHT_COMMODITY_TYPES}
     */
    private static final Set<EntityType> NOT_APPLICABLE_ENTITY_TYPES =
            ImmutableSet.of(EntityType.DATABASE_SERVER, EntityType.DATABASE, EntityType.CONTAINER,
                EntityType.CONTAINER_POD);
    private static final DataMetricSummary SETTINGS_CHANGE_SUMMARY_METRIC =
                DataMetricSummary.builder()
                                .withName("tp_historical_percentile_window_change")
                                .withHelp("The time spent on handling changes of observation windows settings "
                                          + "(this is also part of tp_historical_initialization_time_percentile)")
                                .build();
    private static final DataMetricSummary MAINTENANCE_SUMMARY_METRIC =
                DataMetricSummary.builder()
                               .withName("tp_historical_percentile_maintenance")
                               .withHelp("The time spent on daily maintenance of percentile cache "
                                         + "(this is also part of tp_historical_completion_time_percentile)")
                               .build();

    private final Clock clock;
    private boolean historyInitialized;
    // moment of most recent checkpoint i.e. save of full window in the persistent store
    private long lastCheckpointMs;
    // number of checkpoints happened so far for logging purposes
    private long checkpoints;
    /*
     * Flag set to true in case we need to enforce maintenance. Currently there is only one use
     * case for this - when percentile observation window changed. In case there are no changes
     * in percentile observation window than it is setting to false.
     */
    private boolean enforceMaintenance;

    /**
     * Construct the instance of percentile editor.
     *
     * @param config configuration values
     * @param statsHistoryClient persistence component access handler
     * @param clock the {@link Clock}
     * @param historyLoadingTaskCreator creator of task to load or save data
     */
    public PercentileEditor(@Nonnull PercentileHistoricalEditorConfig config,
                    @Nonnull StatsHistoryServiceStub statsHistoryClient, @Nonnull Clock clock,
                    @Nonnull BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, PercentilePersistenceTask> historyLoadingTaskCreator) {
        super(config, statsHistoryClient, historyLoadingTaskCreator, PercentileCommodityData::new);
        this.clock = clock;
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                PlanScope scope) {
        // percentile should not be set for baseline and cluster headroom plans
        if (TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo)) {
            return false;
        }
        if (CollectionUtils.isEmpty(changes)) {
            return true;
        }
        return !changes.stream()
                        .filter(ScenarioChange::hasPlanChanges)
                        .anyMatch(change -> change.getPlanChanges().hasHistoricalBaseline());
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        return !NOT_APPLICABLE_ENTITY_TYPES.contains(EntityType.forNumber(entity.getEntityType()));
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        return commSold.hasUtilizationData() || REQUIRED_SOLD_COMMODITY_TYPES
                        .contains(CommodityType.forNumber(commSold.getCommodityType().getType()));
    }

    @Override
    public boolean
           isCommodityApplicable(TopologyEntity entity,
                                 TopologyDTO.CommodityBoughtDTO.Builder commBought) {
        return commBought.hasUtilizationData() && ENABLED_BOUGHT_COMMODITY_TYPES
                        .contains(CommodityType.forNumber(commBought.getCommodityType().getType()));
    }

    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    public void initContext(@Nonnull HistoryAggregationContext context,
                            @Nonnull List<EntityCommodityReference> eligibleComms)
                    throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);

        loadPersistedData(context);
        if (!context.isPlan()) {
            checkObservationPeriodsChanged(context);
        }
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityRefs) {
        initializeCacheValues(context, commodityRefs);
        return Collections.emptyList();
    }

    @Override
    public void cleanupCache(@Nonnull final List<EntityCommodityReference> commodities) {
        // We don't need to clean up cache for percentile
    }

    private void initializeCacheValues(@Nonnull HistoryAggregationContext context,
                    @Nonnull Collection<? extends EntityCommodityReference> commodityRefs) {
        // percentile data will be loaded in a single-threaded way into blobs (not chunked)
        // initialize cache values for entries from topology
        // in order to get the percentiles for them calculated as well if they do not get read
        commodityRefs.forEach(commRef -> {
            EntityCommodityFieldReference field =
                (new EntityCommodityFieldReference(commRef, CommodityField.USED))
                    .getLiveTopologyFieldReference(context);
            getCache().computeIfAbsent(field, fieldRef -> {
                PercentileCommodityData data = historyDataCreator.get();
                data.init(field, null, getConfig(), context);
                return data;
            });
        });
    }

    @Override
    public void completeBroadcast(@Nonnull HistoryAggregationContext context) throws HistoryCalculationException, InterruptedException {
        super.completeBroadcast(context);
        if (!context.isPlan()) {
            // perform daily maintenance if needed - synchronously within broadcast (consider scheduling)
            maintenance(context);
            // persist the daily blob
            persistBlob(getCheckpoint(), getMaintenanceWindowInMs(),
                            UtilizationCountStore::getLatestCountsRecord);
            // print the utilization counts from cache for the configured OID in logs
            // if debug is enabled.
            debugLogDataValues(logger,
                            (data) -> String.format("Percentile utilization counts: %s",
                                            data.getUtilizationCountStore().toDebugString()));
        }
    }

    private void persistBlob(long taskTimestamp, long periodMs,
                    ThrowingFunction<UtilizationCountStore, Builder, HistoryCalculationException> countStoreToRecordStore)
                    throws HistoryCalculationException, InterruptedException {
        final PercentileCounts.Builder builder = PercentileCounts.newBuilder();
        for (PercentileCommodityData data : getCache().values()) {
            builder.addPercentileRecords(
                            countStoreToRecordStore.apply(data.getUtilizationCountStore()));
        }
        writeBlob(builder, periodMs, taskTimestamp);
    }

    private PercentilePersistenceTask createTask(long startTimestamp) {
        return createLoadingTask(Pair.create(startTimestamp, null));
    }

    private void loadPersistedData(@Nonnull HistoryAggregationContext context) throws HistoryCalculationException, InterruptedException {
        if (!historyInitialized) {
            Stopwatch sw = Stopwatch.createStarted();
            // read the latest and full window blobs if haven't yet, set into cache
            final PercentilePersistenceTask task =
                            createTask(PercentilePersistenceTask.TOTAL_TIMESTAMP);
            Map<EntityCommodityFieldReference, PercentileRecord> fullPage =
                            task.load(Collections.emptyList(), getConfig());
            for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> fullEntry : fullPage.entrySet()) {
                EntityCommodityFieldReference field = fullEntry.getKey();
                PercentileRecord record = fullEntry.getValue();
                PercentileCommodityData data =
                                getCache().computeIfAbsent(field,
                                                           ref -> historyDataCreator.get());
                data.init(field, null, getConfig(), context);
                data.getUtilizationCountStore().addFullCountsRecord(record, true);
                data.getUtilizationCountStore().setPeriodDays(record.getPeriod());
            }
            logger.info("Initialized percentile full window data for {} commodities in {}",
                         fullPage::size, sw::toString);

            sw.reset();
            sw.start();
            long checkpointMs = getCheckpoint();
            Map<EntityCommodityFieldReference, PercentileRecord> latestPage =
                        createTask(checkpointMs).load(Collections.emptyList(), getConfig());
            for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> latestEntry : latestPage.entrySet()) {
                PercentileCommodityData data =
                                getCache().computeIfAbsent(latestEntry.getKey(),
                                                       ref -> historyDataCreator.get());
                PercentileRecord record = latestEntry.getValue();
                if (data.getUtilizationCountStore() == null) {
                    data.init(latestEntry.getKey(), record, getConfig(),
                              context);
                } else {
                    data.getUtilizationCountStore().setLatestCountsRecord(record);
                }
            }
            logger.info("Initialized percentile latest window data for timestamp {} and {} commodities in {}",
                         () -> checkpointMs, latestPage::size, sw::toString);

            historyInitialized = true;
            lastCheckpointMs = task.getLastCheckpointMs() != 0 ? task.getLastCheckpointMs() : checkpointMs;
        }
    }

    private void checkObservationPeriodsChanged(@Nonnull HistoryAggregationContext context)
            throws HistoryCalculationException, InterruptedException {
        final Map<Long, Integer> entity2period = getEntityToPeriod(context);

        final Map<EntityCommodityFieldReference, PercentileCommodityData> changedPeriodEntries =
                new HashMap<>();
        int maxOfChangedPeriods = 0;
        for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : getCache().entrySet()) {
            final Integer observationPeriod = entity2period.get(entry.getKey().getEntityOid());
            if (observationPeriod != null) {
                final UtilizationCountStore store = entry.getValue().getUtilizationCountStore();
                // We are only interested in "changes" but not in handling the new entities.
                if (store.getPeriodDays() != observationPeriod) {
                    changedPeriodEntries.put(entry.getKey(), entry.getValue());
                    // Update percentile data with observation windows values.
                    store.setPeriodDays(observationPeriod);
                    // Latest counts is already in memory, copy to full to load 1 window less.
                    store.copyCountsFromLatestToFull();
                    // The needed pages count to load will be determined by the max observation period.
                    maxOfChangedPeriods = Math.max(maxOfChangedPeriods, observationPeriod);
                }
            }
        }

        if (changedPeriodEntries.isEmpty()) {
            logger.debug("Observation periods for cache entries have not changed.");
            enforceMaintenance = false;
            return;
        }

        try (DataMetricTimer timer = SETTINGS_CHANGE_SUMMARY_METRIC.startTimer();
             CacheBackup backup = createCacheBackup()) {
            final Stopwatch sw = Stopwatch.createStarted();
            // Read as many page blobs from persistence as constitute the max of new periods
            // and accumulate them into percentile cache, respect per-entity observation window settings

            // Calculate maintenance window in milliseconds.
            final long windowMillis = getConfig().getMaintenanceWindowHours() *
                    TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS;
            final long checkpoint = getCheckpoint();
            // Calculate timestamp for farthest snapshot.
            // Latest counts is already in memory, need to load 1 observation window less.
            long startTimestamp = checkpoint -
                    (maxOfChangedPeriods * TimeInMillisConstants.DAY_LENGTH_IN_MILLIS) +
                    windowMillis;
            // Load snapshots by selecting from history with shifting start timestamp by maintenance window.
            while (startTimestamp < checkpoint) {
                final Map<EntityCommodityFieldReference, PercentileRecord> page =
                        createTask(startTimestamp).load(
                                Collections.emptyList(), getConfig());
                // For each entry in cache with a changed observation period,
                // apply loaded percentile commodity entries.
                for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : changedPeriodEntries
                        .entrySet()) {
                    final UtilizationCountStore store = entry.getValue().getUtilizationCountStore();
                    // Calculate bound timestamp for specific entry.
                    // This is necessary if the changed observation period
                    // for a given entry is less than the maximum modified observation period.
                    final long timestampBound = checkpoint -
                            (store.getPeriodDays() * TimeInMillisConstants.DAY_LENGTH_IN_MILLIS) +
                            windowMillis;
                    if (timestampBound <= startTimestamp) {
                        final PercentileRecord percentileRecord = page.get(entry.getKey());
                        if (percentileRecord != null) {
                            store.addFullCountsRecord(percentileRecord, false);
                        }
                    }
                }
                if (logger.isTraceEnabled()) {
                    logger.trace(
                            "Loaded {} percentile commodity entries for timestamp {} was applied to update the cache",
                            page.size(), startTimestamp);
                }
                startTimestamp += windowMillis;
            }
            enforceMaintenance = true;
            backup.keepCacheOnClose();
            logger.info(
                    "Percentile observation windows changed for {} entries, recalculated from {} pages in {}",
                    changedPeriodEntries.size(), maxOfChangedPeriods, sw);
        }
    }

    private void maintenance(@Nonnull HistoryAggregationContext context) throws InterruptedException {
        if (!historyInitialized) {
            logger.warn("Percentile history is not initialized.");
            return;
        }

        final long checkpointMs = getCheckpoint();

        if (!enforceMaintenance && checkpointMs <= lastCheckpointMs) {
            logger.trace("Percentile cache checkpoint skipped - not enough time passed since last checkpoint "
                         + lastCheckpointMs);
            return;
        }

        final Stopwatch sw = Stopwatch.createStarted();
        try (DataMetricTimer timer = MAINTENANCE_SUMMARY_METRIC.startTimer();
                        CacheBackup backup = createCacheBackup()) {
            logger.debug("Performing percentile cache maintenance {} for {}",
                         ++checkpoints,
                         Instant.ofEpochMilli(checkpointMs));
            if (enforceMaintenance
                && lastCheckpointMs + getMaintenanceWindowInMs() > checkpointMs) {
                enforcedMaintenance(context, checkpointMs);
            } else {
                final Map<Long, Integer> entity2period = getEntityToPeriod(context);
                final Set<Integer> periods = new HashSet<>(entity2period.values());

                /*
                 Process outdated percentiles day by day:

                 We take all possible periods of observation (7 days, 30 days and 90 days)
                 For each period:
                     outdatedPercentiles = load outdated percentiles from (lastCheckpointDay - period) day
                     for each entity in cache:
                         if outdatedPercentiles contain entity:
                             subtract outdated utilization counts from cache
                 Save current cache to DB.
                */
                Map<PercentileCommodityData, List<PercentileRecord>> dataRef2outdatedRecords = new HashMap<>();
                // initialize with empty collections
                getCache().values().forEach(dataRef -> dataRef2outdatedRecords.put(dataRef, new LinkedList<>()));
                for (long currentCheckpointMs = lastCheckpointMs + getMaintenanceWindowInMs();
                     currentCheckpointMs <= checkpointMs;
                     currentCheckpointMs += getMaintenanceWindowInMs()) {
                    for (Integer periodInDays : periods) {
                        final long outdatedTimestamp = currentCheckpointMs - periodInDays
                                                                             * TimeInMillisConstants.DAY_LENGTH_IN_MILLIS;
                        final PercentilePersistenceTask loadOutdated = createTask(outdatedTimestamp);
                        logger.debug("Started checkpoint percentile cache for timestamp {} with period of {} days. Outdated percentile timestamp is {} ",
                                     currentCheckpointMs,
                                     periodInDays,
                                     outdatedTimestamp);

                        final Map<EntityCommodityFieldReference, PercentileRecord> oldValues =
                                        loadOutdated.load(Collections.emptyList(), getConfig());
                        for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> fieldRef2data : getCache()
                                        .entrySet()) {
                            final EntityCommodityFieldReference ref = fieldRef2data.getKey();
                            if (!periodInDays.equals(entity2period.get(ref.getEntityOid()))) {
                                continue;
                            }
                            final PercentileRecord oldRecord = oldValues.get(ref);
                            if (oldRecord != null) {
                                // accumulate daily records that go out of observation window for each cached commodity reference
                                dataRef2outdatedRecords.get(fieldRef2data.getValue()).add(oldRecord);
                            }
                        }
                    }
                }

                final PercentileCounts.Builder total = PercentileCounts.newBuilder();
                for (Map.Entry<PercentileCommodityData, List<PercentileRecord>> entry : dataRef2outdatedRecords.entrySet()) {
                    final PercentileRecord.Builder checkpoint =
                                    entry.getKey().checkpoint(entry.getValue());
                    total.addPercentileRecords(checkpoint);
                }
                writeBlob(total, checkpointMs, PercentilePersistenceTask.TOTAL_TIMESTAMP);
                lastCheckpointMs = checkpointMs;
            }

            backup.keepCacheOnClose();

        } catch (HistoryCalculationException e) {
            logger.error("{} maintenance failed for '{}' checkpoint, last checkpoint was at '{}'",
                            enforceMaintenance ? "Enforced" : "Regular",
                            Instant.ofEpochMilli(checkpointMs),
                            Instant.ofEpochMilli(lastCheckpointMs), e);
        } finally {
            logger.info("Percentile cache {}maintenance {} took {}",
                            enforceMaintenance ? "enforced " : "", checkpoints, sw);
        }
    }

    private void enforcedMaintenance(@Nonnull HistoryAggregationContext context, long checkpointMs)
                    throws InterruptedException, HistoryCalculationException {
        logger.debug("Performing enforced percentile cache maintenance for {}",
                     () -> Instant.ofEpochMilli(checkpointMs));
        final long yesterday = checkpointMs - getMaintenanceWindowInMs();
        final Map<EntityCommodityFieldReference, PercentileRecord> yesterdayRecords =
                        createTask(yesterday).load(Collections.emptyList(), getConfig());

        // initialize LATEST in cache for each entry in loaded percentile data
        initializeCacheValues(context, yesterdayRecords.keySet());

        // accumulate current day's LATEST in cache with data from yesterday
        for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> entry : yesterdayRecords
                        .entrySet()) {
            final PercentileCommodityData percentileCommodityData = getCache().get(entry.getKey());
            if (percentileCommodityData != null) {
                percentileCommodityData.getUtilizationCountStore()
                                .addLatestCountsRecord(entry.getValue());
            }
        }

        // persist the blobs into DB
        persistBlob(yesterday,
                    getMaintenanceWindowInMs(),
                    UtilizationCountStore::getLatestCountsRecord);
        // checkpoint() also clears latest
        persistBlob(PercentilePersistenceTask.TOTAL_TIMESTAMP,
                    lastCheckpointMs,
                    store -> store.checkpoint(Collections.emptyList()));
    }

    /**
     * Creates {@link CacheBackup} instance which is wrapping the original cache and will be
     * responsible for restoring original cache in case of a failure while cache updating
     * operation.
     *
     * @return instance of the {@link CacheBackup}
     * @throws HistoryCalculationException in case {@link CacheBackup} cannot be created.
     */
    @Nonnull
    protected CacheBackup createCacheBackup() throws HistoryCalculationException {
        return new CacheBackup(getCache());
    }

    private void writeBlob(PercentileCounts.Builder blob, long periodMs, long startTimestamp)
                    throws HistoryCalculationException, InterruptedException {
        logger.debug("Writing {} percentile entries with timestamp {} and period {}",
                     blob.getPercentileRecordsCount(), startTimestamp, periodMs);
        createTask(startTimestamp).save(blob.build(), periodMs, getConfig());
    }

    @Nonnull
    private Map<Long, Integer> getEntityToPeriod(@Nonnull HistoryAggregationContext context) {
        /*
         * Calculate per-entity observation periods for all entities in the topology,
         * to handle setting changes individually.
         * Note that maintaining this imposes considerable performance cost
         * as it requires iterations over topology for every broadcast and extra
         * loading times when some periods change.
         * We should consider introducing per-entity-type period settings concept.
         * Or preferably even just one global observation window setting.
         */
        return context.entityToSetting(Predicates.alwaysTrue(),
            entity -> getConfig().getObservationPeriod(context, entity.getOid()));
    }

    private long getCheckpoint() {
        long checkpointMs = clock.millis();
        double window = getMaintenanceWindowInMs();
        return (long)(Math.floor(checkpointMs / window) * window);
    }

    private long getMaintenanceWindowInMs() {
        return getConfig().getMaintenanceWindowHours() *
               TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS;
    }

    /**
     * Helper class to store the latest valid state of cache and restore it in case of failures.
     */
    protected static class CacheBackup implements AutoCloseable {
        private final Map<EntityCommodityFieldReference, PercentileCommodityData> originalCache;
        private Map<EntityCommodityFieldReference, PercentileCommodityData> backup;

        public CacheBackup(@Nonnull Map<EntityCommodityFieldReference, PercentileCommodityData> originalCache)
                        throws HistoryCalculationException {
            this.originalCache = Objects.requireNonNull(originalCache);
            backup = new LinkedHashMap<>();
            for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : originalCache
                            .entrySet()) {
                backup.put(entry.getKey(), new PercentileCommodityData(entry.getValue()));
            }
        }

        /**
         * Don't restore the original state of cache on {@link CacheBackup#close}.
         */
        public void keepCacheOnClose() {
            backup = null;
        }

        @Override
        public void close() {
            if (backup != null) {
                originalCache.clear();
                originalCache.putAll(backup);
                backup = null;
            }
        }
    }
}
