package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.utils.ThrowingFunction;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.AbstractCachingHistoricalEditor;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.InvalidHistoryDataException;
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
    private static final Set<CommodityType> ENABLED_BOUGHT_COMMODITY_TYPES =
            Sets.immutableEnumSet(CommodityDTO.CommodityType.IMAGE_CPU,
                    CommodityDTO.CommodityType.IMAGE_MEM, CommodityDTO.CommodityType.IMAGE_STORAGE,
                    CommodityType.DTU, CommodityType.STORAGE_AMOUNT);

    /**
     * Entity types for which percentile calculation is supported.
     * These entities trade commodities with the types listed in {@link
     * PercentileEditor#REQUIRED_SOLD_COMMODITY_TYPES} and
     * {@link PercentileEditor#ENABLED_BOUGHT_COMMODITY_TYPES}
     */
    private static final Set<EntityType> NOT_APPLICABLE_ENTITY_TYPES =
            ImmutableSet.of(EntityType.DATABASE_SERVER, EntityType.CONTAINER,
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
     * Pre-calculated in non-plan context during initialization to reuse in multiple stages.
     */
    private Map<Long, Integer> entity2period;

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

        // will be required for initial loading, maintenance, observation period change check
        if (!context.isPlan() || !historyInitialized) {
            entity2period = getEntityToPeriod(context);
        }

        loadPersistedData(context, eligibleComms, checkpoint -> {
            // read the latest and full window blobs if haven't yet, set into cache
            final PercentilePersistenceTask task = createTask(checkpoint);
            return Pair.create(task.getLastCheckpointMs(),
                            task.load(Collections.emptyList(), getConfig()));
        }, maintenance -> Pair.create(maintenance,
                        createTask(maintenance).load(Collections.emptyList(), getConfig())));

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
            final long checkpointMs = getCheckpoint();

            // perform enforce maintenance if required
            enforcedMaintenance(context, checkpointMs);

            // persist the daily blob
            persistDailyRecord(context, checkpointMs);

            // perform daily maintenance if needed - synchronously within broadcast (consider scheduling)
            maintenance(context, checkpointMs);

            // print the utilization counts from cache for the configured OID in logs
            // if debug is enabled.
            debugLogDataValues(logger,
                            (data) -> String.format("Percentile utilization counts: %s",
                                            data.getUtilizationCountStore().toDebugString()));
        }
    }

    private void persistDailyRecord(HistoryAggregationContext context, long checkpointMs) throws InterruptedException, HistoryCalculationException {
        // When we are in checkpoint time, we persist the latest in previous days record. For
        // two reasons: this  ensures the sum of all records in the observation period is the
        // same as full record. The maintenance step is going to clear today's record in
        // memory which result that last datapoint getting lost.
        long checkpointTime = lastCheckpointMs >= checkpointMs
            ? checkpointMs : checkpointMs - getMaintenanceWindowInMs();

        persistBlob(checkpointTime, getMaintenanceWindowInMs(),
            UtilizationCountStore::getLatestCountsRecord);
    }

    private void persistBlob(long taskTimestamp, long periodMs,
                    ThrowingFunction<UtilizationCountStore, Builder, HistoryCalculationException> countStoreToRecordStore)
                    throws HistoryCalculationException, InterruptedException {
        writeBlob(createBlob(countStoreToRecordStore), periodMs, taskTimestamp);
    }

    private PercentileCounts.Builder createBlob(
                    ThrowingFunction<UtilizationCountStore, Builder, HistoryCalculationException> countStoreToRecordStore)
                    throws HistoryCalculationException, InterruptedException {
        final PercentileCounts.Builder builder = PercentileCounts.newBuilder();
        for (PercentileCommodityData data : getCache().values()) {
            builder.addPercentileRecords(
                            countStoreToRecordStore.apply(data.getUtilizationCountStore()));
        }
        return builder;
    }

    private PercentilePersistenceTask createTask(long startTimestamp) {
        return createLoadingTask(Pair.create(startTimestamp, null));
    }

    private synchronized void loadPersistedData(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> eligibleComms,
                    @Nonnull ThrowingFunction<Long, Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>>, HistoryCalculationException> latestLoader,
                    @Nonnull ThrowingFunction<Long, Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>>, HistoryCalculationException> fullLoader)
                    throws HistoryCalculationException, InterruptedException {
        if (!historyInitialized) {
            Stopwatch sw = Stopwatch.createStarted();
            // read the latest and full window blobs if haven't yet, set into cache
            try {
                final Map<EntityCommodityFieldReference, PercentileRecord> fullPage =
                                fullLoader.apply(PercentilePersistenceTask.TOTAL_TIMESTAMP).getSecond();
                for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> fullEntry : fullPage.entrySet()) {
                    EntityCommodityFieldReference field = fullEntry.getKey();
                    PercentileRecord record = fullEntry.getValue();
                    PercentileCommodityData data =
                                    getCache().computeIfAbsent(field, ref -> historyDataCreator.get());
                    if (data.getUtilizationCountStore() == null) {
                        data.init(field, null, getConfig(), context);
                    }
                    data.getUtilizationCountStore().addFullCountsRecord(record);
                    data.getUtilizationCountStore().setPeriodDays(record.getPeriod());
                }
                logger.info("Initialized percentile full window data for {} commodities in {}",
                             fullPage::size, sw::toString);
            } catch (InvalidHistoryDataException e) {
                logger.warn("Failed to read percentile full window data, re-assembling from the daily blobs", e);
                initializeCacheValues(context, eligibleComms);
                int maxPeriod = getCache().values().stream()
                                .map(PercentileCommodityData::getUtilizationCountStore)
                                .map(UtilizationCountStore::getPeriodDays).max(Long::compare)
                                .orElse(PercentileHistoricalEditorConfig
                                                .getDefaultObservationPeriod());
                reassembleFullPage(getCache(), maxPeriod, false);
            }

            sw.reset();
            sw.start();
            long checkpointMs = getCheckpoint();
            try {
                final Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>> loaded =
                                latestLoader.apply(checkpointMs);
                checkpointMs = loaded.getFirst() != 0 ? loaded.getFirst() : checkpointMs;
                final Map<EntityCommodityFieldReference, PercentileRecord> latestPage =
                                loaded.getSecond();
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
                             checkpointMs, latestPage.size(), sw);
            } catch (InvalidHistoryDataException e) {
                logger.warn("Failed to load percentile latest window data, proceeding with empty", e);
            }

            historyInitialized = true;
            lastCheckpointMs = checkpointMs;
        }
    }

    private void checkObservationPeriodsChanged(@Nonnull HistoryAggregationContext context)
            throws HistoryCalculationException, InterruptedException {
        final Map<EntityCommodityFieldReference, PercentileCommodityData> changedPeriodEntries =
                new HashMap<>();
        int maxOfChangedPeriods = 0;
        for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : getCache().entrySet()) {
            final Integer observationPeriod = entity2period.get(entry.getKey().getEntityOid());
            if (observationPeriod != null) {
                final UtilizationCountStore store = entry.getValue().getUtilizationCountStore();
                // We are only interested in "changes" but not in handling the new entities.
                if (entry.getValue().needsReinitialization(entry.getKey(), context, getConfig())) {
                    changedPeriodEntries.put(entry.getKey(), entry.getValue());
                    // Update percentile data with observation windows values.
                    // The needed pages count to load will be determined by the max observation period.
                    maxOfChangedPeriods = Math.max(maxOfChangedPeriods, observationPeriod);
                }
                store.setPeriodDays(observationPeriod);
            }
        }

        if (changedPeriodEntries.isEmpty()) {
            logger.debug("Observation periods for cache entries have not changed.");
            enforceMaintenance = false;
            return;
        }

        reassembleFullPage(changedPeriodEntries, maxOfChangedPeriods, true);
    }

    private void reassembleFullPage(
                    Map<EntityCommodityFieldReference, PercentileCommodityData> entriesToUpdate,
                    int maxOfPeriods,
                    boolean enforceMaintenance)
                    throws HistoryCalculationException, InterruptedException {
        logger.debug("Reassembling full page for {} entries from up to {} pages",
                        entriesToUpdate.size(), maxOfPeriods);
        try (DataMetricTimer timer = SETTINGS_CHANGE_SUMMARY_METRIC.startTimer();
             CacheBackup backup = createCacheBackup()) {
            final Stopwatch sw = Stopwatch.createStarted();

            for (PercentileCommodityData data : entriesToUpdate.values()) {
                data.getUtilizationCountStore().clearFullRecord();
            }

            // Read as many page blobs from persistence as constitute the max of new periods
            // and accumulate them into percentile cache, respect per-entity observation window settings

            // Calculate maintenance window in milliseconds.
            final long windowMillis = TimeUnit.HOURS.toMillis(
                    getConfig().getMaintenanceWindowHours());
            final long checkpoint = getCheckpoint();
            // Calculate timestamp for farthest snapshot.
            long startTimestamp = checkpoint
                            - (TimeUnit.DAYS.toMillis(maxOfPeriods))
                            + windowMillis;
            logger.debug("Loading daily blobs in range from {} to {}, step {}",
                            Instant.ofEpochMilli(startTimestamp),
                            Instant.ofEpochMilli(checkpoint),
                            windowMillis);
            // Load snapshots by selecting from history with shifting start timestamp by maintenance window.
            while (startTimestamp < checkpoint) {
                final Map<EntityCommodityFieldReference, PercentileRecord> page;
                try {
                    page = createTask(startTimestamp).load(Collections.emptyList(), getConfig());
                } catch (InvalidHistoryDataException e) {
                    logger.warn("Failed to read percentile daily blob for {}, skipping it for full page reassembly",
                                    startTimestamp, e);
                    startTimestamp += windowMillis;
                    continue;
                }

                // For each entry in cache with a changed observation period,
                // apply loaded percentile commodity entries.
                for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : entriesToUpdate
                        .entrySet()) {
                    final UtilizationCountStore store = entry.getValue().getUtilizationCountStore();
                    // Calculate bound timestamp for specific entry.
                    // This is necessary if the changed observation period
                    // for a given entry is less than the maximum modified observation period.
                    final long timestampBound = checkpoint
                            - TimeUnit.DAYS.toMillis(store.getPeriodDays()) + windowMillis;
                    if (timestampBound <= startTimestamp) {
                        final PercentileRecord percentileRecord = page.get(entry.getKey());
                        if (percentileRecord != null) {
                            store.addFullCountsRecord(percentileRecord);
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

            // at this point full record is a sum of all previous days up to period
            // except the latest page -> add latest to full, rescaling as necessary
            for (PercentileCommodityData entry : entriesToUpdate.values()) {
                entry.getUtilizationCountStore().setLatestCountsRecord(
                                entry.getUtilizationCountStore().getLatestCountsRecord().build());
            }

            this.enforceMaintenance = enforceMaintenance;
            backup.keepCacheOnClose();
            logger.info("Reassembled full page for {} entries from {} pages in {}",
                    entriesToUpdate.size(), maxOfPeriods, sw);
        }
    }

    private void maintenance(@Nonnull HistoryAggregationContext context, long checkpointMs) throws InterruptedException {
        if (!historyInitialized) {
            logger.warn("Percentile history is not initialized.");
            return;
        }

        if (checkpointMs <= lastCheckpointMs) {
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
                    final long outdatedTimestamp =
                            currentCheckpointMs - TimeUnit.DAYS.toMillis(periodInDays);
                    final PercentilePersistenceTask loadOutdated = createTask(outdatedTimestamp);
                    logger.debug("Started checkpoint percentile cache for timestamp {} with period of {} days. Outdated percentile timestamp is {} ",
                                 currentCheckpointMs,
                                 periodInDays,
                                 outdatedTimestamp);

                    final Map<EntityCommodityFieldReference, PercentileRecord> oldValues;
                    try {
                        oldValues = loadOutdated.load(Collections.emptyList(), getConfig());
                    } catch (InvalidHistoryDataException e) {
                        logger.warn("Failed to read percentile daily blob for {}, skipping it for maintenance",
                                        outdatedTimestamp, e);
                        continue;
                    }
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
                    throws InterruptedException {
        if (!historyInitialized) {
            logger.warn("Percentile history is not initialized.");
            return;
        }

        // If maintenance is not enforced or regular maintenance is going to happen just return.
        if (!enforceMaintenance || checkpointMs > lastCheckpointMs) {
            logger.debug("Enforced maintenance is not required as {}.", () -> enforceMaintenance
                ? "the regular maintenance is happening." : "the flag is not set.");
            return;
        }

        final Stopwatch sw = Stopwatch.createStarted();
        try (DataMetricTimer timer = MAINTENANCE_SUMMARY_METRIC.startTimer();
             CacheBackup backup = createCacheBackup()) {

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
                                    .addBeforeLatest(entry.getValue());
                }
            }

            // persist the blobs into DB
            persistBlob(yesterday,
                        getMaintenanceWindowInMs(),
                        UtilizationCountStore::getLatestCountsRecord);
            // checkpoint() also clears latest
            persistBlob(PercentilePersistenceTask.TOTAL_TIMESTAMP,
                        lastCheckpointMs,
                        store -> store.checkpoint(Collections.emptyList(), true));
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
        return context.entityToSetting(this::isEntityApplicable,
            entity -> getConfig().getObservationPeriod(context, entity.getOid()));
    }

    private long getCheckpoint() {
        long checkpointMs = clock.millis();
        double window = getMaintenanceWindowInMs();
        return (long)(Math.floor(checkpointMs / window) * window);
    }

    private long getMaintenanceWindowInMs() {
        return TimeUnit.HOURS.toMillis(getConfig().getMaintenanceWindowHours());
    }

    @Override
    protected void restoreState(@Nonnull final byte[] bytes) throws DiagnosticsException {
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                        .setTopologyId(getConfig().getRealtimeTopologyContextId()).build();
        final Map<Integer, Collection<TopologyEntity>> typeToIndex = new HashMap<>();
        final Long2ObjectMap<TopologyEntity> oidToEntity = new Long2ObjectOpenHashMap<>();
        final TopologyGraph<TopologyEntity> graph = new TopologyGraph<>(oidToEntity, typeToIndex);
        final Map<Long, EntitySettings> oidToSettings = new HashMap<>();
        final Map<Long, SettingPolicy> defaultPolicies = new HashMap<>();
        final GraphWithSettings graphWithSettings =
                        new GraphWithSettings(graph, oidToSettings, defaultPolicies);
        final HistoryAggregationContext context =
                        new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        try (InputStream source = new ByteArrayInputStream(bytes)) {
            historyInitialized = false;
            getCache().clear();
            loadPersistedData(context, Collections.emptyList(),
                            (timestamp) -> loadCachePart(timestamp, source, "latest"),
                            (timestamp) -> loadCachePart(timestamp, source, "full"));
        } catch (HistoryCalculationException | InterruptedException | IOException e) {
            getCache().clear();
            throw new DiagnosticsException(
                            String.format("Cannot load percentile cache from '%s' file",
                                            getFileName()), e);
        }
    }

    @Nonnull
    private static Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>> loadCachePart(
                    @Nonnull Long timestamp, @Nonnull InputStream source, @Nonnull String partType)
                    throws HistoryCalculationException {
        try {
            final Map<EntityCommodityFieldReference, PercentileRecord> result =
                            PercentilePersistenceTask.parse(timestamp, source,
                                            PercentileCounts::parseDelimitedFrom);
            logger.info("Loaded '{}' {} records for '{}' timestamp.", result.size(), partType,
                            timestamp);
            return Pair.create(timestamp, result);
        } catch (IOException ex) {
            throw new HistoryCalculationException(String.format("Cannot read bytes to initialize '%s' records in cache", partType),
                            ex);
        }
    }

    @Override
    protected void exportState(@Nonnull final OutputStream appender)
                    throws DiagnosticsException, IOException {
        try {
            /*
             * Order in the way to write those data is important. Data should be dumped in the
             * way they are read in PercentileEditor#loadPersistedData method, i.e. first full,
             * then latest.
             */
            final int fullSize = dumpPercentileCounts(appender,
                            UtilizationCountStore::getFullCountsRecord);
            final int latestSize = dumpPercentileCounts(appender,
                            UtilizationCountStore::getLatestCountsRecord);
            logger.info("Percentile cache stored in '{}' diagnostic file. Latest has '{}' record(s). Full has '{}' record(s).",
                            getFileName(), latestSize, fullSize);
        } catch (HistoryCalculationException | InterruptedException e) {
            throw new DiagnosticsException(
                            String.format("Cannot write percentile cache into '%s' file",
                                            getFileName()));
        }
    }

    private int dumpPercentileCounts(OutputStream output,
                    ThrowingFunction<UtilizationCountStore, Builder, HistoryCalculationException> dumpingFunction)
                    throws HistoryCalculationException, InterruptedException, IOException {
        final PercentileCounts counts = createBlob(dumpingFunction).build();
        counts.writeDelimitedTo(output);
        return counts.getPercentileRecordsCount();
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
