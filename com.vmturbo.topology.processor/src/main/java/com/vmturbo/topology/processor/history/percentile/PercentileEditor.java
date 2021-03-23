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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.stats.Stats.GetTimestampsRangeRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetTimestampsRangeResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.utils.ThrowingFunction;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO.Severity;
import com.vmturbo.platform.sdk.common.util.NotificationCategoryDTO;
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
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;

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
    // percentile may be calculated on a bought commodity if the provider has infinite capacity
    private static final Map<EntityType, Set<CommodityType>> ENABLED_BOUGHT_FROM_PROVIDER_TYPES =
        ImmutableMap.of(EntityType.COMPUTE_TIER,
            ImmutableSet.of(CommodityType.STORAGE_ACCESS, CommodityType.IO_THROUGHPUT));

    /**
     * Entity types for which percentile calculation is supported.
     * These entities trade commodities with the types listed in {@link
     * PercentileEditor#REQUIRED_SOLD_COMMODITY_TYPES} and
     * {@link PercentileEditor#ENABLED_BOUGHT_COMMODITY_TYPES}
     */
    private static final Set<EntityType> NOT_APPLICABLE_ENTITY_TYPES =
            ImmutableSet.of(EntityType.CONTAINER,
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
    private static final String NOTIFICATION_EVENT = "Percentile";
    private static final String MAINTENANCE_FAILED_NOTIFICATION_MESSAGE_FORMAT =
            "Percentile sliding observation window maintenance failed for %s checkpoint, last checkpoint was at %s";
    private static final String FAILED_READ_PERCENTILE_TRANSACTION_MESSAGE =
            "Failed to read the percentile transaction log entry for timestamp ";

    private final Clock clock;
    private final StatsHistoryServiceBlockingStub statsHistoryBlockingClient;

    private boolean historyInitialized;
    // moment of most recent checkpoint i.e. save of full window in the persistent store
    private long maintenanceLastCheckpointMs;
    // number of checkpoints happened so far for logging purposes
    private long checkpoints;
    /*
     * Flag set to true in case we need to enforce maintenance. Currently there is only one use
     * case for this - when percentile observation window changed. In case there are no changes
     * in percentile observation window than it is setting to false.
     */
    private boolean enforceMaintenance;
    private final SystemNotificationProducer systemNotificationProducer;
    /**
     * Pre-calculated in non-plan context during initialization to reuse in multiple stages.
     */
    private Map<Long, Integer> entity2period;

    /**
     * Construct the instance of percentile editor.
     *
     * @param config configuration values
     * @param statsHistoryClient persistence component access handler
     * @param statsHistoryBlockingClient persistence component blocking access handler
     * @param clock the {@link Clock}
     * @param historyLoadingTaskCreator creator of task to load or save data
     */
    public PercentileEditor(@Nonnull PercentileHistoricalEditorConfig config,
                    @Nonnull StatsHistoryServiceStub statsHistoryClient,
                    @Nonnull StatsHistoryServiceBlockingStub statsHistoryBlockingClient,
                    @Nonnull Clock clock,
                    @Nonnull BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, PercentilePersistenceTask> historyLoadingTaskCreator, @Nonnull SystemNotificationProducer systemNotificationProducer) {
        super(config, statsHistoryClient, historyLoadingTaskCreator, PercentileCommodityData::new);
        this.clock = clock;
        this.statsHistoryBlockingClient = statsHistoryBlockingClient;
        this.systemNotificationProducer = systemNotificationProducer;
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
        if (NOT_APPLICABLE_ENTITY_TYPES.contains(EntityType.forNumber(entity.getEntityType()))) {
            return false;
        }
        return super.isEntityApplicable(entity);
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        return commSold.hasUtilizationData() || REQUIRED_SOLD_COMMODITY_TYPES
                        .contains(CommodityType.forNumber(commSold.getCommodityType().getType()));
    }

    @Override
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
            @Nonnull TopologyDTO.CommodityBoughtDTO.Builder commBought,
            int providerType) {
        final CommodityType boughtType =
            CommodityType.forNumber(commBought.getCommodityType().getType());
        return commBought.hasUtilizationData() &&
            (ENABLED_BOUGHT_COMMODITY_TYPES.contains(boughtType) ||
            ENABLED_BOUGHT_FROM_PROVIDER_TYPES.getOrDefault(EntityType.forNumber(providerType),
                    Collections.emptySet())
                .contains(boughtType));
    }

    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    public synchronized void initContext(@Nonnull HistoryAggregationContext context,
                            @Nonnull List<EntityCommodityReference> eligibleComms)
                    throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);

        // will be required for initial loading, maintenance, observation period change check
        if (!context.isPlan() || !historyInitialized) {
            entity2period = getEntityToPeriod(context);
        }

        // read the latest and full window blobs if haven't yet, set into cache
        loadPersistedData(context, eligibleComms, checkpoint -> {
            // latest
            return Pair.create(null,
                    createTask(checkpoint).load(Collections.emptyList(), getConfig()));
        }, maintenance -> {
            // full
            final PercentilePersistenceTask fullTask = createTask(maintenance);
            final Map<EntityCommodityFieldReference, PercentileRecord> records = fullTask.load(
                    Collections.emptyList(), getConfig());
            return Pair.create(fullTask.getLastCheckpointMs(), records);
        });

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
    public synchronized void completeBroadcast(@Nonnull HistoryAggregationContext context)
                    throws HistoryCalculationException, InterruptedException {
        super.completeBroadcast(context);
        if (!context.isPlan() && historyInitialized) {
            final long potentialNewCheckpoint = clock.millis();

            // clean up the empty entries
            int entriesBefore = getCache().size();
            getCache().entrySet().removeIf(field2data -> field2data.getValue()
                    .getUtilizationCountStore()
                    .isEmptyOrOutdated(potentialNewCheckpoint));
            int entriesAfter = getCache().size();
            if (entriesAfter < entriesBefore && logger.isDebugEnabled()) {
                logger.debug("Cleared {} empty percentile records out of {}",
                                entriesBefore - entriesAfter, entriesBefore);
            }

            // TODO run all the subsequent memory-changing calls within one single CacheBackup

            // persist the daily blob
            persistDailyRecord();

            if (shouldReassemble()) {
                logger.info(
                        "Performing reassembly full page - maintenance will be skipped. Last checkpoint maintenance changed from {} to {}.",
                        Instant.ofEpochMilli(maintenanceLastCheckpointMs), Instant.ofEpochMilli(potentialNewCheckpoint));
                // perform scheduled reassembly of the full page if needed
                periodicReassembleFullPage(potentialNewCheckpoint);
            } else {
                // perform enforce maintenance if required
                if (enforceMaintenance && !shouldRegularMaintenanceHappen(potentialNewCheckpoint)) {
                    enforcedMaintenance(potentialNewCheckpoint);
                } else {
                    // perform daily maintenance if needed - synchronously within broadcast
                    maintenance(potentialNewCheckpoint);
                }
            }

            // print the utilization counts from cache for the configured OID in logs
            // if debug is enabled.
            debugLogDataValues(logger,
                            (data) -> String.format("Percentile utilization counts: %s",
                                            data.getUtilizationCountStore().toDebugString()));
        }
    }

    private boolean shouldReassemble() {
        final PercentileHistoricalEditorConfig config = getConfig();
        final long reassemblyLastCheckpointInMs = config.getFullPageReassemblyLastCheckpointInMs();
        final int fullPageReassemblyPeriodInDays = config.getFullPageReassemblyPeriodInDays();
        if (!historyInitialized) {
            logger.warn(
                    "Cannot reassemble full page: percentile history is not initialized.");
            return false;
        }
        if (fullPageReassemblyPeriodInDays == 0) {
            logger.debug(
                    "Full page reassembly period is set to 0 - periodic reassembly will not be performed");
            return false;
        }
        if (reassemblyLastCheckpointInMs == 0 && maintenanceLastCheckpointMs > 0) {
            config.setFullPageReassemblyLastCheckpoint(maintenanceLastCheckpointMs);
            return false;
        }
        if (clock.millis() - reassemblyLastCheckpointInMs < TimeUnit.DAYS.toMillis(
                fullPageReassemblyPeriodInDays)) {
            logger.trace(
                    "Full page reassembly execution skipped - not enough time passed since last checkpoint "
                            + reassemblyLastCheckpointInMs);
            return false;
        }
        return true;
    }

    private void periodicReassembleFullPage(long maintenanceCheckpoint)
            throws HistoryCalculationException, InterruptedException {
        reassembleFullPage(true, maintenanceCheckpoint);
        getConfig().setFullPageReassemblyLastCheckpoint(maintenanceCheckpoint);
    }

    /**
     * Re-compute the full page from the daily pages over the maximum defined observation period.
     * Update memory cache and if necessary persisting it.
     * Execute synchronously (will block the ongoing broadcast, if happens at the same time).
     *
     * @param persist whether to persist the full blob into DB after reassembly.
     * @param checkpointMs moment of time to store as checkpoint
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    public synchronized void reassembleFullPage(boolean persist, long checkpointMs)
            throws InterruptedException, HistoryCalculationException {
        int maxPeriod = getCache().values().stream()
                        .map(PercentileCommodityData::getUtilizationCountStore)
                        .map(UtilizationCountStore::getPeriodDays).max(Long::compare)
                        .orElse(PercentileHistoricalEditorConfig
                                        .getDefaultObservationPeriod());
        reassembleFullPageInMemory(getCache(), maxPeriod);
        if (enforceMaintenance = persist) {
            enforcedMaintenance(checkpointMs);
        }
    }

    private HistoryAggregationContext createEmptyContext() {
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(
                getConfig().getRealtimeTopologyContextId()).build();
        final Map<Integer, Collection<TopologyEntity>> typeToIndex = new HashMap<>();
        final Long2ObjectMap<TopologyEntity> oidToEntity = new Long2ObjectOpenHashMap<>();
        final TopologyGraph<TopologyEntity> graph = new TopologyGraph<>(oidToEntity, typeToIndex);
        final Map<Long, EntitySettings> oidToSettings = new HashMap<>();
        final Map<Long, SettingPolicy> defaultPolicies = new HashMap<>();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph, oidToSettings,
                defaultPolicies);
        return new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
    }

    private void persistDailyRecord() throws InterruptedException, HistoryCalculationException {
        // When we are in checkpoint time, we persist the latest in last successfully written day record.
        // For reason: this ensures the sum of all records in the observation period is the same as full record.
        persistBlob(maintenanceLastCheckpointMs, getMaintenanceWindowInMs(),
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
            PercentileRecord.Builder record = countStoreToRecordStore.apply(data.getUtilizationCountStore());
            if (record != null) {
                builder.addPercentileRecords(record);
            }
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
            // assume today's midnight
            long now = clock.millis();
            maintenanceLastCheckpointMs = getDefaultMaintenanceCheckpointTimeMs(now);
            Stopwatch sw = Stopwatch.createStarted();
            // read the latest and full window blobs if haven't yet, set into cache
            try {
                final Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>> full =
                        fullLoader.apply(PercentilePersistenceTask.TOTAL_TIMESTAMP);
                final Map<EntityCommodityFieldReference, PercentileRecord> fullPage =
                        full.getSecond();
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
                if (full.getFirst() > 0) {
                    // if full is present in the db, it's 'end' is the actual checkpoint moment
                    maintenanceLastCheckpointMs = full.getFirst();
                }
                logger.info("Initialized percentile full window data for {} commodities in {}",
                             fullPage::size, sw::toString);
            } catch (InvalidHistoryDataException e) {
                logger.warn("Failed to read percentile full window data, re-assembling from the daily blobs", e);
                initializeCacheValues(context, eligibleComms);
                reassembleFullPage(true, now);
            }
            sw.reset();
            sw.start();
            try {
                final Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>> loaded =
                                latestLoader.apply(maintenanceLastCheckpointMs);
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
                        maintenanceLastCheckpointMs, latestPage.size(), sw);
            } catch (InvalidHistoryDataException e) {
                logger.warn("Failed to load percentile latest window data, proceeding with empty", e);
            }
            historyInitialized = true;
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
                // TODO these changes to cache are made outside of 'cache backup'
                // they don't get reverted if reassembly fails
                // instead of modifying here they should be collected and passed to reassemble call below
                store.setPeriodDays(observationPeriod);
            }
        }

        if (changedPeriodEntries.isEmpty()) {
            logger.debug("Observation periods for cache entries have not changed.");
            enforceMaintenance = false;
        } else {
            reassembleFullPageInMemory(changedPeriodEntries, maxOfChangedPeriods);
            enforceMaintenance = true;
        }
    }

    private void reassembleFullPageInMemory(
            Map<EntityCommodityFieldReference, PercentileCommodityData> entriesToUpdate,
            int maxOfPeriods) throws HistoryCalculationException, InterruptedException {
        logger.debug("Reassembling full page for {} entries from up to {} days",
                        entriesToUpdate.size(), maxOfPeriods);
        try (DataMetricTimer timer = SETTINGS_CHANGE_SUMMARY_METRIC.startTimer();
             CacheBackup<PercentileCommodityData> backup = createCacheBackup()) {
            final Stopwatch sw = Stopwatch.createStarted();

            for (PercentileCommodityData data : entriesToUpdate.values()) {
                data.getUtilizationCountStore().clearFullRecord();
            }

            // Read as many page blobs from persistence as constitute the max of new periods
            // and accumulate them into percentile cache, respect per-entity observation window settings

            final long startTimestamp = shiftByObservationPeriod(maintenanceLastCheckpointMs, maxOfPeriods);
            logger.debug("Reassembling daily blobs in range [{} - {})",
                            Instant.ofEpochMilli(startTimestamp),
                            Instant.ofEpochMilli(maintenanceLastCheckpointMs));
            // Load snapshots by selecting from history
            List<Long> timestamps = getTimestampsInRange(startTimestamp, maintenanceLastCheckpointMs);
            for (long timestamp : timestamps) {
                final Map<EntityCommodityFieldReference, PercentileRecord> page;
                try {
                    page = createTask(timestamp).load(Collections.emptyList(), getConfig());
                } catch (InvalidHistoryDataException e) {
                    sendNotification(
                            FAILED_READ_PERCENTILE_TRANSACTION_MESSAGE + startTimestamp
                                    + " when performing full page reassembly", Severity.MAJOR);
                    logger.warn("Failed to read percentile daily blob for {}, skipping it for full page reassembly",
                                    timestamp, e);
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
                    final long timestampBound = shiftByObservationPeriod(
                                    maintenanceLastCheckpointMs, store.getPeriodDays());
                    if (timestampBound <= timestamp) {
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
            }

            // at this point full record is a sum of all previous days up to period
            // except the latest page -> add latest to full, rescaling as necessary
            for (PercentileCommodityData entry : entriesToUpdate.values()) {
                PercentileRecord.Builder record = entry.getUtilizationCountStore().getLatestCountsRecord();
                if (record != null) {
                    entry.getUtilizationCountStore().setLatestCountsRecord(record.build());
                }
            }

            backup.keepCacheOnClose();
            logger.info("Reassembled full page for {} entries from {} pages in {}",
                    entriesToUpdate.size(), maxOfPeriods, sw);
        }
    }

    private void maintenance(long newCheckpointMs) throws InterruptedException {
        if (!historyInitialized) {
            logger.warn("Not performing maintenance: percentile history is not initialized.");
            return;
        }

        if (!shouldRegularMaintenanceHappen(newCheckpointMs)) {
            logger.trace("Percentile cache checkpoint skipped - not enough time passed since last checkpoint "
                         + maintenanceLastCheckpointMs);
            return;
        }

        final Stopwatch sw = Stopwatch.createStarted();
        try (DataMetricTimer timer = MAINTENANCE_SUMMARY_METRIC.startTimer();
                        CacheBackup<PercentileCommodityData> backup = createCacheBackup()) {
            logger.debug("Performing percentile cache maintenance {} for {}",
                        ++checkpoints,
                        Instant.ofEpochMilli(newCheckpointMs));

            final Set<Integer> periods = new HashSet<>(entity2period.values());

            /*
             Process outdated percentiles day by day:

             We take all possible periods of observation (7 days, 30 days and 90 days)
             For each period:
                 outdatedPercentiles = load outdated percentiles in a range [old checkpoint - period, new checkpoint - period)
                    inclusive to exclusive
                 for each entity in cache:
                     if outdatedPercentiles contain entity:
                         subtract outdated utilization counts from cache
             Save current cache to db.
             Clear and save latest cache to db.
            */
            Map<PercentileCommodityData, List<PercentileRecord>> dataRef2outdatedRecords = new HashMap<>();
            // initialize with empty collections
            getCache().values().forEach(dataRef -> dataRef2outdatedRecords.put(dataRef, new LinkedList<>()));

            for (Integer periodInDays : periods) {
                final long outdatedStart = shiftByObservationPeriod(maintenanceLastCheckpointMs, periodInDays);
                final long outdatedEnd = shiftByObservationPeriod(newCheckpointMs, periodInDays);
                logger.info("Loading outdated blobs for period {} in range [{} - {})",
                                periodInDays,
                                Instant.ofEpochMilli(outdatedStart),
                                Instant.ofEpochMilli(outdatedEnd));
                // Load snapshots by selecting from history
                List<Long> timestamps = getTimestampsInRange(outdatedStart, outdatedEnd);
                for (long outdatedTimestamp : timestamps) {
                    final PercentilePersistenceTask loadOutdated = createTask(outdatedTimestamp);
                    logger.debug("Started checkpoint percentile cache for timestamp {} with period of {} days",
                                    outdatedTimestamp, periodInDays);

                    final Map<EntityCommodityFieldReference, PercentileRecord> oldValues;
                    try {
                        oldValues = loadOutdated.load(Collections.emptyList(), getConfig());
                    } catch (InvalidHistoryDataException e) {
                        sendNotification(
                                FAILED_READ_PERCENTILE_TRANSACTION_MESSAGE
                                        + outdatedTimestamp + " when performing maintenance", Severity.MAJOR);
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
                if (checkpoint != null) {
                    total.addPercentileRecords(checkpoint);
                }
            }
            writeBlob(total, newCheckpointMs, PercentilePersistenceTask.TOTAL_TIMESTAMP);

            // store the cleared latest
            persistBlob(newCheckpointMs, getMaintenanceWindowInMs(),
                    UtilizationCountStore::getLatestCountsRecord);

            backup.keepCacheOnClose();
            maintenanceLastCheckpointMs = newCheckpointMs;

        } catch (HistoryCalculationException e) {
            sendNotification(String.format(MAINTENANCE_FAILED_NOTIFICATION_MESSAGE_FORMAT,
                    Instant.ofEpochMilli(newCheckpointMs),
                    Instant.ofEpochMilli(maintenanceLastCheckpointMs)), Severity.CRITICAL);
            logger.error("{} maintenance failed for '{}' checkpoint, last checkpoint was at '{}'",
                    enforceMaintenance ? "Enforced" : "Regular", Instant.ofEpochMilli(newCheckpointMs),
                    Instant.ofEpochMilli(maintenanceLastCheckpointMs), e);
        } finally {
            logger.info("Percentile cache {}maintenance {} took {}",
                            enforceMaintenance ? "enforced " : "", checkpoints, sw);
        }
    }

 private void sendNotification(@Nonnull String description, @Nonnull Severity severity) {
        systemNotificationProducer.sendSystemNotification(
                Collections.singletonList(NotificationDTO.newBuilder()
                        .setEvent(NOTIFICATION_EVENT)
                        .setSeverity(severity)
                        .setCategory(NotificationCategoryDTO.NOTIFICATION.name())
                        .setDescription(description)
                        .build()), null);
    }

    private void enforcedMaintenance(long checkpointMs) throws InterruptedException {
        if (!historyInitialized) {
            logger.warn("Not performing enforced maintenance: percentile history is not initialized.");
            return;
        }

        final Stopwatch sw = Stopwatch.createStarted();
        try (DataMetricTimer timer = MAINTENANCE_SUMMARY_METRIC.startTimer();
             CacheBackup<PercentileCommodityData> backup = createCacheBackup()) {

            logger.debug("Performing enforced percentile cache maintenance for {}",
                         () -> Instant.ofEpochMilli(checkpointMs));
            // checkpoint() also clears latest
            persistBlob(PercentilePersistenceTask.TOTAL_TIMESTAMP, maintenanceLastCheckpointMs,
                        store -> store.checkpoint(Collections.emptyList(), true));

            // TODO currently 'enforced maintenance' does not subtract outdated blobs
            // in a range [old checkpoint - period, new checkpoint - period)
            // that contradicts the 'reassemble' behavior that implicitly drops values from outdated blobs
            // 'enforced' should be merged with 'maintenance' to repeat that behavior

            // store the cleared latest
            persistBlob(checkpointMs, getMaintenanceWindowInMs(),
                    UtilizationCountStore::getLatestCountsRecord);

            backup.keepCacheOnClose();
            maintenanceLastCheckpointMs = checkpointMs;

        } catch (HistoryCalculationException e) {
            sendNotification(String.format(MAINTENANCE_FAILED_NOTIFICATION_MESSAGE_FORMAT,
                    Instant.ofEpochMilli(checkpointMs),
                    Instant.ofEpochMilli(maintenanceLastCheckpointMs)), Severity.CRITICAL);
            logger.error("{} maintenance failed for '{}' checkpoint, last checkpoint was at '{}'",
                    enforceMaintenance ? "Enforced" : "Regular", Instant.ofEpochMilli(checkpointMs),
                    Instant.ofEpochMilli(maintenanceLastCheckpointMs), e);
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
    protected CacheBackup<PercentileCommodityData> createCacheBackup() throws HistoryCalculationException {
        return new CacheBackup<>(getCache(), PercentileCommodityData::new);
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

    /**
     * Rounded down to sliding window granularity assumed time of checkpoint (default - this midnight).
     *
     * @param now current moment
     * @return default checkpoint time in ms since epoch
     */
    private long getDefaultMaintenanceCheckpointTimeMs(long now) {
        long window = getMaintenanceWindowInMs();
        return (long)(Math.floor(now / window) * window);
    }

    private static long shiftByObservationPeriod(long checkpoint, int periodInDays) {
        return checkpoint - TimeUnit.DAYS.toMillis(periodInDays);
    }

    private long getMaintenanceWindowInMs() {
        return TimeUnit.HOURS.toMillis(getConfig().getMaintenanceWindowHours());
    }

    private boolean shouldRegularMaintenanceHappen(long now) {
        // still try to run it around midnights but not if checkpoint happened just recently
        // e.g. if checkpoint for whatever reason happened at 23:30, do not run another this midnight
        return maintenanceLastCheckpointMs <= getDefaultMaintenanceCheckpointTimeMs(now)
                        - TimeUnit.HOURS.toMillis(1L);
    }

   protected SystemNotificationProducer getSystemNotificationProducer() {
        return systemNotificationProducer;
    }

    @Override
    protected void restoreState(@Nonnull final byte[] bytes) throws DiagnosticsException {
        final HistoryAggregationContext context = createEmptyContext();
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

    private List<Long> getTimestampsInRange(long startMs, long endMs)
                    throws HistoryCalculationException {
        try {
            GetTimestampsRangeResponse response = statsHistoryBlockingClient
                            .getPercentileTimestamps(GetTimestampsRangeRequest.newBuilder()
                                            .setStartTimestamp(startMs).setEndTimestamp(endMs).build());
            logger.info("Blobs in range [{} - {}): {}",
                            Instant.ofEpochMilli(startMs),
                            Instant.ofEpochMilli(endMs),
                            response.getTimestampList());
            return response.getTimestampList();
        } catch (StatusRuntimeException e) {
            throw new HistoryCalculationException("Failed to query daily blobs in range", e);
        }
    }
}
