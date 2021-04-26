package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
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
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
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
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
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
    // map of commodity type that should have percentile to entity types that sell (add unknown for any)
    private static final Map<CommodityType, Set<EntityType>> REQUIRED_SOLD_COMMODITY_TYPES =
                    ImmutableMap.of(CommodityType.VCPU, Collections.singleton(EntityType.UNKNOWN),
                                    CommodityType.VMEM, Collections.singleton(EntityType.UNKNOWN),
                                    CommodityType.STORAGE_ACCESS, Collections.singleton(EntityType.VIRTUAL_VOLUME));
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
    private static final String FULL_DIAG_NAME_SUFFIX = "full";
    private static final String LATEST_DIAG_NAME_SUFFIX = "latest";

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
    private static final String MAINTENANCE = "Maintenance";
    private static final String NOTIFICATION_EVENT = "Percentile";
    private static final String FAILED_READ_PERCENTILE_TRANSACTION_MESSAGE =
                    "Failed to read the percentile transaction log entry for timestamp '%s' when performing %s";
    private static final String FAILED_TO_READ_FULL_BLOB_MESSAGE =
                    "Failed to read percentile full window data, re-assembling from the daily blobs";
    private static final String FAILED_TO_LOAD_LATEST_BLOB_MESSAGE =
                    "Failed to load percentile latest window data, proceeding with empty";

    private final Clock clock;
    private final StatsHistoryServiceBlockingStub statsHistoryBlockingClient;
    private final SystemNotificationProducer systemNotificationProducer;
    private final IdentityProvider identityProvider;
    private final boolean enableExpiredOidFiltering;

    private boolean historyInitialized;
    // moment of most recent checkpoint i.e. save of full window in the persistent store
    private long lastCheckpointMs;
    // number of checkpoints happened so far for logging purposes
    private long checkpoints;
    /**
     * Pre-calculated in non-plan context during initialization to reuse in multiple stages.
     */
    private Map<Long, Integer> entity2period;
    /**
     * Contains entities that are going to be/have been reassembled. By default it should be empty.
     * Every broadcast we are clearing it. TTL of this variable is one broadcast.
     * It is getting populated in the following use-cases:
     *  - During TP start in case full blob failed to load we are placing all entities from the
     *  current broadcast into this collection.
     *  - During regular TP broadcast we are placing entities which max observation period has
     *  changed since last broadcast. Ignored in case it is already populated.
     *  - In case the time has come to do scheduled reassemble all entities from the cache are
     *  going to be placed into this map.
     */
    private Map<EntityCommodityFieldReference, PercentileCommodityData> entitiesToReassemble =
                    new HashMap<>();

    /**
     * Construct the instance of percentile editor.
     *
     * @param config configuration values
     * @param statsHistoryClient persistence component access handler
     * @param statsHistoryBlockingClient persistence component blocking access handler
     * @param clock the {@link Clock}
     * @param historyLoadingTaskCreator creator of task to load or save data
     * @param systemNotificationProducer system notification producer
     * @param identityProvider The identity provider used to get existing oids
     * @param enableExpiredOidFiltering whether to apply filtering to expired oids or not
     */
    public PercentileEditor(@Nonnull PercentileHistoricalEditorConfig config,
                            @Nonnull StatsHistoryServiceStub statsHistoryClient,
                            @Nonnull StatsHistoryServiceBlockingStub statsHistoryBlockingClient,
                            @Nonnull Clock clock,
                            @Nonnull BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, PercentilePersistenceTask> historyLoadingTaskCreator,
                            @Nonnull SystemNotificationProducer systemNotificationProducer,
                            @Nonnull IdentityProvider identityProvider, boolean enableExpiredOidFiltering) {
        super(config, statsHistoryClient, historyLoadingTaskCreator, PercentileCommodityData::new);
        this.clock = clock;
        this.statsHistoryBlockingClient = statsHistoryBlockingClient;
        this.systemNotificationProducer = systemNotificationProducer;
        this.identityProvider = identityProvider;
        this.enableExpiredOidFiltering = enableExpiredOidFiltering;
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
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                         @Nonnull TopologyDTO.CommoditySoldDTO.Builder commSold) {
        if (commSold.hasUtilizationData()) {
            return true;
        }
        // sold commodities from cloud environments that need percentile calculation have
        // utilizationData set and don't rely on REQUIRED_SOLD_COMMODITY_TYPES map
        if (entity.getEnvironmentType() == EnvironmentType.CLOUD) {
            return false;
        }
        Set<EntityType> allowedTypes = REQUIRED_SOLD_COMMODITY_TYPES
                        .get(CommodityType.forNumber(commSold.getCommodityType().getType()));
        if (allowedTypes == null) {
            return false;
        }
        return allowedTypes.contains(EntityType.UNKNOWN)
                        || allowedTypes.contains(EntityType.forNumber(entity.getEntityType()));
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
        final boolean isRealtime = !context.isPlan();
        if (isRealtime) {
            entitiesToReassemble.clear();
        }
        initializeCacheValues(context, eligibleComms);
        // will be required for initial loading, maintenance, observation period change check
        if (isRealtime || !historyInitialized) {
            entity2period = getEntityToPeriod(context);
        }

        try (CacheBackup<PercentileCommodityData> backup = createCacheBackup()) {

            LongSet currentOidsInIdentityCache = getCurrentOidsInInIdentityCache(identityProvider);
            // read the latest and full window blobs if haven't yet, set into cache
            loadPersistedData(context, latestTimestamp -> {
                // latest
                return Pair.create(null, createTask(latestTimestamp)
                                .load(Collections.emptyList(), getConfig(), currentOidsInIdentityCache));
            }, fullTimestamp -> {
                // full
                final PercentilePersistenceTask fullTask = createTask(fullTimestamp);
                final Map<EntityCommodityFieldReference, PercentileRecord> records =
                                fullTask.load(Collections.emptyList(), getConfig(), currentOidsInIdentityCache);
                return Pair.create(fullTask.getLastCheckpointMs(), records);
            });

            /*
             There is no sense to check observation period changes in case we've already reassembled
             data in memory, which might happen only in case full blob loading failed. Reassembling
             making sure that max observation period is correct, so no reason to check it again.
             */
            if (isRealtime && entitiesToReassemble.isEmpty()) {
                checkObservationPeriodsChanged(context);
            }
            backup.keepCacheOnClose();
        }
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityRefs) {
        return Collections.emptyList();
    }

    @Override
    public void cleanupCache(@Nonnull final List<EntityCommodityReference> commodities) {
        // We don't need to clean up cache for percentile
    }

    private LongSet getCurrentOidsInInIdentityCache(@Nonnull final IdentityProvider identityProvider) throws HistoryCalculationException {
        try {
            return identityProvider.getCurrentOidsInIdentityCache();
        } catch (IdentityUninitializedException e) {
            throw new HistoryCalculationException("Could not get existing oids from the " +
                "identity cache", e);
        }
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

            /*
              Latest should always be stored with the timestamp of the last full blob successful
              persistence. In case there is no such timestamp, i.e. TP just started, then we are
              taking current midnight.
              For reason: this ensures the sum of all records in the observation period is the same
              as full record.
             */
            persistBlob(lastCheckpointMs, getMaintenanceWindowInMs(),
                            UtilizationCountStore::getLatestCountsRecord);
            final boolean maintenanceRequired = shouldRegularMaintenanceHappen(potentialNewCheckpoint);
            if (!maintenanceRequired) {
                logger.trace("Percentile cache checkpoint skipped - not enough time passed since last checkpoint "
                                + lastCheckpointMs);
            }
            final boolean reassembleRequired = shouldReassemble(potentialNewCheckpoint);
            checkpoint(potentialNewCheckpoint, maintenanceRequired, reassembleRequired,
                            reassembleRequired);
            // print the utilization counts from cache for the configured OID in logs
            // if debug is enabled.
            debugLogDataValues(logger,
                            (data) -> String.format("Percentile utilization counts: %s",
                                            data.getUtilizationCountStore().toDebugString()));
        }
    }

    /**
     * Persists full blob if required. Creates new full blob instance which is ready to be recorded
     * to DB in case it is required:
     * <ul>
     *     <li>regular maintenance;</li>
     *     <li>observation period changed;</li>
     *     <li>scheduled reassemble.</li>
     * </ul>
     * In case full blob was created it will be send to history component, so it will be written.
     * Right after full writing it is required to persist updated latest blob. In all cases when we
     * are going to write full blob we are clearing latest and its clear state should be persisted.
     *
     * @param potentialNewCheckpoint timestamp which might be used to persist full
     *                 blob. It will be also used as a start timestamp for cleared latest.
     * @param maintenanceRequired {@code true} in case it is time to do
     *                 maintenance.
     * @param reassembleRequired {@code true} in case it is time to do reassemble or
     *                 in case reassemble called through API call.
     * @param updateReassemblyCheckpoint {@code true} in case scheduled reassembly
     *                 happened.
     * @throws InterruptedException in case operation has been interrupted.
     */
    private void checkpoint(long potentialNewCheckpoint, boolean maintenanceRequired,
                    boolean reassembleRequired, boolean updateReassemblyCheckpoint)
                    throws InterruptedException {
        try (CacheBackup<PercentileCommodityData> backup = createCacheBackup()) {
            if (enableExpiredOidFiltering) {
                expireStaleOidsFromCache(identityProvider);
            }
            final PercentileCounts.Builder full =
                            createFullForPersistence(potentialNewCheckpoint, reassembleRequired,
                                            maintenanceRequired);
            if (full != null) {
                writeBlob(full, potentialNewCheckpoint, PercentilePersistenceTask.TOTAL_TIMESTAMP);
                persistBlob(potentialNewCheckpoint, getMaintenanceWindowInMs(),
                                UtilizationCountStore::getLatestCountsRecord);
                logger.info("Last percentile checkpoint is going to be changed from {} to {}.",
                                Instant.ofEpochMilli(lastCheckpointMs),
                                Instant.ofEpochMilli(potentialNewCheckpoint));
                lastCheckpointMs = potentialNewCheckpoint;
            }
            if (updateReassemblyCheckpoint) {
                getConfig().setFullPageReassemblyLastCheckpoint(potentialNewCheckpoint);
            }
            backup.keepCacheOnClose();
        } catch (HistoryCalculationException ex) {
            final String operation = reassembleRequired ?
                            "Reassemble" :
                            maintenanceRequired ? MAINTENANCE : "Broadcast";
            final String message =
                            String.format("%s failed for '%s' checkpoint, last checkpoint was at '%s'",
                                            operation, Instant.ofEpochMilli(potentialNewCheckpoint),
                                            Instant.ofEpochMilli(lastCheckpointMs));
            sendNotification(message, Severity.CRITICAL);
            logger.error(message, ex);
        } finally {
            entitiesToReassemble.clear();
        }
    }

    @Nullable
    private PercentileCounts.Builder createFullForPersistence(long potentialNewCheckpoint,
                    boolean reassemble, boolean maintenance)
                    throws InterruptedException, HistoryCalculationException {
        /*
         Reassemble only in case the time has come and we did not reassemble full for
         all entities in initContext. It might be in two cases:
          - all entities changed theirs observation period;
          - we had problems in reading full blob, so it was reassembled.
         */
        if (reassemble && entitiesToReassemble.size() < getCache()
                        .size()) {
            entitiesToReassemble.putAll(getCache());
            final int maxObservationPeriod = getMaxObservationPeriod();
            logger.info("Performing reassembly full page for '{}' entities with '{}' max observation period at '{}' - maintenance will be skipped.",
                            entitiesToReassemble.size(), maxObservationPeriod,
                            potentialNewCheckpoint);
            reassembleFullPageInMemory(maxObservationPeriod);
        }
        /*
         There is no sense in tries to remove outdated records in case whole full blob
         has been reassembled.
         */
        if (maintenance && entitiesToReassemble.size() < getCache().size()) {
            return removeOutdated(potentialNewCheckpoint);
        }
        if (!entitiesToReassemble.isEmpty()) {
            return createBlob(store -> store.checkpoint(Collections.emptyList(), true));
        }
        return null;
    }

    /**
     * Remove all the oids in the cache that are not contained in the identity cache.
     * @param identityProvider that has access to the identity cache
     * @throws HistoryCalculationException is oids can't be read from the identity cache
     */
    private void expireStaleOidsFromCache(final IdentityProvider identityProvider) throws HistoryCalculationException {
        LongSet currentOidsInIdentityCache = getCurrentOidsInInIdentityCache(identityProvider);
        int originalCacheSize = getCache().keySet().size();
        getCache().keySet().removeIf(entityRef -> !currentOidsInIdentityCache.contains(entityRef.getEntityOid()));
        int sizeAfterExpiration = originalCacheSize - getCache().keySet().size();
        Level level = sizeAfterExpiration > 0 ? Level.INFO : Level.DEBUG;
        logger.log(level, "{} expired oids were filtered out during maintenance",
            sizeAfterExpiration);
    }

    private boolean shouldReassemble(long now) {
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
        if (reassemblyLastCheckpointInMs == 0 && lastCheckpointMs > 0) {
            config.setFullPageReassemblyLastCheckpoint(lastCheckpointMs);
            return false;
        }
        if (now - reassemblyLastCheckpointInMs < TimeUnit.DAYS
                        .toMillis(fullPageReassemblyPeriodInDays)) {
            logger.trace(
                    "Full page reassembly execution skipped - not enough time passed since last checkpoint "
                            + reassemblyLastCheckpointInMs);
            return false;
        }
        return true;
    }

    /**
     * Re-compute the full page from the daily pages over the maximum defined observation period.
     * Update memory cache and if necessary persisting it.
     * Execute synchronously (will block the ongoing broadcast, if happens at the same time).
     *
     * @param checkpointMs moment of time to store as checkpoint
     * @throws InterruptedException when interrupted
     */
    public synchronized void requestedReassembleFullPage(long checkpointMs)
            throws InterruptedException {
        checkpoint(checkpointMs, false, true, false);
    }

    private int getMaxObservationPeriod() {
        return getCache().values().stream().map(PercentileCommodityData::getUtilizationCountStore)
                        .map(UtilizationCountStore::getPeriodDays).max(Long::compare)
                        .orElse(PercentileHistoricalEditorConfig.getDefaultObservationPeriod());
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
                    @Nonnull ThrowingFunction<Long, Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>>, HistoryCalculationException> latestLoader,
                    @Nonnull ThrowingFunction<Long, Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>>, HistoryCalculationException> fullLoader)
                    throws HistoryCalculationException, InterruptedException {
        if (!historyInitialized) {
            // assume today's midnight
            long now = clock.millis();
            lastCheckpointMs = getDefaultMaintenanceCheckpointTimeMs(now);
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
                    lastCheckpointMs = full.getFirst();
                }
                logger.info("Initialized percentile full window data for {} commodities in {}",
                             fullPage::size, sw::toString);
            } catch (InvalidHistoryDataException e) {
                sendNotification(FAILED_TO_READ_FULL_BLOB_MESSAGE, Severity.MAJOR);
                logger.warn(FAILED_TO_READ_FULL_BLOB_MESSAGE, e);
                entitiesToReassemble.putAll(getCache());
                /*
                 Attempt to reassemble full page will create clear latest blob. It means that all
                 blobs created before current moment should now be included in full.
                 */
                lastCheckpointMs = now;
                /*
                  We should reassemble full page here because otherwise we will never know what
                  is the timestamp for latest.
                 */
                reassembleFullPageInMemory(getMaxObservationPeriod());
                historyInitialized = true;
                return;
            }
            sw.reset();
            sw.start();
            try {
                final Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>> loaded =
                                latestLoader.apply(lastCheckpointMs);
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
                                lastCheckpointMs, latestPage.size(), sw);
            } catch (InvalidHistoryDataException e) {
                sendNotification(FAILED_TO_LOAD_LATEST_BLOB_MESSAGE, Severity.MAJOR);
                logger.warn(FAILED_TO_LOAD_LATEST_BLOB_MESSAGE, e);
            }
            historyInitialized = true;
        }
    }

    private void checkObservationPeriodsChanged(@Nonnull HistoryAggregationContext context)
                    throws InterruptedException, HistoryCalculationException {
        final Map<EntityCommodityFieldReference, PercentileCommodityData> changedPeriodEntries =
                new HashMap<>();
        int maxOfChangedPeriods = 0;
        for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : getCache()
                        .entrySet()) {
            final EntityCommodityFieldReference ref = entry.getKey();
            final Integer observationPeriod = entity2period.get(ref.getEntityOid());
            if (observationPeriod != null) {
                final PercentileCommodityData data = entry.getValue();
                // We are only interested in "changes" but not in handling the new entities.
                if (data.needsReinitialization(ref, context, getConfig())) {
                    changedPeriodEntries.put(ref, data);
                    // Update percentile data with observation windows values.
                    // The needed pages count to load will be determined by the max observation period.
                    maxOfChangedPeriods = Math.max(maxOfChangedPeriods, observationPeriod);
                }
            }
        }

        if (changedPeriodEntries.isEmpty()) {
            logger.debug("Observation periods for cache entries have not changed.");
        } else {
            entitiesToReassemble.putAll(changedPeriodEntries);
            /*
             * It is required to do reassemble at least in memory, because without it new setting
             * for observation period will not take place, before we will do percentile
             * calculations.
             */
            reassembleFullPageInMemory(maxOfChangedPeriods);
        }
    }

    private synchronized long reassembleFullPageInMemory(int maxOfPeriods)
                    throws HistoryCalculationException, InterruptedException {
        logger.debug("Reassembling full page for {} entries from up to {} days",
                        entitiesToReassemble.size(), maxOfPeriods);
        try (DataMetricTimer timer = SETTINGS_CHANGE_SUMMARY_METRIC.startTimer()) {
            final Stopwatch sw = Stopwatch.createStarted();
            for (Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : entitiesToReassemble
                                                        .entrySet()) {
                final Integer observationPeriod = entity2period.get(entry.getKey().getEntityOid());
                final UtilizationCountStore utilizationCountStore =
                                entry.getValue().getUtilizationCountStore();
                if (observationPeriod != null) {
                    utilizationCountStore.setPeriodDays(observationPeriod);
                }
                utilizationCountStore.clearFullRecord();
            }

            // Read as many page blobs from persistence as constitute the max of new periods
            // and accumulate them into percentile cache, respect per-entity observation window settings

            final long startTimestamp = shiftByObservationPeriod(lastCheckpointMs, maxOfPeriods);
            logger.debug("Reassembling daily blobs in range [{} - {})",
                            Instant.ofEpochMilli(startTimestamp),
                            Instant.ofEpochMilli(lastCheckpointMs));
            // Load snapshots by selecting from history
            final List<Long> timestamps = getTimestampsInRange(startTimestamp, lastCheckpointMs,
                            "to reassemble full page");
            for (long timestamp : timestamps) {
                final Map<EntityCommodityFieldReference, PercentileRecord> page;
                try {
                    page = createTask(timestamp).load(Collections.emptyList(), getConfig(),
                        getCurrentOidsInInIdentityCache(identityProvider));
                } catch (InvalidHistoryDataException e) {
                    sendNotification(String.format(FAILED_READ_PERCENTILE_TRANSACTION_MESSAGE,
                                    startTimestamp, "full page reassembly"), Severity.MAJOR);
                    logger.warn("Failed to read percentile daily blob for {}, skipping it for full page reassembly",
                                    timestamp, e);
                    continue;
                }

                // For each entry in cache with a changed observation period,
                // apply loaded percentile commodity entries.
                for (Map.Entry<EntityCommodityFieldReference, PercentileCommodityData> entry : entitiesToReassemble
                        .entrySet()) {
                    final UtilizationCountStore store = entry.getValue().getUtilizationCountStore();
                    // Calculate bound timestamp for specific entry.
                    // This is necessary if the changed observation period
                    // for a given entry is less than the maximum modified observation period.
                    final long timestampBound = shiftByObservationPeriod(lastCheckpointMs, store.getPeriodDays());
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
            for (PercentileCommodityData entry : entitiesToReassemble.values()) {
                PercentileRecord.Builder record = entry.getUtilizationCountStore().getLatestCountsRecord();
                if (record != null) {
                    entry.getUtilizationCountStore().setLatestCountsRecord(record.build());
                }
            }
            logger.info("Reassembled full page for {} entries from {} pages in {}",
                            entitiesToReassemble.size(), maxOfPeriods, sw);
            return timestamps.stream().reduce((f, s) -> s).orElse(lastCheckpointMs);
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

    @Nullable
    private PercentileCounts.Builder removeOutdated(long newCheckpointMs)
                    throws InterruptedException, HistoryCalculationException {
        final Stopwatch sw = Stopwatch.createStarted();
        try (DataMetricTimer timer = MAINTENANCE_SUMMARY_METRIC.startTimer()) {
            /*
             Process outdated percentiles day by day:

             We take all possible periods of observation (7 days, 30 days and 90 days)
             For each period:
                 outdatedPercentiles = load outdated percentiles in a range [old checkpoint - period, new checkpoint - period)
                    inclusive to exclusive
                 for each entity in cache:
                     if outdatedPercentiles contain entity:
                         subtract outdated utilization counts from cache
             Clear latest.
            */
            final Map<PercentileCommodityData, List<PercentileRecord>> dataRef2outdatedRecords =
                            collectOutdatedRecords(newCheckpointMs);
            final PercentileCounts.Builder total = PercentileCounts.newBuilder();
            for (Map.Entry<PercentileCommodityData, List<PercentileRecord>> entry : dataRef2outdatedRecords
                            .entrySet()) {
                final PercentileRecord.Builder checkpoint =
                                entry.getKey().checkpoint(entry.getValue());
                if (checkpoint != null) {
                    total.addPercentileRecords(checkpoint);
                }
            }
            return total;
        } finally {
            logger.info("Outdated records removal {} took {} ms", checkpoints,
                            sw.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Nonnull
    private Map<PercentileCommodityData, List<PercentileRecord>> collectOutdatedRecords(
                    long newCheckpointMs) throws HistoryCalculationException, InterruptedException {
        final Map<PercentileCommodityData, List<PercentileRecord>> result = new HashMap<>();
        // initialize with empty collections
        getCache().values().forEach(dataRef -> result.put(dataRef, new LinkedList<>()));
        final Iterable<Integer> periods = new HashSet<>(entity2period.values());
        for (Integer periodInDays : periods) {
            final long outdatedStart =
                            shiftByObservationPeriod(lastCheckpointMs, periodInDays);
            final long outdatedEnd = shiftByObservationPeriod(newCheckpointMs, periodInDays);
            logger.debug("Loading outdated blobs for period {} in range [{} - {})", periodInDays,
                            Instant.ofEpochMilli(outdatedStart), Instant.ofEpochMilli(outdatedEnd));
            // Load snapshots by selecting from history
            final List<Long> timestamps = getTimestampsInRange(outdatedStart, outdatedEnd,
                            "to remove outdated records");
            for (long outdatedTimestamp : timestamps) {
                final PercentilePersistenceTask loadOutdated = createTask(outdatedTimestamp);
                logger.debug("Started checkpoint percentile cache for timestamp {} with period of {} days",
                                outdatedTimestamp, periodInDays);

                final Map<EntityCommodityFieldReference, PercentileRecord> oldValues;
                try {
                    oldValues = loadOutdated.load(Collections.emptyList(), getConfig(),
                        getCurrentOidsInInIdentityCache(identityProvider));
                } catch (InvalidHistoryDataException e) {
                    sendNotification(String.format(FAILED_READ_PERCENTILE_TRANSACTION_MESSAGE,
                                    outdatedTimestamp, "maintenance"), Severity.MAJOR);
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
                        result.get(fieldRef2data.getValue()).add(oldRecord);
                    }
                }
            }
        }
        return result;
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
        return lastCheckpointMs
                        <= getDefaultMaintenanceCheckpointTimeMs(now) - TimeUnit.HOURS.toMillis(1L);
    }

    @Override
    protected void restoreState(@Nonnull final byte[] bytes) throws DiagnosticsException {
        final Map<String, byte[]> diags = getDiagsMaping(bytes);
        final byte[] fullDiags = getSubDiags(diags, FULL_DIAG_NAME_SUFFIX);
        final byte[] latestDiags = getSubDiags(diags, LATEST_DIAG_NAME_SUFFIX);
        final HistoryAggregationContext context = createEmptyContext();
        try (InputStream latest = new ByteArrayInputStream(latestDiags);
             InputStream full = new ByteArrayInputStream(fullDiags)) {
            historyInitialized = false;
            getCache().clear();
            loadPersistedData(context,
                            (timestamp) -> loadCachePart(timestamp, latest, LATEST_DIAG_NAME_SUFFIX),
                            (timestamp) -> loadCachePart(timestamp, full, FULL_DIAG_NAME_SUFFIX));
        } catch (HistoryCalculationException | InterruptedException | IOException e) {
            getCache().clear();
            throw new DiagnosticsException(
                            String.format("Cannot load percentile cache from '%s' file",
                                            getFileName()), e);
        }
    }

    /**
     * Get mapping of percentile diags names to bits.
     *
     * Percentile are split into 2 files: full and latest diags, and stored
     * in its own zip with the following file structure:
     * ------ Percentile.Editor.state.binary
     * ------------- Percentile.Editor.state.full.binary
     * ------------- Percentile.Editor.state.latest.binary
     * @param compressedDiags Percentile diags zip
     * @return combined diags (full + latest) to be consumed by the loading logic
     */
    @Nonnull
    private static Map<String, byte[]> getDiagsMaping(@Nonnull final byte[] compressedDiags)  {
        final Map<String, byte[]> nameToBytes = new HashMap<>();
        final Iterable<Diags> diagsReader = new DiagsZipReader(
            new ByteArrayInputStream(compressedDiags), null, true);
        diagsReader.iterator().forEachRemaining(diags -> {
            nameToBytes.putIfAbsent(diags.getName(), diags.getBytes());
        });
        return nameToBytes;
    }

    /**
     * Get diags bits of the specified type.
     *
     * @param diagsMapping mapping of diags name to diags content
     * @param type type of subdiags requested: full or latest
     * @return requested subdiags content
     * @throws DiagnosticsException if requested subdiags are missing
     */
    @Nonnull
    private final byte[] getSubDiags(@Nonnull final Map<String, byte[]> diagsMapping,
                                     @Nonnull final String type)
        throws DiagnosticsException {
        final byte[] subDiags = diagsMapping.getOrDefault(getDiagsSubFileName(type), null);
        if (subDiags == null) {
            throw new DiagnosticsException(
                String.format("Cannot load percentile cache from '%s' file due to missing %s percentile part",
                    getFileName(), type));
        }
        return subDiags;
    }

    @Nonnull
    private static Pair<Long, Map<EntityCommodityFieldReference, PercentileRecord>> loadCachePart(
                    @Nonnull Long timestamp, @Nonnull InputStream source, @Nonnull String partType)
                    throws HistoryCalculationException {
        try {
            final Map<EntityCommodityFieldReference, PercentileRecord> result =
                            PercentilePersistenceTask.parse(timestamp, source,
                                            PercentileCounts::parseDelimitedFrom,
                                LongSets.EMPTY_SET, false);
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
            // Two diag files containing full and latest counts will be compressed in its own zip
            final ByteArrayOutputStream percentileDiagsOutput = new ByteArrayOutputStream();
            try (ZipOutputStream percentileZos = new ZipOutputStream(percentileDiagsOutput)) {
                final String fullZipEntryName = getDiagsSubFileName(FULL_DIAG_NAME_SUFFIX);
                final int fullSize = dumpDiagsToZipEntry(percentileZos, fullZipEntryName,
                    UtilizationCountStore::getFullCountsRecord);

                final String latestZipEntryName = getDiagsSubFileName(LATEST_DIAG_NAME_SUFFIX);
                final int latestSize = dumpDiagsToZipEntry(percentileZos, latestZipEntryName,
                    UtilizationCountStore::getLatestCountsRecord);

                logger.info("Full percentile cache stored in '{}' diagnostic file, latest percentile "
                        + "stored in '{}' diagnostic file. Latest has '{}' record(s). Full has '{}' record(s).",
                    () -> fullZipEntryName, () -> latestZipEntryName, () -> latestSize, () -> fullSize);
            }
            percentileDiagsOutput.writeTo(appender);
        } catch (HistoryCalculationException | InterruptedException e) {
            throw new DiagnosticsException(
                            String.format("Cannot write percentile cache into '%s' file",
                                            getFileName()));
        }
    }

    /**
     * Dump percentile diags to zip entry.
     *
     * @param percentileZos Percentile zip output stream to append
     * @param zipEntryName Zip entry name
     * @param dumpingFunction Function to generate counts
     * @return Number or records dumped
     * @throws IOException when failed to write zip entry
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    private int dumpDiagsToZipEntry(@Nonnull final ZipOutputStream percentileZos,
                                    @Nonnull final String zipEntryName,
                                    @Nonnull ThrowingFunction<UtilizationCountStore, Builder, HistoryCalculationException> dumpingFunction)
        throws IOException, InterruptedException, HistoryCalculationException {
        final ZipEntry ze = new ZipEntry(zipEntryName);
        ze.setTime(clock.millis());
        percentileZos.putNextEntry(ze);
        final int size = dumpPercentileCounts(percentileZos, dumpingFunction);
        percentileZos.closeEntry();
        return size;
    }

    @Nonnull
    private String getDiagsSubFileName(@Nonnull String type) {
        return getFileName() + "." + type + DiagsZipReader.BINARY_DIAGS_SUFFIX;
    }

    private int dumpPercentileCounts(OutputStream output,
                    ThrowingFunction<UtilizationCountStore, Builder, HistoryCalculationException> dumpingFunction)
                    throws HistoryCalculationException, InterruptedException, IOException {
        final PercentileCounts counts = createBlob(dumpingFunction).build();
        counts.writeDelimitedTo(output);
        return counts.getPercentileRecordsCount();
    }

    private List<Long> getTimestampsInRange(long startMs, long endMs, String operation)
                    throws HistoryCalculationException {
        try {
            final GetTimestampsRangeResponse response = statsHistoryBlockingClient
                            .getPercentileTimestamps(GetTimestampsRangeRequest.newBuilder()
                                            .setStartTimestamp(startMs).setEndTimestamp(endMs).build());
            logger.info("Blobs in range [{} - {}): {} loaded {}",
                            Instant.ofEpochMilli(startMs),
                            Instant.ofEpochMilli(endMs),
                            response.getTimestampList(),
                            operation);
            return response.getTimestampList();
        } catch (StatusRuntimeException e) {
            throw new HistoryCalculationException("Failed to query daily blobs in range", e);
        }
    }
}
