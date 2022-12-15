package com.vmturbo.topology.processor.history.movingstats;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import it.unimi.dsi.fastutil.longs.LongSets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.ContainerSpecInfoView;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.utils.ThrowingFunction;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CPUThrottlingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO.Severity;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractBlobsPersistenceTask;
import com.vmturbo.topology.processor.history.BlobPersistingCachingHistoricalEditor;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
import com.vmturbo.topology.processor.history.exceptions.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;

/**
 * Calculate and provide moving statistics such as moving average and standard deviation
 * we can use to set the historical value for topology commodities.
 * <p/>
 * Note that currently MovingStatistics are only supported for sold commodities and not bought
 * commodities.
 */
public class MovingStatisticsEditor extends BlobPersistingCachingHistoricalEditor<MovingStatisticsCommodityData,
    MovingStatisticsPersistenceTask,
    MovingStatisticsHistoricalEditorConfig,
    MovingStatisticsRecord,
    StatsHistoryServiceStub,
    MovingStatisticsRecord.Builder,
    MovingStatistics.Builder> {

    private static final Logger logger = LogManager.getLogger();
    private static final String FAILED_TO_READ_FULL_BLOB_MESSAGE = "Failed to read moving statistics data";
    private static final String NOTIFICATION_EVENT = "Moving statistics";

    /**
     * Whether we have finished initializing moving statistics from the persistent store.
     */
    private boolean historyInitialized = false;

    /**
     * Construct the instance of a caching history editor.
     *
     * @param config                     per-type configuration
     * @param statsHistoryClient         history db client
     * @param historyLoadingTaskCreator  create an instance of a db value loading task
     * @param clock                      the clock to use for timing
     * @param systemNotificationProducer system notification producer
     * @param identityProvider           the identity provider used to get existing oids
     */
    public MovingStatisticsEditor(@Nonnull MovingStatisticsHistoricalEditorConfig config,
                                  @Nonnull StatsHistoryServiceStub statsHistoryClient,
                                  @Nonnull BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, MovingStatisticsPersistenceTask> historyLoadingTaskCreator,
                                  @Nonnull final Clock clock,
                                  @Nonnull SystemNotificationProducer systemNotificationProducer,
                                  @Nonnull IdentityProvider identityProvider) {
        super(config, statsHistoryClient, historyLoadingTaskCreator,
                MovingStatisticsCommodityData::new, clock, systemNotificationProducer,
                identityProvider);
    }

    @Override
    public boolean isApplicable(@Nullable List<ScenarioChange> changes,
                                @Nonnull TopologyInfo topologyInfo,
                                @Nullable PlanScope scope) {
        // moving statistics should not be set for baseline and cluster headroom plans
        if (TopologyDTOUtil.isPlanType(PlanProjectType.CLUSTER_HEADROOM, topologyInfo)) {
            return false;
        }
        if (CollectionUtils.isEmpty(changes)) {
            return true;
        }
        return changes.stream()
            .filter(ScenarioChange::hasPlanChanges)
            .noneMatch(change -> change.getPlanChanges().hasHistoricalBaseline());
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        if (entity.getEntityType() != EntityType.CONTAINER_SPEC_VALUE) {
            return false;
        }
        if (!entity.getTypeSpecificInfo().hasContainerSpec()) {
            return false;
        }
        ContainerSpecInfoView containerSpecInfo = entity.getTypeSpecificInfo().getContainerSpec();
        if (!containerSpecInfo.hasCpuThrottlingType()
                || containerSpecInfo.getCpuThrottlingType() != CPUThrottlingType.timeBased) {
            return false;
        }
        return super.isEntityApplicable(entity);
    }

    @Override
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                         @Nonnull CommoditySoldImpl commSold,
                                         @Nullable TopologyInfo topoInfo) {
        return getConfig().getSamplingConfigurations(commSold.getCommodityType().getType()) != null;
    }

    @Override
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                         @Nonnull CommodityBoughtImpl commBought,
                                         int providerType) {
        // Do not apply to any bought commodities.
        return false;
    }

    @Override
    public boolean isMandatory() {
        return true;
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
        // We don't need to clean up cache for moving statistics
    }

    @Override
    @Nonnull
    protected Stream<CommodityField> commodityFieldsForCalculationTasks() {
        // We only need to compute moving statistics for used values, not peak.
        return Stream.of(CommodityField.USED);
    }

    @Override
    public synchronized void completeBroadcast(@Nonnull HistoryAggregationContext context)
        throws HistoryCalculationException, InterruptedException {
        super.completeBroadcast(context);
        if (!context.isPlan()) {
            final long currentTimeMs = getClock().millis();
            final MovingStatistics.Builder builder = createBlob(MovingStatisticsCommodityData::serialize);
            logger.debug("Writing {} moving statistics entries with timestamp {}",
                    builder.getStatisticRecordsCount(), currentTimeMs);
            createTask(currentTimeMs).save(builder.build(), 0, getConfig());
        }
    }

    protected synchronized void restorePersistedData(@Nonnull Map<String, InputStream> diagsMapping,
                                                     @Nonnull HistoryAggregationContext context)
        throws HistoryCalculationException, InterruptedException, IOException {
        // Read the blob into the cache
        try {
            final Map<EntityCommodityFieldReference, MovingStatisticsRecord> loadedRecords =
                MovingStatisticsPersistenceTask.parse(diagsMapping.get(getDiagsSubFileName()),
                    MovingStatistics::parseDelimitedFrom, getConfig(), LongSets.EMPTY_SET, false);
            getCache().clear();
            addRecordsToCache(context, loadedRecords);

            // Ensure we do not overwrite the restored data with loaded data on the next broadcast.
            historyInitialized = true;
        } catch (IOException ex) {
            throw new HistoryCalculationException("Failed to read MovingStatistics persisted data from bytes", ex);
        }
    }

    @Nonnull
    @Override
    protected List<Pair<String, ThrowingFunction<MovingStatisticsCommodityData, MovingStatisticsRecord.Builder, HistoryCalculationException>>>
    getStateExportingFunctions() {
        return Collections.singletonList(Pair.create(getDiagsSubFileName(), MovingStatisticsCommodityData::serialize));
    }

    @Nonnull
    private String getDiagsSubFileName() {
        return getFileName() + DiagsZipReader.BINARY_DIAGS_SUFFIX;
    }

    @Nonnull
    @Override
    protected MovingStatistics.Builder createBlob(
        @Nonnull ThrowingFunction<MovingStatisticsCommodityData, Builder, HistoryCalculationException> dumpingFunction)
        throws HistoryCalculationException, InterruptedException {
        final MovingStatistics.Builder builder = MovingStatistics.newBuilder();
        for (MovingStatisticsCommodityData data : getCache().values()) {
            MovingStatisticsRecord.Builder record = dumpingFunction.apply(data);
            if (record != null) {
                builder.addStatisticRecords(record);
            }
        }
        return builder;
    }

    @Override
    protected int getRecordCount(@Nonnull MovingStatistics.Builder builder) {
        return builder.getStatisticRecordsCount();
    }

    @Override
    public synchronized void initContext(@Nonnull HistoryAggregationContext context,
            @Nonnull List<EntityCommodityReference> eligibleComms)
            throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);
        loadPersistedData(context);
    }

    private synchronized void loadPersistedData(@Nonnull HistoryAggregationContext context)
            throws HistoryCalculationException, InterruptedException {
        if (!historyInitialized) {
            final Stopwatch sw = Stopwatch.createStarted();
            final Set<Long> currentOidsInIdentityCache = getCurrentOidsInInIdentityCache();

            try {
                final MovingStatisticsPersistenceTask loadTask = createTask(AbstractBlobsPersistenceTask.TOTAL_TIMESTAMP);
                final Map<EntityCommodityFieldReference, MovingStatisticsRecord> records =
                    loadTask.load(Collections.emptyList(), getConfig(), currentOidsInIdentityCache);
                addRecordsToCache(context, records);
                logger.info("Initialized moving statistics data for {} commodities in {}",
                    records::size, sw::toString);
            } catch (InvalidHistoryDataException e) {
                sendNotification(NOTIFICATION_EVENT, FAILED_TO_READ_FULL_BLOB_MESSAGE, Severity.MAJOR);
                logger.warn(FAILED_TO_READ_FULL_BLOB_MESSAGE, e);
            }

            historyInitialized = true;
        }
    }

    private void addRecordsToCache(@Nonnull HistoryAggregationContext context,
                                   @Nonnull Map<EntityCommodityFieldReference, MovingStatisticsRecord> records)
            throws InvalidHistoryDataException {
        for (Map.Entry<EntityCommodityFieldReference, MovingStatisticsRecord> entry : records.entrySet()) {
            final EntityCommodityFieldReference field = entry.getKey();
            MovingStatisticsCommodityData data =
                    getCache().computeIfAbsent(field, ref -> historyDataCreator.get());
            if (data.needsReinitialization(field, context, getConfig())) {
                data.init(field, null, getConfig(), context);
            }
            data.deserialize(entry.getValue());
        }
    }
}