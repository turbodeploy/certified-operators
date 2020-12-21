package com.vmturbo.topology.processor.history.timeslot;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.time.Duration;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractBackgroundLoadingHistoricalEditor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;

/**
 * Calculate and provide time slot historical values for topology commodities.
 */
public class TimeSlotEditor extends
                AbstractBackgroundLoadingHistoricalEditor<TimeSlotCommodityData,
                                TimeSlotLoadingTask,
                                TimeslotHistoricalEditorConfig,
                                List<Pair<Long, StatRecord>>,
                                StatsHistoryServiceBlockingStub,
                                Void> {

    private static final Set<CommodityType> ENABLED_TIMESLOT_COMMODITY_TYPES = Sets.immutableEnumSet(
                        CommodityDTO.CommodityType.POOL_CPU,
                        CommodityDTO.CommodityType.POOL_MEM,
                        CommodityDTO.CommodityType.POOL_STORAGE);

    /**
     * Stores information about time when we first time started TP. It is required to re-enable back
     * TimeSlot analysis in the following scenario:
     * <ol>
     *     <li> TP started, but background loading process failed and whole TimeSlot analysis
     *     was disabled.
     *     <li> In case TP successfully was working and collecting data for more than observation
     *     period time required for TimeSlot analysis we want to enable TimeSlot analysis back.
     *     Moreover since that moment of time we are clearing background loading statistic and
     *     background loading is ready to process big amount of data in background mode as usually.
     * </ol>
     */
    private long startTimestamp;

    /**
     * Construct the timeslot historical values editor.
     *
     * @param config configuration settings
     * @param statsHistoryClient history component stub
     * @param loadingPool loads data for time slot analysis in background.
     * @param taskCreator create an instance of a db value loading task
     */
    public TimeSlotEditor(@Nonnull TimeslotHistoricalEditorConfig config,
                    @Nonnull StatsHistoryServiceBlockingStub statsHistoryClient,
                    @Nonnull ExecutorService loadingPool,
                    @Nonnull BiFunction<StatsHistoryServiceBlockingStub, Pair<Long, Long>, TimeSlotLoadingTask> taskCreator) {
        super(config, statsHistoryClient, taskCreator, TimeSlotCommodityData::new, loadingPool,
                        TimeSlotCommodityData::toDebugString);
    }

    @Override
    public void initContext(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> eligibleComms)
                    throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);
        if (!context.isPlan() && startTimestamp <= 0) {
            startTimestamp = getConfig().getClock().millis();
        }
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                    PlanScope scope) {
        return true;
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        return super.isEntityApplicable(entity);
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                    TopologyDTO.CommoditySoldDTO.Builder commSold) {
        return ENABLED_TIMESLOT_COMMODITY_TYPES
            .contains(CommodityType.forNumber(commSold.getCommodityType().getType()));
    }

    @Override
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
            @Nonnull Builder commBought, int providerType) {
        return ENABLED_TIMESLOT_COMMODITY_TYPES
                .contains(CommodityType.forNumber(commBought.getCommodityType().getType()));
    }

    @Override
    public boolean isMandatory() {
        return false;
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>> createPreparationTasks(
                    @Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> commodityRefs) {
        if (context.isPlan()) {
            // Convert the reference to live topology references
            commodityRefs = commodityRefs.stream()
                .map(ref -> ref.getLiveTopologyCommodityReference(context::getClonedFromEntityOid))
                .collect(Collectors.toList());
        }

        if (!hasEnoughHistoricalData(context, commodityRefs)) {
            return Collections.emptyList();
        }
        final List<EntityCommodityReference> uninitializedCommodities =
                        gatherUninitializedCommodities(commodityRefs);
        // partition by configured observation window first
        // we are only interested in business users
        final Map<Integer, List<EntityCommodityReference>> period2comms =
                        uninitializedCommodities.stream().collect(Collectors
                                        .groupingBy(comm -> getConfig()
                                                        .getObservationPeriod(context, comm)));

        List<HistoryLoadingCallable> loadingTasks = new LinkedList<>();
        final long now = getConfig().getClock().millis();
        for (Map.Entry<Integer, List<EntityCommodityReference>> period2comm : period2comms.entrySet()) {
            // chunk commodities of each period by configured size
            List<List<EntityCommodityReference>> partitions = Lists
                .partition(period2comm.getValue(), getConfig().getLoadingChunkSize());
            for (List<EntityCommodityReference> chunk : partitions) {
                final Pair<Long, Long> range = new Pair<>(now
                    - period2comm.getKey() * Duration.ofDays(1).toMillis(),
                    null);
                loadingTasks.add(new HistoryLoadingCallable(context,
                    new TimeSlotLoadingTask(getStatsHistoryClient(), range), chunk));
            }
        }
        return loadingTasks;
    }

    @Override
    protected void exportState(@Nonnull OutputStream appender)
            throws DiagnosticsException, IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(appender)) {
            out.writeLong(startTimestamp);
            final int limit = getConfig().getLoadingChunkSize();
            Map<EntityCommodityFieldReference, TimeSlotCommodityData> buffer = new HashMap<>();
            int countEntries = 0;
            for (Entry<EntityCommodityFieldReference, TimeSlotCommodityData> entry : getCache().entrySet()) {
                buffer.put(entry.getKey(), entry.getValue());
                countEntries++;
                if (countEntries >= limit) {
                    out.writeObject(buffer);
                    countEntries = 0;
                    buffer = new HashMap<>();
                }
                if (Thread.interrupted()) {
                    throw new DiagnosticsException(String.format(
                            "Cannot write timeslots cache into '%s' file. State exporting was interrupted",
                            getFileName()));
                }
            }
        }
    }

    @Override
    protected boolean hasEnoughHistoricalData(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> commodityRefs) {
        if (super.hasEnoughHistoricalData(context, commodityRefs)) {
            return true;
        }

        final int maxObservationPeriod = commodityRefs.stream()
                        .mapToInt(comm -> getConfig().getObservationPeriod(context, comm)).max()
                        .orElse(Integer.MAX_VALUE);
        final long runningTime = getConfig().getClock().millis() - startTimestamp;
        final boolean hasHistoricalData =
                        runningTime > Duration.ofDays(maxObservationPeriod).toMillis();
        if (hasHistoricalData) {
            clearLoadingStatistic();
            debugLogDataValues("Time slot data after re-enabled analysis %s");
            getLogger().info(
                            "Data collected for '{}'ms, which is greater than '{}' days (max observation period). So, analysis will be re-enabled",
                            Duration.ofMillis(runningTime), maxObservationPeriod);
        }
        return hasHistoricalData;
    }

    @Override
    public void completeBroadcast(@Nonnull HistoryAggregationContext context)
                    throws HistoryCalculationException, InterruptedException {
        debugLogDataValues("Time slot data when calculations applied %s");
        super.completeBroadcast(context);
        maintenance(context);
    }

    private void maintenance(@Nonnull HistoryAggregationContext context) {
        if (context.isPlan() || isRunning()) {
            return;
        }
        final Collection<EntityCommodityReference> outdatedReferences = new HashSet<>();
        long startMs = Long.MAX_VALUE;
        long endMs = Long.MIN_VALUE;
        final long nowMs = getConfig().getClock().millis();
        final long maintenancePeriodMs = TimeUnit.HOURS.toMillis(
                getConfig().getMaintenanceWindowHours());
        // anyMatch has been called explicitly for optimization purposes
        final Logger logger = getLogger();
        for (Entry<EntityCommodityFieldReference, TimeSlotCommodityData> refToData : getCache()
                        .entrySet()) {
            final EntityCommodityFieldReference ref = refToData.getKey();
            final long observationWindowMs = TimeUnit.DAYS.toMillis(
                    getConfig().getObservationPeriod(context, ref));
            final long startTimestampMs = refToData.getValue().getLastMaintenanceTimestamp();
            if (!(startTimestampMs < nowMs - observationWindowMs - maintenancePeriodMs)) {
                continue;
            }
            startMs = Math.min(startMs, startTimestampMs);
            endMs = Math.max(endMs, nowMs - observationWindowMs);
            outdatedReferences.add(ref);
        }
        if (outdatedReferences.isEmpty()) {
            return;
        }
        logger.info("Starting maintenance for '{}'ms-'{}'ms time range for '{}' references",
                        Instant.ofEpochMilli(startMs), Instant.ofEpochMilli(endMs),
                        outdatedReferences.size());
        debugLogDataValues("Time slot data before maintenance %s");
        final Stopwatch stopwatch = Stopwatch.createStarted();
        // partition by entity type and configured chunk size
        final Collection<List<EntityCommodityReference>> partitions = partitionCommodities(context,
            outdatedReferences);
        try {
            for (final List<EntityCommodityReference> partitionedOutdatedRefs : partitions) {
                final Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> outdatedRecords =
                    createLoadingTask(Pair.create(startMs, endMs))
                        .load(partitionedOutdatedRefs, getConfig());
                for (Entry<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> refToData : outdatedRecords
                    .entrySet()) {
                    final EntityCommodityFieldReference reference = refToData.getKey();
                    final TimeSlotCommodityData timeSlotCommodityData = getCache().get(reference);
                    if (timeSlotCommodityData == null) {
                        // shouldn't have happened, preparation is supposed to add entries
                        logger.error("TimeSlot maintenance: Missing historical data cache entry for {}",
                            () -> reference);
                        continue;
                    }
                    timeSlotCommodityData.checkpoint(Collections.singletonList(refToData.getValue()));
                }
                logger.info("Maintenance completed for '{}'ms-'{}'ms time range for '{}' references in '{}'ms",
                    Instant.ofEpochMilli(startMs), Instant.ofEpochMilli(endMs),
                    outdatedReferences.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            }
        } catch (HistoryCalculationException e) {
            logger.error("Maintenance failed for '{}'ms-'{}'ms time range for '{}' references in '{}'ms",
                            Instant.ofEpochMilli(startMs), Instant.ofEpochMilli(endMs),
                            outdatedReferences.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), e);
        } finally {
            debugLogDataValues("Time slot data after maintenance %s");
        }
    }

    @Override
    protected void restoreState(@NotNull byte[] bytes) throws DiagnosticsException {
        try (CacheBackup<TimeSlotCommodityData> backup = new CacheBackup<>(getCache(),
                TimeSlotCommodityData::new)) {
            getCache().clear();
            final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            try (ObjectInputStream o = new ObjectInputStream(in)) {
                if (in.available() != 0) {
                    this.startTimestamp = o.readLong();
                }
                while (in.available() != 0) {
                    final Map<EntityCommodityFieldReference, TimeSlotCommodityData> data =
                            (Map<EntityCommodityFieldReference, TimeSlotCommodityData>)o.readObject();
                    if (data != null) {
                        this.getCache().putAll(data);
                    }
                    if (Thread.interrupted()) {
                        throw new DiagnosticsException(String.format(
                                "Cannot write timeslots cache into '%s' file. State exporting was interrupted",
                                getFileName()));
                    }
                }
                backup.keepCacheOnClose();
            } catch (IOException | ClassNotFoundException e) {
                throw new DiagnosticsException("Failed to deserialize timeslot data", e);
            }
        }
    }

    protected long getStartTimestamp() {
        return startTimestamp;
    }
}
