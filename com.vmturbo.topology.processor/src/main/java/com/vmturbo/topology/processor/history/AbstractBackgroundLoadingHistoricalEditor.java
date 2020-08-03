package com.vmturbo.topology.processor.history;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Historical commodities editor with cached commodity data state that initiates data from
 * persistent store in background. While background loading is in progress, related historical data
 * are not set into the broadcast.
 *
 * @param <DbValue> the pre-calculated element of per-field data as retrieved from
 *                 the persistent store
 * @param <HistoryData> per-commodity field historical data to cache that wraps
 *                 DbValue with runtime info
 * @param <HistoryLoadingTask> loader of DbValue's from the persistent store
 * @param <Config> per-editor type configuration values holder
 * @param <Stub> type of history component stub
 * @param <CheckpointResult> the result of checkpoint, if applicable
 */
public abstract class AbstractBackgroundLoadingHistoricalEditor<HistoryData extends IHistoryCommodityData<Config, DbValue, CheckpointResult>,
                HistoryLoadingTask extends IHistoryLoadingTask<Config, DbValue>,
                Config extends BackgroundLoadingHistoricalEditorConfig,
                DbValue,
                Stub extends io.grpc.stub.AbstractStub<Stub>,
                CheckpointResult>
                extends AbstractCachingHistoricalEditor<HistoryData, HistoryLoadingTask, Config, DbValue, Stub, CheckpointResult> {
    private final Logger logger = LogManager.getLogger(getClass());
    private final ExecutorService backgroundLoadingPool;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    /**
     * Lifecycle of field below is longer that lifecycle of the broadcast. This is caused by the
     * fact that background loading is long process which is going to take more than one
     * broadcast/discovery cycle.
     */
    private final Collection<Pair<List<EntityCommodityReference>, Future<?>>> running =
                    new ArrayList<>();
    private final Function<HistoryData, String> dataToDebugString;

    /**
     * Lifecycle for fields below differ from what was described for running field. First of all
     * those two fields could be reset back, in case TP was working for more than max observation
     * period time required to calculate historical data. In this case we will reset those fields to
     * allow background calculation for the case when customer will add a target with big amount of
     * entities that would require background processing. IMPORTANT those fields need to be reset
     * simultenaously in described use-case.
     */
    private long backgroundLoadingStartTimestamp;
    private int attempt;

    /**
     * Construct the instance.
     *
     * @param config per-type configuration
     * @param statsHistoryClient history db client
     * @param historyLoadingTaskCreator create an instance of a db value loading
     *                 task
     * @param historyDataCreator create an instance of cached history element
     * @param backgroundLoadingPool pool to execute background loading tasks.
     * @param dataToDebugString debug string provider for history data implementation.
     */
    protected AbstractBackgroundLoadingHistoricalEditor(@Nonnull Config config,
                    @Nonnull Stub statsHistoryClient,
                    @Nonnull BiFunction<Stub, Pair<Long, Long>, HistoryLoadingTask> historyLoadingTaskCreator,
                    @Nonnull Supplier<HistoryData> historyDataCreator,
                    @Nonnull ExecutorService backgroundLoadingPool,
                    @Nonnull Function<HistoryData, String> dataToDebugString) {
        super(config, statsHistoryClient, historyLoadingTaskCreator, historyDataCreator);
        this.backgroundLoadingPool = backgroundLoadingPool;
        this.dataToDebugString = dataToDebugString;
    }

    @Override
    public void initContext(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> eligibleComms)
                    throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);
        if (context.isPlan()) {
            return;
        }

        // chunk by configured size
        final Set<EntityCommodityReference> recentCommodities =
                        new HashSet<>(gatherUninitializedCommodities(eligibleComms));
        recentCommodities.removeAll(running.stream().map(Pair::getFirst)
                        .flatMap(Collection::stream).collect(Collectors.toSet()));
        final Collection<EntityCommodityReference> settingsChangedCommodities =
                        gatherSettingsChangedCommodities(eligibleComms, context);
        if (!settingsChangedCommodities.isEmpty()) {
            logger.info("Settings changed for '{}' references", settingsChangedCommodities.size());
        }
        recentCommodities.addAll(settingsChangedCommodities);
        if (!running.isEmpty() || recentCommodities.size() > getConfig().getBackgroundLoadThreshold()) {
            if (running.isEmpty()) {
                debugLogDataValues("Data before background loading: %s");
                backgroundLoadingStartTimestamp = getConfig().getClock().millis();
                stopwatch.start();
                logger.info("Background loading process started for '{}' commodities.",
                                recentCommodities::size);
            }

            // we need to partition by entity type because we request history for multiples entity
            // types, e.g. DESKTOP_POOL, BUSINESS_USER.
            // History component does not support pagination by multiple entity types hence the split
            final Collection<List<EntityCommodityReference>> chunksToSchedule  =
                partitionCommodities(context, recentCommodities);
            chunksToSchedule.addAll(checkScheduledTasks());
            if (attemptsExceeded()) {
                closeResources("Attempts",
                                getConfig().getBackgroundLoadRetries());
                return;
            }

            if (timeoutExceeded()) {
                closeResources("Timeout",
                                getConfig().getBackgroundLoadTimeoutMin());
                return;
            }
            chunksToSchedule.forEach(chunk -> running.add(Pair.create(chunk, backgroundLoadingPool
                            .submit(new HistoryLoadingCallable(context, createLoadingTask(null),
                                            chunk)))));
            if (running.isEmpty()) {
                clearLoadingStatistic();
                debugLogDataValues("Data after background loading completed successfully: %s");
                logger.info("Background loading process completed successfully in '{}' ms",
                                stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
            }
        } else {
            settingsChangedCommodities.forEach(ref -> getCache()
                            .remove(new EntityCommodityFieldReference(ref, CommodityField.USED)));
        }
    }

    /**
     * Prints debug information for history data about specific object with specified description.
     *
     * @param description describes a context when we are printing history data
     *                 values.
     */
    protected void debugLogDataValues(String description) {
        debugLogDataValues(logger,
                        (data) -> String.format(description, dataToDebugString.apply(data)));
    }

    private boolean timeoutExceeded() {
        return backgroundLoadingStartTimestamp > 0
                        && Duration
                        .ofMillis(getConfig().getClock().millis() - backgroundLoadingStartTimestamp)
                        .toMinutes() > getConfig().getBackgroundLoadTimeoutMin();
    }

    private void closeResources(String parameterName, long value) {
        final String message =
                        String.format("%s to do background loading history data exceeded '%s' in '%s'ms. Analysis will be disabled.",
                                        parameterName, value,
                                        stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
        logger.error(message);
        debugLogDataValues("Data after background loading failed: %s");
        running.stream().map(Pair::getSecond).forEach(f -> f.cancel(true));
        running.clear();
    }

    private boolean attemptsExceeded() {
        return attempt >  getConfig().getBackgroundLoadRetries();
    }

    private List<List<EntityCommodityReference>> checkScheduledTasks() throws InterruptedException {
        final List<List<EntityCommodityReference>> chunksToReschedule = new ArrayList<>();
        final Iterator<Pair<List<EntityCommodityReference>, Future<?>>> it = running.iterator();
        while (it.hasNext()) {
            final Pair<List<EntityCommodityReference>, Future<?>> itemsToTask = it.next();
            final Future<?> task = itemsToTask.getSecond();
            if (!task.isDone()) {
                continue;
            }
            final List<EntityCommodityReference> items = itemsToTask.getFirst();
            try {
                task.get();
                logger.trace("Background loading task completed successfully '{}' attempt for '{}' commodity references.",
                                attempt, items.size());
            } catch (ExecutionException e) {
                attempt++;
                chunksToReschedule.add(items);
                logger.warn("Background loading task failed '{}' attempt for '{}' commodity references. Task will be rescheduled",
                                attempt, items.size(), e.getCause());
            } finally {
                it.remove();
            }
        }
        return chunksToReschedule;
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<Void>>> createCalculationTasks(
                    @Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> commodityRefs) {
        if (hasEnoughHistoricalData(context, commodityRefs)) {
            return super.createCalculationTasks(context, commodityRefs);
        } else {
            logger.warn("Analysis will be disabled because there are still '{}' running loading tasks",
                            running.size());
            return Collections.emptyList();
        }
    }

    /**
     * Returns logger instance.
     *
     * @return logger instance.
     */
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Checks whether we have enough historical data to run calculation tasks.
     *
     * @param context invocation context i.e current graph
     * @param commodityRefs commodities that have to be processed
     * @return {@code true} in case background loading did not fail and in case it is does not run.
     */
    protected boolean hasEnoughHistoricalData(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> commodityRefs) {
        return running.isEmpty() && !(attemptsExceeded() || timeoutExceeded());
    }

    /**
     * Clears loading statistic collected for the previous background loading process.
     */
    protected void clearLoadingStatistic() {
        attempt = 0;
        backgroundLoadingStartTimestamp = 0;
    }

    /**
     * Returns {@code} true in case background loading is running.
     *
     * @return {@code} true in case background loading is running.
     */
    protected boolean isRunning() {
        return !running.isEmpty();
    }


    /**
     * Partition commodities by entity type and configured load size.
     *
     * @param context Invocation context i.e current graph
     * @param commRefs Commodities to be processed
     * @return Commodity references partitioned by entity type
     */
    @Nonnull
    protected Collection<List<EntityCommodityReference>> partitionCommodities(
            @Nonnull final HistoryAggregationContext context,
            @Nonnull final Collection<EntityCommodityReference> commRefs) {
        final Collection<List<EntityCommodityReference>> chunksToSchedule = new ArrayList<>();
        commRefs.stream()
            .collect(Collectors.groupingBy(comm -> context.getEntityType(comm.getEntityOid())))
            .values().forEach(comms -> chunksToSchedule.addAll(Lists.partition(comms,
                                                                getConfig().getLoadingChunkSize())));
        return chunksToSchedule;
    }

}
