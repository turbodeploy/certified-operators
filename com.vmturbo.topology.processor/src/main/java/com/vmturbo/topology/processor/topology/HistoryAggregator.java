package com.vmturbo.topology.processor.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.utils.ThrowingConsumer;
import com.vmturbo.components.common.utils.TriFunction;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.IHistoricalEditor;

/**
 * A pipeline stage to calculate historical values for commodities.
 * Different kinds of aggregations of per-commodity history into smaller number of values (typically one):
 * - hist utilization (to be ported here)
 * - max quantity (to be ported here)
 * - percentile
 * - time slot (several values)
 * - system load
 * Covers the following aspects:
 * - most (not all) aggregations will have a state between pipeline invocations -
 *   they cache intermediate pre-calculated data and must perform initial loading of
 *   the data from the history component upon topology processor startup
 * - optionally re-load data on a schedule
 * - accumulate the result from cached data and current point and update the topology commodity
 */
public class HistoryAggregator {
    private static final Logger logger = LogManager.getLogger();
    private static final String FAILURE_CAUSE = "Failed to apply historical usages to commodities";
    private static final DataMetricSummary LOAD_HISTORY_SUMMARY_METRIC =
                    DataMetricSummary.builder()
            .withName("tp_historical_loading_time")
            .withHelp("The time spent requesting historical data in chunks from the history component")
            .build();
    private static final DataMetricSummary CALCULATE_HISTORY_SUMMARY_METRIC =
                    DataMetricSummary.builder()
            .withName("tp_historical_calculation_time")
            .withHelp("The time spent aggregating historical data for commodities in the topology")
            .build();

    private final ExecutorService executorService;
    private final Set<IHistoricalEditor<?>> editors;
    private final Map<String, DataMetricSummary> registeredPerEditorMetrics = new HashMap<>();

    /**
     * Construct the history aggregation stage instance.
     *
     * @param executorService executor (shared for initialization and calculation)
     * @param editors configured collection of history data editors
     */
    public HistoryAggregator(@Nonnull ExecutorService executorService,
                                   @Nonnull Set<IHistoricalEditor<?>> editors) {
        this.executorService = executorService;
        this.editors = editors;
    }

    /**
     * Update the commodities with history data.
     *
     * @param graph topology graph
     * @param changes scenarios changes
     * @param topologyInfo topology information
     * @param scope plan scope
     * @throws PipelineStageException when a mandatory history calculation fails
     */
    public void applyCommodityEdits(@Nonnull GraphWithSettings graph,
                                    @Nullable List<ScenarioChange> changes,
                                    @Nonnull TopologyDTO.TopologyInfo topologyInfo,
                                    @Nullable PlanScope scope) throws PipelineStageException {
        if (graph.getTopologyGraph().size() <= 0) {
            logger.debug("Topology graph is empty. Unable to update commodities.");
            return;
        }

        Set<IHistoricalEditor<?>> editorsToRun = editors.stream()
                        .filter(editor -> editor.isApplicable(changes, topologyInfo, scope))
                        .collect(Collectors.toSet());
        if (editorsToRun.isEmpty()) {
            return;
        }

        try {
            final Stopwatch stopwatch = Stopwatch.createStarted();
            Map<IHistoricalEditor<?>, List<EntityCommodityReference>> commsToUpdate =
                            gatherEligibleCommodities(graph.getTopologyGraph(), editorsToRun);
            // store reference to the current settings for policies access
            // set up commodity builders lazy/fast access
            HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo, graph, !CollectionUtils
                            .isEmpty(changes));

            // this may initiate background loading of certain data
            forEachEditor(editorsToRun,
                          editor -> editor.initContext(context, commsToUpdate.get(editor)),
                          "initialization",
                          "The time spent initializing historical data cache for {}");

            try {
                // submit the preparation tasks and wait for completion
                executeTasks(context, commsToUpdate, IHistoricalEditor::createPreparationTasks,
                             true, true, "chunked prepare", LOAD_HISTORY_SUMMARY_METRIC);

                // submit the calculation tasks and wait for completion
                executeTasks(context, commsToUpdate, IHistoricalEditor::createCalculationTasks,
                             false, false, "calculate", CALCULATE_HISTORY_SUMMARY_METRIC);
            } catch (RejectedExecutionException | InterruptedException | ExecutionException e) {
                Throwable reason = e;
                // any failure of a mandatory sub-task stops everything (typically first failure to load from db)
                // next broadcast will initiate loading again
                // executor failure or interruption stop everything regardless of editor type
                if (e instanceof ExecutionException && e.getCause() != null) {
                    reason = e.getCause();
                }
                throw new PipelineStageException(FAILURE_CAUSE, reason);
            }

            forEachEditor(editorsToRun, editor -> editor.completeBroadcast(context), "completion",
                          "The time spent completing historical data broadcast preparation for {}");

            logger.info("History aggregator commodities modified: {} in {}", editorsToRun.stream()
                            .map(editor -> editor.getClass().getSimpleName() + ":" + context.getAccessor()
                                            .getUpdateCount(editor.getClass().getSimpleName()))
                            .collect(Collectors.joining(" ")), stopwatch.stop());
        } catch (PipelineStageException e) {
            // double handling - because pipeline runner swallows stack trace for non-mandatory stages
            logger.warn("History aggregation stage failed", e);
            throw e;
        }
    }

    /**
     * Sequentially execute a task for each editor.
     *
     * @param editorsToRun historical editors
     * @param task task to run
     * @param description logging description
     * @param metricHelpFormat string format for the metric timer to measure performance
     * @throws PipelineStageException when failed critically
     */
    private void forEachEditor(@Nonnull Set<IHistoricalEditor<?>> editorsToRun,
                               @Nonnull ThrowingConsumer<IHistoricalEditor<?>, HistoryCalculationException> task,
                               @Nonnull String description, @Nonnull String metricHelpFormat) throws PipelineStageException {
        for (IHistoricalEditor<?> editor : editorsToRun) {
            String editorName = editor.getClass().getSimpleName();
            String metricName = String.format("tp_historical_%s_time_%s", description, editorName);
            DataMetricSummary metric = registeredPerEditorMetrics.computeIfAbsent(metricName,
                            (name) -> DataMetricSummary.builder().withName(metricName)
                                            .withHelp(String.format(metricHelpFormat, editorName))
                                            .build().register());
            try (DataMetricTimer timer = metric.startTimer()) {
                try {
                    task.accept(editor);
                } catch (HistoryCalculationException | InterruptedException e) {
                    throw new PipelineStageException("Historical calculations " + description
                                                     + " failed for "
                                                     + editor.getClass().getSimpleName(), e);
                } finally {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("%s: %s secs", metricHelpFormat,
                                        timer.getTimeElapsedSecs()), editorName);
                    }
                }
            }
        }
    }

    @Nonnull
    private <Output> Map<IHistoricalEditor<?>, List<Output>>
            executeTasks(@Nonnull HistoryAggregationContext context,
                         @Nonnull Map<IHistoricalEditor<?>, List<EntityCommodityReference>> editor2comms,
                         @Nonnull TriFunction<IHistoricalEditor<?>,
                             HistoryAggregationContext,
                             List<EntityCommodityReference>,
                             List<? extends Callable<List<Output>>>> tasksCreator,
                         boolean splitByEntityType,
                         boolean cleanupCache,
                         @Nonnull String description,
                         @Nonnull DataMetricSummary metric)
                     throws InterruptedException, RejectedExecutionException, ExecutionException {
        try (DataMetricTimer timer = metric.startTimer()) {
            Stopwatch sw = Stopwatch.createStarted();
            Map<IHistoricalEditor<?>, List<Future<List<Output>>>> futures = new HashMap<>();

            int tasksCount = 0;
            // for each editor, submit all chunks to the same executor
            for (Map.Entry<IHistoricalEditor<?>, List<EntityCommodityReference>> editorEntry : editor2comms
                            .entrySet()) {
                IHistoricalEditor<?> editor = editorEntry.getKey();
                List<EntityCommodityReference> editorComms = editorEntry.getValue();
                List<Future<List<Output>>> editorFutures = futures
                                .computeIfAbsent(editor, v -> new LinkedList<>());
                if (!CollectionUtils.isEmpty(editorComms)) {
                    // remove commodities not present in current broadcast if needed
                    if (cleanupCache) {
                        editor.cleanupCache(editorComms);
                    }
                    // if requested, partition eligible commodities by entity type
                    final Collection<List<EntityCommodityReference>> partitionedComms;
                    if (splitByEntityType) {
                        partitionedComms = editorComms.stream().collect(Collectors
                                        .groupingBy(comm -> context.getEntityType(comm.getEntityOid())))
                                        .values();
                    } else {
                        partitionedComms = Collections.singletonList(editorComms);
                    }
                    // create and submit tasks for execution
                    List<Callable<List<Output>>> tasks = partitionedComms.stream()
                                    .flatMap(comms -> tasksCreator
                                                    .apply(editorEntry.getKey(), context, comms)
                                                    .stream())
                                    .collect(Collectors.toList());
                    logger.debug("Submitting {} tasks to {} {} history data of {} commodities",
                                 tasks::size, description::toString,
                                 () -> editor.getClass().getSimpleName(), editorComms::size);
                    tasks.stream().map(executorService::submit).forEach(editorFutures::add);
                    tasksCount += tasks.size();
                }
            }

            // assemble the futures' results
            Map<IHistoricalEditor<?>, List<Output>> results = new HashMap<>();
            for (Map.Entry<IHistoricalEditor<?>, List<Future<List<Output>>>> futuresEntry : futures
                            .entrySet()) {
                List<Output> editorResults = results.computeIfAbsent(futuresEntry.getKey(),
                                                                     v -> new LinkedList<>());
                IHistoricalEditor<?> editor = futuresEntry.getKey();
                boolean editorFailureLogged = false;
                for (Future<List<Output>> editorFuture : futuresEntry.getValue()) {
                    try {
                        editorResults.addAll(editorFuture.get());
                    } catch (ExecutionException e) {
                        if (editor.isMandatory()) {
                            throw e;
                        } else {
                            // failure of a non-mandatory sub-task does not stop broadcast
                            // optional initialization and subsequent calculation will be retried in the next attempt
                            if (!editorFailureLogged) {
                                // only log one chunk failure
                                logger.warn(FAILURE_CAUSE + " for "
                                            + editor.getClass().getSimpleName(), e);
                                editorFailureLogged = true;
                            }
                        }
                    }
                }
            }
            logger.info("Executed {} tasks to {} historical data in {}", tasksCount, description, sw);
            return results;
        }
    }

    @Nonnull
    private static Map<IHistoricalEditor<?>, List<EntityCommodityReference>>
            gatherEligibleCommodities(@Nonnull TopologyGraph<TopologyEntity> graph,
                                      @Nonnull Set<IHistoricalEditor<?>> editorsToRun) {
        Map<IHistoricalEditor<?>, List<EntityCommodityReference>> commoditiesToEdit = new HashMap<>();
        for (IHistoricalEditor<?> editor : editorsToRun) {
            List<EntityCommodityReference> editorComms = commoditiesToEdit
                            .computeIfAbsent(editor, e -> new LinkedList<>());
            graph.entities()
                .filter(editor::isEntityApplicable)
                .forEach(entity -> {
                    TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
                    // gather sold
                    for (CommoditySoldDTO.Builder commSold : entityBuilder
                                    .getCommoditySoldListBuilderList()) {
                        if (editor.isCommodityApplicable(entity, commSold)) {
                            editorComms.add(new EntityCommodityReference(entity.getOid(),
                                                                         commSold.getCommodityType(),
                                                                         null));
                        }
                    }
                    // gather bought
                    for (CommoditiesBoughtFromProvider.Builder commBoughtFromProvider : entityBuilder
                                    .getCommoditiesBoughtFromProvidersBuilderList()) {
                        for (CommodityBoughtDTO.Builder commBought : commBoughtFromProvider
                                        .getCommodityBoughtBuilderList()) {
                            if (editor.isCommodityApplicable(entity, commBought,
                                    commBoughtFromProvider.getProviderEntityType())) {
                                editorComms.add(new EntityCommodityReference(entity.getOid(),
                                                                             commBought.getCommodityType(),
                                                                             commBoughtFromProvider.getProviderId()));
                            }
                        }
                    }
                });
        }
        return commoditiesToEdit;
    }

}
