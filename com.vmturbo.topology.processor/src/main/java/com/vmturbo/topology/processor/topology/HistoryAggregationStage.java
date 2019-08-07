package com.vmturbo.topology.processor.topology;

import java.util.Collection;
import java.util.Comparator;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.history.EntityCommodityReferenceWithBuilder;
import com.vmturbo.topology.processor.history.IHistoricalEditor;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

/**
 * A pipeline stage to calculate historical values for commodities.
 * Different kinds of aggregations of per-commodity history into smaller number of values (typically one):
 * - hist utilization
 * - max quantity
 * - percentile
 * - time slot (several values)
 * - system load
 * Covers the following aspects:
 * - most (not all) aggregations will have a state between pipeline invocations -
 *   they cache intermediate pre-calculated data and must perform initial loading of
 *   the data from the history component upon topology processor startup
 * - optionally re-load data on a schedule
 * - accumulate the result from cached data and current point and update the topology commodity
 * TODO dmitry add to pipelines when all underlying aggregations are implemented
 */
public class HistoryAggregationStage {
    private static final Logger logger = LogManager.getLogger();
    private static final String FAILURE_CAUSE = "Failed to apply historical usages to commodities";
    private static final DataMetricSummary LOAD_HISTORY_SUMMARY =
                    DataMetricSummary.builder()
            .withName("tp_historical_loading_time")
            .withHelp("The time spent initializing historical data cache from the history component")
            .build();
    private static final DataMetricSummary CALCULATE_HISTORY_SUMMARY =
                    DataMetricSummary.builder()
            .withName("tp_historical_calculation_time")
            .withHelp("The time spent aggregating historical data for commodities in the topology")
            .build();

    private final ExecutorService executorService;
    private final Set<IHistoricalEditor<?>> editors;

    /**
     * Construct the history aggregation stage instance.
     *
     * @param executorService executor (shared for initialization and calculation)
     * @param editors configured collection of history data editors
     */
    public HistoryAggregationStage(@Nonnull ExecutorService executorService,
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
    public void applyCommodityEdits(@Nonnull TopologyGraph<TopologyEntity> graph,
                                    @Nonnull List<ScenarioChange> changes,
                                    @Nonnull TopologyDTO.TopologyInfo topologyInfo,
                                    @Nonnull PlanScope scope) throws PipelineStageException {
        Set<IHistoricalEditor<?>> editorsToRun = editors.stream()
                        .filter(editor -> editor.isApplicable(changes, topologyInfo, scope))
                        .collect(Collectors.toSet());
        Map<IHistoricalEditor<?>, List<EntityCommodityReferenceWithBuilder>> commsToUpdate =
                        gatherEligibleCommodities(graph, editorsToRun);

        try {
            // submit the preparation tasks and wait for completion
            executeTasks(commsToUpdate, IHistoricalEditor::createPreparationTasks,
                         "initialize", LOAD_HISTORY_SUMMARY);

            // TODO dmitry limit the waiting time (config setting) for non-mandatory tasks
            // if it's exceeded, proceed without setting the relevant historical values
            // and keep loading in the background

            // submit the calculation tasks and wait for completion
            executeTasks(commsToUpdate, IHistoricalEditor::createCalculationTasks,
                         "calculate", CALCULATE_HISTORY_SUMMARY);
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
    }

    @Nonnull
    private <Output> Map<IHistoricalEditor<?>, List<Output>>
            executeTasks(@Nonnull Map<IHistoricalEditor<?>, List<EntityCommodityReferenceWithBuilder>> editor2comms,
                         @Nonnull BiFunction<IHistoricalEditor<?>,
                             List<EntityCommodityReferenceWithBuilder>,
                             List<? extends Callable<List<Output>>>> tasksCreator,
                         @Nonnull String description,
                         @Nonnull DataMetricSummary metric)
                     throws InterruptedException, RejectedExecutionException, ExecutionException {
        try (DataMetricTimer timer = metric.startTimer()) {
            Stopwatch sw = Stopwatch.createStarted();
            Map<IHistoricalEditor<?>, List<Future<List<Output>>>> futures = new HashMap<>();

            int tasksCount = 0;
            // for each editor, submit all chunks to the same executor
            for (Map.Entry<IHistoricalEditor<?>, List<EntityCommodityReferenceWithBuilder>> editorEntry : editor2comms
                            .entrySet()) {
                IHistoricalEditor<?> editor = editorEntry.getKey();
                List<EntityCommodityReferenceWithBuilder> editorComms = editorEntry.getValue();
                List<Future<List<Output>>> editorFutures = futures
                                .computeIfAbsent(editor, v -> new LinkedList<>());
                if (!CollectionUtils.isEmpty(editorComms)) {
                    Collection<? extends Callable<List<Output>>> tasks = tasksCreator
                                    .apply(editorEntry.getKey(), editorComms);
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
    private static Map<IHistoricalEditor<?>, List<EntityCommodityReferenceWithBuilder>>
            gatherEligibleCommodities(@Nonnull TopologyGraph<TopologyEntity> graph,
                                      @Nonnull Set<IHistoricalEditor<?>> editorsToRun) {
        Map<IHistoricalEditor<?>, List<EntityCommodityReferenceWithBuilder>> commoditiesToEdit = new HashMap<>();
        for (IHistoricalEditor<?> editor : editorsToRun) {
            List<EntityCommodityReferenceWithBuilder> editorComms = commoditiesToEdit
                            .computeIfAbsent(editor, e -> new LinkedList<>());
            graph.entities().filter(editor::isEntityApplicable).forEach(entity -> {
                TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
                // gather sold
                for (CommoditySoldDTO.Builder commSold : entityBuilder
                                .getCommoditySoldListBuilderList()) {
                    if (editor.isCommodityApplicable(entity, commSold)) {
                        editorComms.add(new EntityCommodityReferenceWithBuilder(entity.getOid(),
                                                                                commSold.getCommodityType(),
                                                                                commSold));
                    }
                }
                // gather bought
                for (CommoditiesBoughtFromProvider.Builder commBoughtFromProvider : entityBuilder
                                .getCommoditiesBoughtFromProvidersBuilderList()) {
                    for (CommodityBoughtDTO.Builder commBought : commBoughtFromProvider
                                    .getCommodityBoughtBuilderList()) {
                        if (editor.isCommodityApplicable(entity, commBought)) {
                            editorComms.add(new EntityCommodityReferenceWithBuilder(entity.getOid(),
                                                                                    commBought.getCommodityType(),
                                                                                    commBoughtFromProvider.getProviderId(),
                                                                                    commBought));
                        }
                    }
                }
            });
            // we shouldn't have known, but we do - that history component stores 'main' history
            // in per-entity-type tables
            editorComms.sort(new Comparator<EntityCommodityReferenceWithBuilder>() {
                @Override
                public int compare(EntityCommodityReferenceWithBuilder o1,
                                   EntityCommodityReferenceWithBuilder o2) {
                    // by entity type only
                    Integer type1 = graph.getEntity(o1.getEntityOid()).map(TopologyGraphEntity::getEntityType).orElse(null);
                    Integer type2 = graph.getEntity(o2.getEntityOid()).map(TopologyGraphEntity::getEntityType).orElse(null);
                    return (type1 == null || type2 == null) ? 0 : Integer.compare(type1, type2);
                }});
        }
        return commoditiesToEdit;
    }

}
