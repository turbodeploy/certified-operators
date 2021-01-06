package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.time.Clock;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.severity.SeverityMap;
import com.vmturbo.common.protobuf.severity.SeverityUtil;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * The {@link ActionWriter} is responsible for listening for action updates from the action
 * orchestrator, and writing action-related data to the extractor's database at regular
 * configurable intervals.
 */
public class ActionWriter implements ActionsListener {
    private static final Logger logger = LogManager.getLogger();

    /**
     * System clock.
     */
    private final Clock clock;

    private final ActionsServiceBlockingStub actionService;

    private final EntitySeverityServiceStub severityService;

    private final DataProvider dataProvider;

    private final long realtimeContextId;

    /**
     * The minimum interval for writing action information to the database. We don't write actions
     * every broadcast, because for reporting purposes we don't need action information at 10-minute
     * intervals - especially because actions don't change as frequently as commodities.
     */
    private final long actionWritingIntervalMillis;

    private final boolean enableReportingActionIngestion;

    private final boolean enableSearchActionIngestion;

    private final Supplier<ReportingActionWriter> reportingActionWriterSupplier;

    private final Supplier<SearchActionWriter> searchActionWriterSupplier;

    /**
     * The last time we wrote actions for each action plan type. It's necessary to split up by
     * type, because otherwise a BuyRI action plan will prevent a follow-up Market action plan
     * from being processed.
     */
    private final Map<ActionPlanInfo.TypeInfoCase, Long> lastActionWrite = new EnumMap<>(TypeInfoCase.class);

    ActionWriter(@Nonnull final Clock clock,
            @Nonnull final ActionsServiceBlockingStub actionService,
            @Nonnull final EntitySeverityServiceStub severityService,
            @Nonnull final DataProvider dataProvider,
            final long actionWritingIntervalMillis,
            final boolean enableReportingActionIngestion,
            final boolean enableSearchActionIngestion,
            final long realtimeContextId,
            final Supplier<ReportingActionWriter> reportingActionWriterSupplier,
            final Supplier<SearchActionWriter> searchActionWriterSupplier) {
        this.clock = clock;
        this.actionService = actionService;
        this.severityService = severityService;
        this.actionWritingIntervalMillis = actionWritingIntervalMillis;
        this.realtimeContextId = realtimeContextId;
        this.enableReportingActionIngestion = enableReportingActionIngestion;
        this.enableSearchActionIngestion = enableSearchActionIngestion;
        this.dataProvider = dataProvider;
        this.searchActionWriterSupplier = searchActionWriterSupplier;
        this.reportingActionWriterSupplier = reportingActionWriterSupplier;
        logger.info("Initialized action writer. Reporting action ingestion {}, "
                        + "search action ingestion {}",
                enableReportingActionIngestion ? "enabled" : "disabled",
                enableSearchActionIngestion ? "enabled" : "disabled");
    }

    @Override
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        final long contextId = ActionDTOUtil.getActionPlanContextId(actionsUpdated.getActionPlanInfo());
        if (contextId != realtimeContextId) {
            // Skip non-realtime action plans.
            return;
        }

        final MultiStageTimer timer = new MultiStageTimer(logger);
        final AsyncTimer elapsedTimer = timer.async("Total Elapsed");
        timer.stop();

        final TypeInfoCase actionPlanType = actionsUpdated.getActionPlanInfo().getTypeInfoCase();
        final List<IActionWriter> writers = createActionWriters(actionPlanType);
        if (writers.isEmpty()) {
            // no need to write
            return;
        }

        timer.start("Retrieve actions");
        final int totalActionsCount = fetchActions(writers);
        timer.stop();
        timer.start("Retrieve severities");
        fetchSeverities(contextId, writers);
        timer.stop();
        for (IActionWriter writer : writers) {
            try {
                writer.write(lastActionWrite, actionPlanType, timer);
            } catch (UnsupportedDialectException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("dialect").increment();
                // This shouldn't happen.
                logger.error("{} failed to write action data due to invalid dialect.",
                        writer.getClass().getSimpleName(), e);
            } catch (SQLException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("sql_" + e.getErrorCode()).increment();
                logger.error("{} failed to write action data due to SQL error.",
                        writer.getClass().getSimpleName(), e);
            } catch (InterruptedException e) {
                Metrics.PROCESSING_ERRORS_CNT.labels("interrupted").increment();
                Thread.currentThread().interrupt();
                logger.error("{} interrupted while waiting for action data to write.",
                        writer.getClass().getSimpleName(), e);
            }
        }

        elapsedTimer.close();
        timer.visit((stageName, stopped, totalDurationMs) -> {
            if (stopped) {
                Metrics.PROCESSING_HISTOGRAM.labels(stageName)
                        .observe((double)TimeUnit.MILLISECONDS.toSeconds(totalDurationMs));
            }
        });
        timer.info(FormattedString.format("Processed {} actions", totalActionsCount), Detail.STAGE_SUMMARY);
    }

    /**
     * Create action writers for this cycle based on the given action plan type.
     *
     * @param actionPlanType type of the action plan
     * @return list of action writers to write actions for this cycle
     */
    private List<IActionWriter> createActionWriters(TypeInfoCase actionPlanType) {
        final ImmutableList.Builder<IActionWriter> builder = ImmutableList.builder();
        if (isEnableSearchActionIngestion()) {
            // there are two notifications in short time (buyRI & market), we should avoid fetching
            // twice in short time, currently we only fetch when it's MARKET type. maybe we need a
            // minimal fetching interval like 10 minutes for search, no matter which type.
            if (actionPlanType == TypeInfoCase.MARKET) {
                builder.add(searchActionWriterSupplier.get());
            }
        }

        if (isEnableReportingActionIngestion()) {
            // check if we need to ingest actions for reporting this time
            long lastWriteForType = lastActionWrite.computeIfAbsent(actionPlanType, k -> 0L);
            final long now = clock.millis();
            final long nextUpdateTime = lastWriteForType + actionWritingIntervalMillis;
            if (nextUpdateTime <= now) {
                builder.add(reportingActionWriterSupplier.get());
            } else {
                logger.info("Not writing reporting action metrics for another {} minutes.",
                        TimeUnit.MILLISECONDS.toMinutes(nextUpdateTime - now));
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    public boolean isEnableReportingActionIngestion() {
        return enableReportingActionIngestion;
    }

    @VisibleForTesting
    public boolean isEnableSearchActionIngestion() {
        return enableSearchActionIngestion;
    }

    @VisibleForTesting
    int fetchActions(@Nonnull List<IActionWriter> actionConsumers) {
        final MutableInt totalActionsCount = new MutableInt(0);
        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(realtimeContextId)
                // stream all actions
                .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                .build();
        logger.debug("Retrieving actions for context {}", realtimeContextId);
        try {
            actionService.getAllActions(actionRequest).forEachRemaining(response -> {
                List<ActionOrchestratorAction> actionsList =
                        response.getActionChunk().getActionsList();
                actionsList.forEach(aoAction -> {
                    if (aoAction.hasActionSpec()) {
                        actionConsumers.forEach(actionConsumer -> actionConsumer.accept(aoAction));
                    }
                });
                totalActionsCount.add(actionsList.size());
            });
            logger.info("Retrieved {} actions from action orchestrator", totalActionsCount);
        } catch (StatusRuntimeException e) {
            Metrics.PROCESSING_ERRORS_CNT.labels("grpc_" + e.getStatus().getCode()).increment();
            logger.error("Failed to retrieve actions.", e);
        }
        return totalActionsCount.intValue();
    }

    /**
     * Fetch severities for the given topologyContextId.
     *
     * @param topologyContextId id of the topology context
     * @param severityConsumers consumers of the severity
     */
    void fetchSeverities(long topologyContextId, List<IActionWriter> severityConsumers) {
        TopologyGraph<SupplyChainEntity> topologyGraph = dataProvider.getTopologyGraph();
        if (topologyGraph == null) {
            logger.error("Topology graph is not ready, skipping fetching severities for this cycle");
            return;
        }
        final List<Long> allEntities = topologyGraph.entities()
                .map(SupplyChainEntity::getOid)
                .collect(Collectors.toList());
        try {
            SeverityMap severityMap = SeverityUtil.calculateSeverities(topologyContextId,
                    allEntities, severityService).get();
            logger.info("Retrieved {} severities from action orchestrator", severityMap.size());
            severityConsumers.forEach(consumer -> consumer.acceptSeverity(severityMap));
        } catch (InterruptedException | ExecutionException | StatusRuntimeException e) {
            logger.error("Error fetching severities for {}", topologyContextId, e);
        }
    }

    /**
     * Interface for writing actions and severities.
     */
    interface IActionWriter extends Consumer<ActionOrchestratorAction> {
        default void acceptSeverity(SeverityMap severityMap) {}

        void write(Map<ActionPlanInfo.TypeInfoCase, Long> lastActionWrite,
                TypeInfoCase actionPlanType, MultiStageTimer timer)
                throws UnsupportedDialectException, InterruptedException, SQLException;
    }

    /**
     * Metrics relevant for action processing.
     */
    private static class Metrics {

        private Metrics() {}

        private static final DataMetricHistogram PROCESSING_HISTOGRAM =
            DataMetricHistogram.builder()
                .withName("xtr_action_processing_duration_seconds")
                .withHelp("Duration of action processing (fetching data and writing)" + " broken down by stage. Only for cycles that fully processed without errors.")
                .withLabelNames("stage")
                // 10s, 1min, 3min, 7min, 10min, 15min, 30min
                // The SLA is to process everything in under 10 minutes.
                .withBuckets(10, 60, 180, 420, 600, 900, 1800)
                .build()
                .register();

        private static final DataMetricCounter PROCESSING_ERRORS_CNT =
            DataMetricCounter.builder()
                .withName("xtr_action_processing_error_count")
                .withHelp("Errors encountered during action processing, broken down by high level type.")
                .withLabelNames("type")
                .build()
                .register();
    }
}
