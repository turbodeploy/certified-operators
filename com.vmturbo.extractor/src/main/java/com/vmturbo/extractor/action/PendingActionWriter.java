package com.vmturbo.extractor.action;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.severity.SeverityMap;
import com.vmturbo.common.protobuf.severity.SeverityUtil;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * The {@link PendingActionWriter} is responsible for listening for action updates from the action
 * orchestrator, and writing action-related data to the extractor's database at regular
 * configurable intervals.
 */
public class PendingActionWriter implements ActionsListener {
    private static final Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionService;

    private final EntitySeverityServiceStub severityService;

    private final PolicyServiceBlockingStub policyService;

    private final DataProvider dataProvider;

    private final long realtimeContextId;

    private final ExtractorFeatureFlags extractorFeatureFlags;

    private final ActionWriterFactory actionWriterFactory;

    PendingActionWriter(@Nonnull final ActionsServiceBlockingStub actionService,
                        @Nonnull final EntitySeverityServiceStub severityService,
                        @Nonnull final PolicyServiceBlockingStub policyService,
                        @Nonnull final DataProvider dataProvider,
                        @Nonnull final ExtractorFeatureFlags extractorFeatureFlags,
                        final long realtimeContextId,
                        @Nonnull final ActionWriterFactory actionWriterFactory) {
        this.actionService = actionService;
        this.severityService = severityService;
        this.policyService = policyService;
        this.realtimeContextId = realtimeContextId;
        this.extractorFeatureFlags = extractorFeatureFlags;
        this.dataProvider = dataProvider;
        this.actionWriterFactory = actionWriterFactory;
        logger.info("Initialized action writer. Feature flags: {}", extractorFeatureFlags);
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

        // fetch policies before actions, since they may be used in action writers
        // currently it's only fetched when data extraction is enabled
        final List<IActionWriter> writersRequiringPolicy = writers.stream()
                .filter(IActionWriter::requirePolicy)
                .collect(Collectors.toList());
        if (!writersRequiringPolicy.isEmpty()) {
            timer.start("Retrieve policies");
            fetchPolicies(writersRequiringPolicy);
            timer.stop();
        }

        // actions
        timer.start("Retrieve actions");
        final int totalActionsCount = fetchActions(writers);
        timer.stop();

        // severities
        final List<IActionWriter> writersRequiringSeverity = writers.stream()
                .filter(IActionWriter::requireSeverity)
                .collect(Collectors.toList());
        if (!writersRequiringSeverity.isEmpty()) {
            timer.start("Retrieve severities");
            fetchSeverities(contextId, writersRequiringSeverity);
            timer.stop();
        }

        // write
        for (IActionWriter writer : writers) {
            try {
                writer.write(timer);
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
        if (extractorFeatureFlags.isSearchEnabled()) {
            // there are two notifications in short time (buyRI & market), we should avoid fetching
            // twice in short time, currently we only fetch when it's MARKET type. maybe we need a
            // minimal fetching interval like 10 minutes for search, no matter which type.
            if (actionPlanType == TypeInfoCase.MARKET) {
                actionWriterFactory.getSearchPendingActionWriter().ifPresent(builder::add);
            }
        }

        if (extractorFeatureFlags.isReportingActionIngestionEnabled()) {
            actionWriterFactory.getReportPendingActionWriter(actionPlanType).ifPresent(builder::add);
        }

        if (extractorFeatureFlags.isExtractionEnabled()) {
            actionWriterFactory.getDataExtractionActionWriter().ifPresent(builder::add);
        }
        return builder.build();
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
                        actionConsumers.forEach(actionConsumer -> actionConsumer.recordAction(aoAction));
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
     * Fetch all policies in the system.
     *
     * @param policyConsumers consumers of the policies
     */
    void fetchPolicies(List<IActionWriter> policyConsumers) {
        try {
            final Map<Long, PolicyDTO.Policy> policyById = new HashMap<>();
            policyService.getPolicies(PolicyDTO.PolicyRequest.newBuilder().build()).forEachRemaining(
                    response -> policyById.put(response.getPolicy().getId(), response.getPolicy()));
            logger.info("Retrieved {} policies from group component", policyById.size());
            policyConsumers.forEach(consumer -> consumer.acceptPolicy(policyById));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to fetch policies", e);
        }
    }

    /**
     * Interface for writing actions and severities.
     */
    interface IActionWriter {
        default boolean requireSeverity() {
            return false;
        }

        default void acceptSeverity(SeverityMap severityMap) {}

        default boolean requirePolicy() {
            return false;
        }

        default void acceptPolicy(Map<Long, PolicyDTO.Policy> policyById) {}

        void recordAction(ActionOrchestratorAction action);

        void write(MultiStageTimer timer) throws UnsupportedDialectException, InterruptedException, SQLException;
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
