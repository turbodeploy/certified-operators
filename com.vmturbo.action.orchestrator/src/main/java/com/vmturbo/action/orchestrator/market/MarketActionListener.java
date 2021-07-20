package com.vmturbo.action.orchestrator.market;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.AnalysisSummaryListener;

/**
 * Listens to action recommendations from the market.
 */
public class MarketActionListener implements ActionsListener, AnalysisSummaryListener, AutoCloseable {

    private static final Logger logger = LogManager.getLogger();

    private final ActionOrchestrator orchestrator;

    private final ActionPlanAssessor actionPlanAssessor;

    private final Object latestKnownActionPlanLock = new Object();

    /**
     * The ID of the latest known action plan (received on the analysis-summary topic).
     */
    private long latestMarketActionPlanId = -1;

    private final ExecutorService planActionsExecutorService;
    private final ExecutorService realtimeActionsExecutorService;
    private final long realtimeTopologyContextId;

    /**
     * Constructs new instance of {@code MarketActionListener}.
     *
     * @param actionOrchestrator The orchestrator that handles the processing of a new
     *         action plan.
     * @param actionPlanAssessor Action plan assessor.
     * @param realtimeTopologyContextId The ID of the realtime topology context.
     */
    public MarketActionListener(@Nonnull final ActionOrchestrator actionOrchestrator,
            @Nonnull final ActionPlanAssessor actionPlanAssessor, long realtimeTopologyContextId) {
        this(actionOrchestrator, actionPlanAssessor,
                createSingleThreadExecutor("processing-plan-actions-%d"),
                createSingleThreadExecutor("processing-realtime-actions-%d"),
                realtimeTopologyContextId);
    }

    /**
     * Constructs new instance of {@code MarketActionListener}.
     *
     * @param actionOrchestrator The orchestrator that handles the processing of a new
     *         action plan.
     * @param actionPlanAssessor Action plan assessor.
     * @param planActionsExecutorService Thread pool for processing plan actions
     * @param realtimeActionsExecutorService Thread pool for processing realtime actions
     * @param realtimeTopologyContextId The ID of the realtime topology context.
     */
    @VisibleForTesting
    public MarketActionListener(@Nonnull final ActionOrchestrator actionOrchestrator,
            @Nonnull final ActionPlanAssessor actionPlanAssessor,
            @Nonnull ExecutorService planActionsExecutorService,
            @Nonnull ExecutorService realtimeActionsExecutorService,
            long realtimeTopologyContextId) {
        this.orchestrator = Objects.requireNonNull(actionOrchestrator);
        this.actionPlanAssessor = Objects.requireNonNull(actionPlanAssessor);
        this.planActionsExecutorService = Objects.requireNonNull(planActionsExecutorService);
        this.realtimeActionsExecutorService = Objects.requireNonNull(realtimeActionsExecutorService);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void onAnalysisSummary(@Nonnull final AnalysisSummary analysisSummary) {
        if (analysisSummary.getSourceTopologyInfo().getTopologyType() == TopologyType.REALTIME) {
            synchronized (latestKnownActionPlanLock) {
                latestMarketActionPlanId = analysisSummary.getActionPlanSummary().getActionPlanId();
            }
        }
    }

    /**
     * Return whether or not we should skip processing the action plan.
     *
     * @param actionPlan The action plan.
     * @return True if we shouldn't use this action plan to populate actions.
     */
    private boolean shouldSkip(@Nonnull final ActionPlan actionPlan) {
        // We never skip plan action plans.
        if (!isLiveMktActionPlan(actionPlan)) {
            return false;
        }

        // We skip expired live action plans no matter what.
        //
        // This means that when we first start up, if we get an old action plan before getting
        // all the analysis summaries, we still skip it.
        if (actionPlanAssessor.isActionPlanExpired(actionPlan)) {
            return true;
        }

        synchronized (latestKnownActionPlanLock) {
            // We rely on the fact that (right now) action plan ids are increasing.
            // If we already got an analysis summary indicating there is a "newer" action plan
            // available, we skip the current action plan.
            return actionPlan.getId() < latestMarketActionPlanId;
        }
    }

    @Override
    public void onActionsReceived(@Nonnull final ActionPlan orderedActions,
                                  @Nonnull final SpanContext tracingContext) {
        if (logger.isDebugEnabled()) {
            orderedActions.getActionList().forEach(action -> logger.debug("Received action: " + action));
        }

        final LocalDateTime analysisStart = localDateTimeFromSystemTime(orderedActions.getAnalysisStartTimestamp());
        final LocalDateTime analysisEnd = localDateTimeFromSystemTime(orderedActions.getAnalysisCompleteTimestamp());
        if (shouldSkip(orderedActions)) {
            logger.warn("Dropping action plan {} (info: {}) " +
                            "with {} actions (analysis start [{}] completion [{}])",
                    orderedActions.getId(),
                    orderedActions.getInfo(),
                    orderedActions.getActionCount(),
                    analysisStart,
                    analysisEnd);
        } else {
            // Populate the store with the new recommendations and refresh the cache.
            try (TracingScope tracingScope = Tracing.trace("on_actions_received", tracingContext)) {
                // process actions for realtime and plan action plans in separate threads
                // in order not to block receiving new action plans
                final Runnable processActionPlan = () -> {
                    if (!shouldSkip(orderedActions)) {
                        orchestrator.processActions(orderedActions, analysisStart, analysisEnd);
                    } else {
                        logger.warn("Dropping action plan {} (info: {}) " +
                                        "with {} actions (analysis start [{}] completion [{}])",
                                orderedActions.getId(),
                                orderedActions.getInfo(),
                                orderedActions.getActionCount(),
                                analysisStart,
                                analysisEnd);
                    }
                };
                if (ActionDTOUtil.getActionPlanContextId(orderedActions.getInfo()) == realtimeTopologyContextId) {
                    realtimeActionsExecutorService.execute(processActionPlan);
                } else {
                    planActionsExecutorService.execute(processActionPlan);
                }
            }
        }
    }

    private LocalDateTime localDateTimeFromSystemTime(final long systemTimeMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(systemTimeMillis), ZoneOffset.UTC);
    }

    /**
     * Decide if an action plan is a live action plan.
     *
     * @param orderedActions The action plan to assess.
     * @return True if the action plan is due to a live analysis, false if it is not.
     */
    private boolean isLiveMktActionPlan(@Nonnull final ActionPlan orderedActions) {
        return orderedActions.getInfo().hasMarket() &&
            orderedActions.getInfo().getMarket().getSourceTopologyInfo().getTopologyType() == TopologyType.REALTIME;
    }

    private static ExecutorService createSingleThreadExecutor(@Nonnull String threadNameFormat) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                threadNameFormat).build();
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void close() {
        planActionsExecutorService.shutdownNow();
        realtimeActionsExecutorService.shutdownNow();
    }
}
