package com.vmturbo.action.orchestrator.market;

import java.time.LocalDateTime;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline;
import com.vmturbo.action.orchestrator.store.pipeline.ActionProcessingInfo;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.action.orchestrator.store.pipeline.PlanActionPipelineFactory;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.communication.CommunicationException;

/**
 * Orchestrates the processing of a new action plan from the market.
 */
public class ActionOrchestrator {

    private static final Logger logger = LogManager.getLogger();

    private final LiveActionPipelineFactory liveActionPipelineFactory;

    private final PlanActionPipelineFactory planActionPipelineFactory;

    private final ActionOrchestratorNotificationSender notificationSender;

    private final long realtimeTopologyContextId;

    /**
     * Create a new {@link ActionOrchestrator}.
     *
     * @param liveActionPipelineFactory Factory to create pipeline for processing live actions.
     * @param planActionPipelineFactory Factory to create pipeline for processing plan actions.
     * @param notificationSender sender for notifications about actions.
     * @param realtimeTopologyContextId The context ID for the live/realtime topology.
     */
    public ActionOrchestrator(@Nonnull final LiveActionPipelineFactory liveActionPipelineFactory,
                              @Nonnull final PlanActionPipelineFactory planActionPipelineFactory,
                              @Nonnull final ActionOrchestratorNotificationSender notificationSender,
                              final long realtimeTopologyContextId) {
        this.liveActionPipelineFactory = Objects.requireNonNull(liveActionPipelineFactory);
        this.planActionPipelineFactory = Objects.requireNonNull(planActionPipelineFactory);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Process a newly available {@link ActionPlan}.
     *
     * @param orderedActions The new {@link ActionPlan}.
     * @param analysisStart The time at which the analysis that generated the action plan started.
     * @param analysisEnd The time at which the analysis that generated the action plan ended. The duration
     *                    of the analysis is the difference between analysisEnd and analysisStart.
     */
    public void processActions(@Nonnull final ActionPlan orderedActions,
                               @Nonnull final LocalDateTime analysisStart,
                               @Nonnull final LocalDateTime analysisEnd) {
        Objects.requireNonNull(orderedActions);
        Objects.requireNonNull(analysisStart);
        Objects.requireNonNull(analysisEnd);

        try {
            // We don't store "transient" plan actions.
            // However, for transient plans we still want to send the actions update notification
            // so the plan lifecycle can continue as normal.
            if (PlanDTOUtil.isTransientPlan(orderedActions.getInfo().getMarket().getSourceTopologyInfo())) {
                logger.info("Skipping processing of {} actions for transient plan {}",
                    orderedActions.getActionCount(), orderedActions.getInfo().getMarket().getSourceTopologyInfo().getTopologyContextId());
            } else {
                final ActionPipeline<ActionPlan, ActionProcessingInfo> pipeline = buildPipeline(orderedActions);
                final ActionProcessingInfo result = pipeline.run(orderedActions);
                logger.info("Received {} actions in action plan {} (info: {})"
                        + " (analysis start [{}] completion [{}])"
                        + " (store population: {}).",
                    orderedActions.getActionCount(),
                    orderedActions.getId(),
                    orderedActions.getInfo(),
                    analysisStart,
                    analysisEnd,
                    result.getActionStoreSize());
            }
            // Notify listeners that actions are ready for retrieval.
            try {
                notificationSender.notifyActionsUpdated(orderedActions);
            } catch (CommunicationException e) {
                logger.error("Could not send actions recommended notification for "
                    + orderedActions.getId(), e);
            }
        } catch (InterruptedException e) {
            logger.info("Thread interrupted while processing market actions", e);
        } catch (Exception e) {
            logger.error("An error happened while processing market actions.", e);
            notificationSender.notifyActionsUpdateFailure(orderedActions);
        }
    }

    private ActionPipeline<ActionPlan, ActionProcessingInfo> buildPipeline(
        @Nonnull final ActionPlan actionPlan) {
        if (ActionDTOUtil.getActionPlanContextId(actionPlan.getInfo()) == realtimeTopologyContextId) {
            return liveActionPipelineFactory.actionPipeline(actionPlan);
        } else {
            return planActionPipelineFactory.actionPipeline(actionPlan);
        }
    }
}
