package com.vmturbo.cost.component.savings;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * Listens for events from the action orchestrator and inserts events into the internal
 * savings event log.  Interesting event types are: new recommendation and action successfully
 * executed.
 */
public class ActionListener implements ActionsListener {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param config entity savings configuration.
     */
    ActionListener(EntitySavingsConfig config) {
        logger.debug("Action listener enabled.");
    }

    /**
     * A notification sent by the Topology Processor to report the successful
     * completion of an action being executed. This notification will be triggered
     * when the Topology Processor receives the corresponding success update from
     * a probe.
     *
     * @param actionSuccess The progress notification for an action.
     */
    @Override
    public void onActionSuccess(@Nonnull ActionSuccess actionSuccess) {
        /*
         * TODO:
         *  - Locate the target entity in the internal entity state.  If not present, create an
         *    entry for it.
         *  - Log a provider change event into the event log.
         */
    }

    /**
     * Callback when the actions stored in the ActionOrchestrator have been updated. Replaces the
     * "onActionsReceived" event.
     *
     * @param actionsUpdated Context containing topology and action plan information.
     */
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        if (!actionsUpdated.hasActionPlanInfo() || !actionsUpdated.hasActionPlanId()) {
            logger.warn("Malformed action update - skipping");
        }
        ActionPlanInfo actionPlan = actionsUpdated.getActionPlanInfo();
        if (!actionPlan.hasMarket()) {
            // We currently only want to see market (vs. buy RI) action plans.
            return;
        }
        logger.debug("Processing onActionsUpdated, actionPlanId = {}",
                actionsUpdated.getActionPlanId());
        TopologyInfo info = actionPlan.getMarket().getSourceTopologyInfo();
        if (TopologyType.REALTIME != info.getTopologyType()) {
            // We only care about realtime actions.
            return;
        }

        logger.debug("Handling realtime action updates");
        /*
         * TODO:
         *  - Iterate over actions list and identify resize recommendations.
         *  - Add any target entities that are not currently in the internal state database.
         *  - Insert recommendation events into the event log.
         */
    }

}
