package com.vmturbo.action.orchestrator.execution;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalTask;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * Execution task with the condition defined in
 * {@link #compareTo(ConditionalTask)}. No two tasks with the same condition
 * can be executed at the same time.
 */
public class ManuallyOrExternallyApprovedActionTask extends ConditionalActionTask {
    private static final Logger logger = LogManager.getLogger();

    private static final Set<ActionMode> ELIGIBLE_MODES_FOR_EXECUTION = ImmutableSet.of(
            ActionMode.MANUAL, ActionMode.EXTERNAL_APPROVAL, ActionMode.AUTOMATIC
    );

    /**
     * Action task.
     *
     * @param targetId target ID
     * @param actionList action list
     */
    public ManuallyOrExternallyApprovedActionTask(@Nonnull Long targetId, @Nonnull List<Action> actionList,
                                                  @Nonnull WorkflowStore workflowStore,
                                                  @Nonnull ActionTranslator actionTranslator,
                                                  @Nonnull ActionExecutor actionExecutor,
                                                  @Nonnull ActionExecutionListener actionExecutionListener
                               ) {
        super(targetId, actionList, workflowStore, actionTranslator, actionExecutor, actionExecutionListener);
    }

    /**
     * Returns true, if the action is manually approved we execute it regardless how long it has
     * been in the pool.
     */
    @Override
    public boolean checkIfEligibilityForExecutionExpired(@Nonnull Action action) {
        boolean result = false;
        final Optional<ActionSchedule> execScheduleOpt = action.getSchedule();

        ActionMode actionMode = action.getMode();
        if (execScheduleOpt.isPresent()) {
            if (!isExecutionWindowActive(action)) {
                result = true;
                logger.info("Accepted Action with {} mode and {}:{} ID and accepted by {} "
                    + "for schedule {}({}) won't be executed because the associated execution "
                    + "window schedule is not active; rolling it back to ACCEPTED state",
                    actionMode, action.getId(), action.getRecommendationOid(),
                    execScheduleOpt.get().getAcceptingUser(),
                    execScheduleOpt.get().getScheduleId(),
                    execScheduleOpt.get().getScheduleDisplayName());
                action.receive(new ActionEvent.RollBackToAcceptedEvent());
            } else {
                // no need to check the mode here because if it has active execution schedule
                // it is in a mode that allows execution
                logger.info(
                        "The accepted action {} will be executed because it's accepted by {}"
                        + " and the execution window {}({}) is active.",
                        action.getId(), execScheduleOpt.get().getAcceptingUser(),
                        execScheduleOpt.get().getScheduleId(),
                        execScheduleOpt.get().getScheduleDisplayName());
            }
        } else {
            if (!ELIGIBLE_MODES_FOR_EXECUTION.contains(actionMode)) {
                logger.info("Action with {} mode and {}:{} ID won't be executed"
                            + "because it's not in an eligible mode; rolling it back to READY state",
                        actionMode, action.getId(), action.getRecommendationOid());
                action.receive(new ActionEvent.RollBackToReadyEvent());
                result = true;
            } else {
                logger.info(
                    "The action {}:{} accepted by {} is eligible for execution.",
                    action.getId(), action.getRecommendationOid(), action.getExecutionAuthorizerId());
            }
        }

        return result;
    }
}

