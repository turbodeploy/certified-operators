package com.vmturbo.action.orchestrator.execution;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalTask;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * Execution task with the condition defined in
 * {@link #compareTo(ConditionalTask)}. No two tasks with the same condition
 * can be executed at the same time.
 */
public class AutomatedActionTask extends ConditionalActionTask {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Action task.
     *
     * @param targetId target ID
     * @param actionList action list
     */
    public AutomatedActionTask(@Nonnull Long targetId, @Nonnull List<Action> actionList,
                               @Nonnull WorkflowStore workflowStore,
                               @Nonnull ActionTranslator actionTranslator,
                               @Nonnull ActionExecutor actionExecutor,
                               @Nonnull ActionExecutionListener actionExecutionListener
                               ) {
        super(targetId, actionList, workflowStore, actionTranslator, actionExecutor, actionExecutionListener);
    }

    /**
     * Return true if the input action has been rolled back to a previous state, false otherwise.
     * The action roll back can happen in the following situations:
     * - when the action execution schedule is not active, a roll back from QUEUED to ACCEPTED state
     * will happen when the action mode is set MANUAL or EXTERNAL_APPROVAL. For all other action
     * modes, the action will be rolled back from QUEUED to READY state.
     * - when the action execution schedule is active, action mode is AUTOMATIC and the action doesn't
     * have user acceptance, the action will be rolled back from QUEUED to READY state.
     *
     * @param action The input action that can be rolled backed to a previous state.
     * @return true if the input action has been rolled back to a previous state, false otherwise.
     */
    @Override
    public boolean checkIfEligibilityForExecutionExpired(@Nonnull Action action) {
        boolean result = false;
        final Optional<ActionSchedule> execScheduleOpt = action.getSchedule();

        ActionDTO.ActionMode actionMode = action.getMode();
        if (execScheduleOpt.isPresent()) {
            if (!isExecutionWindowActive(action)) {
                result = true;
                if ((actionMode == ActionDTO.ActionMode.MANUAL || actionMode == ActionDTO.ActionMode.EXTERNAL_APPROVAL)
                        && execScheduleOpt.get().getAcceptingUser() != null) {
                    // Rollback action from QUEUED to ACCEPTED state because of
                    // a missing execution window
                    logger.debug("Action with {} mode and {} ID won't be executed because the associated "
                                    + "execution window is not active; rolling it back to ACCEPTED state",
                            actionMode, action.getId());
                    action.receive(new ActionEvent.RollBackToAcceptedEvent());
                } else {
                    logger.debug("Action with {} mode and {} ID won't be executed because the associated "
                                    + "execution window is not active; rolling it back to READY state",
                            actionMode, action.getId());
                    action.receive(new ActionEvent.RollBackToReadyEvent());
                }
            } else {
                if (actionMode != ActionDTO.ActionMode.AUTOMATIC && execScheduleOpt.get().getAcceptingUser() == null) {
                    logger.debug("Action with {} mode and {} ID won't be executed because it's not automated "
                                    + "and doesn't have user acceptance; rolling it back to READY state",
                            actionMode, action.getId());
                    action.receive(new ActionEvent.RollBackToReadyEvent());
                    result = true;
                } else {
                    logger.debug(
                            "The automated action {} will be executed because it's accepted by user",
                            action.getId());
                }
            }
        } else {
            if (actionMode != ActionDTO.ActionMode.AUTOMATIC) {
                logger.debug("Action with {} mode and {} ID won't be executed during the execution window "
                                + "because it's not automated; rolling it back to READY state",
                        actionMode, action.getId());
                action.receive(new ActionEvent.RollBackToReadyEvent());
                result = true;
            } else {
                logger.debug(
                        "The automated action {} will be executed during its execution window",
                        action.getId());
            }
        }

        return result;
    }
}

