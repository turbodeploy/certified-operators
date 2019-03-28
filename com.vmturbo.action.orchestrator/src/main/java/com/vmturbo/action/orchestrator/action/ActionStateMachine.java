package com.vmturbo.action.orchestrator.action;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.CannotExecuteEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AfterFailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AfterSuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.state.machine.StateMachine;
import com.vmturbo.action.orchestrator.state.machine.Transition;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * A state machine for use in tracking action states.
 */
public class ActionStateMachine {

    private static final Logger logger = LogManager.getLogger();

    private ActionStateMachine() {}

    /**
     * Generate a new state machine for an action. The state machine looks like:
     *
     * READY
     *   |-> CLEARED
     *   |      ^
     *   |      |
     *   |-> QUEUED |---> PRE_IN_PROGRESS ---> IN_PROGRESS ----> POST_IN_PROGRESS
     *                      |                                     |
     *                      |                                     |-------> SUCCEEDED
     *                      |                                     |
     *                      |------------------------------------>|-------> FAILED
     *
     * Transitions from READY -> QUEUED are guarded by checks that verify the action
     * is in an appropriate mode that allows such a transition.
     *
     * @param action The action to create the state machine for.
     * @param currentState The current state of the action.
     * @return A state machine for the action.
     */
    public static StateMachine<ActionState, ActionEvent> newInstance(
                                @Nonnull final Action action,
                                @Nonnull final ActionState currentState) {

        Objects.requireNonNull(action);
        final long actionId = action.getId();

        return StateMachine.<ActionState, ActionEvent>newBuilder(currentState)
            // The legal state transitions
            .addTransition(from(ActionState.READY).to(ActionState.CLEARED)
                .onEvent(NotRecommendedEvent.class)
                .after(action::onActionCleared))
            .addTransition(from(ActionState.READY).to(ActionState.CLEARED)
                .onEvent(CannotExecuteEvent.class)
                .after(action::onActionCleared))

            .addTransition(from(ActionState.READY).to(ActionState.QUEUED)
                .onEvent(ManualAcceptanceEvent.class)
                .guardedBy(action::acceptanceGuard)
                .after(action::onActionAccepted))
            .addTransition(from(ActionState.READY).to(ActionState.QUEUED)
                .onEvent(AutomaticAcceptanceEvent.class)
                .guardedBy(action::acceptanceGuard)
                .after(action::onActionAccepted))

            .addTransition(from(ActionState.QUEUED).to(ActionState.PRE_IN_PROGRESS)
                .onEvent(PrepareExecutionEvent.class)
                .after(action::onActionPrepare))
            .addTransition(from(ActionState.QUEUED).to(ActionState.FAILED)
                .onEvent(FailureEvent.class)
                .after(action::onActionFailure))
            .addTransition(from(ActionState.QUEUED).to(ActionState.CLEARED)
                .onEvent(NotRecommendedEvent.class)
                .after(action::onActionCleared))

            // Handle progress events while in PRE
            .addTransition(from(ActionState.PRE_IN_PROGRESS).to(ActionState.PRE_IN_PROGRESS)
                .onEvent(ProgressEvent.class)
                .after(action::onActionProgress))

            // Transition from PRE_IN_PROGRESS to IN_PROGRESS when no PRE workflow is required
            // If a pre workflow is in progress, this will not transition.
            .addTransition(from(ActionState.PRE_IN_PROGRESS).to(ActionState.IN_PROGRESS)
                .onEvent(BeginExecutionEvent.class)
                // Guard against attempts to begin action execution while a PRE workflow
                // is still running.
                .guardedBy(action::executionGuard)
                .after(action::onActionStart))
            // Transition from PRE_IN_PROGRESS to IN_PROGRESS when a PRE workflow completes successfully
            .addTransition(from(ActionState.PRE_IN_PROGRESS).to(ActionState.IN_PROGRESS)
                .onEvent(SuccessEvent.class)
                .after(action::onPreExecuted))
            .addTransition(from(ActionState.PRE_IN_PROGRESS).to(ActionState.FAILED)
                .onEvent(FailureEvent.class)
                .after(action::onActionFailure))

            .addTransition(from(ActionState.IN_PROGRESS).to(ActionState.IN_PROGRESS)
                .onEvent(ProgressEvent.class)
                .after(action::onActionProgress))

            .addTransition(from(ActionState.IN_PROGRESS).to(ActionState.POST_IN_PROGRESS)
                .onEvent(SuccessEvent.class)
                .after(action::onActionPostSuccess))

            .addTransition(from(ActionState.IN_PROGRESS).to(ActionState.POST_IN_PROGRESS)
                .onEvent(FailureEvent.class)
                .after(action::onActionPostFailure))

            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.POST_IN_PROGRESS)
                .onEvent(ProgressEvent.class)
                .after(action::onActionProgress))

            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.SUCCEEDED)
                .onEvent(AfterSuccessEvent.class)
                // Guard against attempts to complete action execution while a POST workflow
                // still needs to be run.
                .guardedBy(action::completionGuard)
                .after(action::onActionSuccess))

            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.FAILED)
                .onEvent(AfterFailureEvent.class)
                // Guard against attempts to complete action execution while a POST workflow
                // still needs to be run.
                .guardedBy(action::completionGuard)
                .after(action::onActionFailure))

            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.SUCCEEDED)
                .onEvent(SuccessEvent.class)
                .guardedBy(action::successGuard)
                .after(action::onActionSuccess))

            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.FAILED)
                .onEvent(FailureEvent.class)
                .after(action::onActionFailure))

            // Called on all events
            .addEventListener((preState, postState, event, performedTransition) -> {
                onActionEvent(actionId, preState, postState, event, performedTransition);
            })

            .build();
    }

    /**
     * Create a new instance of an action state machine in the initial state.
     * See {@link ActionStateMachine#newInstance(Action, ActionState)}.
     *
     * @param action The action whose state machine should be created.
     * @return A new state machine for the action.
     */
    public static StateMachine<ActionState, ActionEvent> newInstance(@Nonnull final Action action) {
        return newInstance(action, ActionState.READY);
    }

    private static Transition.SourceBuilder<ActionState, ActionEvent> from(
        @Nonnull final ActionState initialState) {
        return Transition.from(initialState);
    }

    /**
     * Called on receiving all action events.
     * This method should record an entry in the audit log when we create an action audit log.
     *
     * @param actionId The ID of the action associated with the event.
     * @param preState The state prior to the event being received.
     * @param postState The state after the event being received.
     * @param event The event that was received.
     * @param performedTransition Whether the transition was performed.
     */
    private static void onActionEvent(long actionId,
                                      @Nonnull final ActionState preState,
                                      @Nonnull final ActionState postState,
                                      @Nonnull final ActionEvent event,
                                      boolean performedTransition) {
        logger.info("Action {} {} event {} ({} -> {})",
            actionId,
            performedTransition ? "handled" : "dropped",
            event,
            preState,
            postState
        );
    }
}
