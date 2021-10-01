package com.vmturbo.action.orchestrator.action;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceRemovalEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.CannotExecuteEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionRemovalEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.state.machine.StateMachine;
import com.vmturbo.action.orchestrator.state.machine.Transition;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * A state machine for use in tracking action states.
 */
public class ActionStateMachine {

    private ActionStateMachine() {}

    /**
     * Generate a new state machine for an action. The state machine looks like:
     * <pre>
     *          READY ----------> CLEARED
     *           ^  ^               ^^^
     *           |  |               |||
     *           |  v               |||
     *           |  REJECTED ------- ||
     *           v                   ||
     *        ACCEPTED---------------||
     *           ^                    |
     *           |                    |
     *           v                    |
     *        QUEUED------------------
     *           |
     *           v
     *        PRE_IN_PROGRESS---------------------------------
     *           |                                           |
     *           V                                           |
     *   -----IN_PROGRESS----------------------              |
     *  |  ___/  |                             |             |
     *  | |      v                             v             |
     *  | |   POST_IN_PROGRESS -----         FAILING         |
     *  | |      |                  |           |            |
     *  | |      v                  |           v            |
     *  |  -->SUCCEEDED              -------> FAILED <-------
     *  |                                       ^
     *  |                                       |
     *   ---------------------------------------
     * </pre>
     *
     * <p>Transitions from READY -> QUEUED are guarded by checks that verify the action
     * is in an appropriate mode that allows such a transition.</p>
     *
     * @param action The action to create the state machine for.
     * @param currentState The current state of the action.
     * @param actionEventListeners listeners of action events received by the action state machine.
     * @return A state machine for the action.
     */
    public static StateMachine<ActionState, ActionEvent> newInstance(
            @Nonnull final Action action,
            @Nonnull final ActionState currentState,
            @Nonnull List<ActionEventListener> actionEventListeners) {

        Objects.requireNonNull(action);

        StateMachine.Builder<ActionState, ActionEvent> builder = StateMachine.<ActionState, ActionEvent>newBuilder(currentState)
            // The legal state transitions
            .addTransition(from(ActionState.READY).to(ActionState.CLEARED)
                .onEvent(NotRecommendedEvent.class)
                .after(action::onActionCleared))
            .addTransition(from(ActionState.READY).to(ActionState.CLEARED)
                .onEvent(CannotExecuteEvent.class)
                .after(action::onActionCleared))
            .addTransition(from(ActionState.READY).to(ActionState.ACCEPTED)
                .onEvent(ManualAcceptanceEvent.class)
                .guardedBy(action::acceptanceGuard)
                .after(action::onActionAccepted))
            .addTransition(from(ActionState.READY).to(ActionState.ACCEPTED)
                .onEvent(AutomaticAcceptanceEvent.class)
                .guardedBy(action::acceptanceGuard)
                .after(action::onActionAccepted))
            .addTransition(from(ActionState.READY).to(ActionState.REJECTED)
                .onEvent(RejectionEvent.class)
                .after(action::onActionRejected))

            .addTransition(from(ActionState.REJECTED).to(ActionState.READY)
                .onEvent(RejectionRemovalEvent.class)
                .after(action::onRejectionRemoved))
            .addTransition(from(ActionState.REJECTED).to(ActionState.CLEARED)
                .onEvent(NotRecommendedEvent.class)
                .after(action::onActionCleared))

            .addTransition(from(ActionState.ACCEPTED).to(ActionState.QUEUED)
                .onEvent(QueuedEvent.class)
                .after(action::onActionQueued))
            .addTransition(from(ActionState.ACCEPTED).to(ActionState.READY)
                .onEvent(AcceptanceRemovalEvent.class)
                .after(action::onAcceptanceRemoved))
            .addTransition(from(ActionState.ACCEPTED).to(ActionState.CLEARED)
                .onEvent(NotRecommendedEvent.class)
                .after(action::onActionCleared))

            .addTransition(from(ActionState.QUEUED).to(action::getExecutionState)
                .onEvent(BeginExecutionEvent.class)
                .after(action::onActionPrepare))
            .addTransition(from(ActionState.QUEUED).to(ActionState.CLEARED)
                .onEvent(NotRecommendedEvent.class)
                .after(action::onActionCleared))
            .addTransition(from(ActionState.QUEUED).to(ActionState.ACCEPTED)
                .onEvent(RollBackToAcceptedEvent.class)
                .after(action::onActionRemovedFromQueue))

            // Handle progress events while in PRE
            .addTransition(from(ActionState.PRE_IN_PROGRESS).to(ActionState.PRE_IN_PROGRESS)
                .onEvent(ProgressEvent.class)
                .after(action::onActionProgress))
            // Transition from PRE_IN_PROGRESS to IN_PROGRESS when no PRE workflow is required
            // If a pre workflow is in progress, this will not transition.
            .addTransition(from(ActionState.PRE_IN_PROGRESS).to(ActionState.IN_PROGRESS)
                .onEvent(BeginExecutionEvent.class)
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
            .addTransition(from(ActionState.IN_PROGRESS).to(
                    () -> action.getPostExecutionStep(
                            ActionState.POST_IN_PROGRESS,
                            ActionState.SUCCEEDED))
                .onEvent(SuccessEvent.class)
                .after(action::onActionPostSuccess))
            .addTransition(from(ActionState.IN_PROGRESS).to(
                    () -> action.getPostExecutionStep(
                            ActionState.FAILING,
                            ActionState.FAILED))
                .onEvent(FailureEvent.class)
                .after(action::onActionPostFailure))

            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.POST_IN_PROGRESS)
                .onEvent(ProgressEvent.class)
                .after(action::onActionProgress))
            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.FAILED)
                .onEvent(FailureEvent.class)
                .after(action::onActionFailure))
            .addTransition(from(ActionState.POST_IN_PROGRESS).to(ActionState.SUCCEEDED)
                .onEvent(SuccessEvent.class)
                .after(action::onActionSuccess))

            .addTransition(from(ActionState.FAILING).to(ActionState.FAILING)
                .onEvent(ProgressEvent.class)
                .after(action::onActionProgress))
            .addTransition(from(ActionState.FAILING).to(ActionState.FAILED)
                .onEvent(FailureEvent.class)
                .after(action::onActionFailure))
            .addTransition(from(ActionState.FAILING).to(ActionState.FAILED)
                .onEvent(SuccessEvent.class)
                .after(action::onActionSuccess));

        for (final ActionEventListener actionEventListener : actionEventListeners) {
            builder.addEventListener((preState, postState, event, performedTransition) ->
                actionEventListener.onActionEvent(action, preState, postState, event, performedTransition));
        }

        return builder.build();
    }

    /**
     * Create a new instance of an action state machine in the initial state.
     * See {@link ActionStateMachine#newInstance(Action, ActionState, List)}.
     *
     * @param action The action whose state machine should be created.
     * @param actionEventListeners listeners of action events received by the action state machine.
     * @return A new state machine for the action.
     */
    public static StateMachine<ActionState, ActionEvent> newInstance(
            @Nonnull final Action action,
            @Nonnull List<ActionEventListener> actionEventListeners) {
        return newInstance(action, ActionState.READY, actionEventListeners);
    }

    private static Transition.SourceBuilder<ActionState, ActionEvent> from(
        @Nonnull final ActionState initialState) {
        return Transition.from(initialState);
    }

    /**
     * Interface used for registering an object as a listener of the action state machine.
     *
     * <p>Previously, new features would add themselves to ActionStateUpdater, creating this
     * monster that monitors action step state changes from TP and then translating that into the
     * business logic depending on if there's another action step. Instead of repeating that,
     * ActionStateMachine already knows the next state. Use this listener for ActionOrchestrator's
     * version of the action state, which is more complete than TopologyProcessor's.</p>
     *
     * <p><b>WARNING:</b> Do not change the state of the action within the listener. This will result in
     * unexpected behavior that is difficult to debug. We intentionally provide the ActionView to
     * try to prevent you from changing the action state.</p>
     *
     * <p><b>WARNING:</b> Do not perform blocking calls like writing to the database as this will prevent
     * the action state from progressing. Instead place blocking calls in an Executor service.</p>
     */
    @FunctionalInterface
    public interface ActionEventListener {

        /**
         * This method will be called when an action event is received by the action state machine.
         *
         * @param actionView A reference to a read only version of the action.
         * @param preState The state prior to the event being received.
         * @param postState The state after the event being received.
         * @param event The event that was received.
         * @param performedTransition Whether a transition was actually performed.
         *                            It is possible for preState.equals(postState) == true && performedTransition == true
         *                            (on a self-transition), but it is not possible for
         *                            preState.equals(postState) == false && performedTransition == true
         */
        void onActionEvent(
                @Nonnull ActionView actionView,
                @Nonnull ActionState preState,
                @Nonnull ActionState postState,
                @Nonnull ActionEvent event,
                boolean performedTransition);
    }
}
