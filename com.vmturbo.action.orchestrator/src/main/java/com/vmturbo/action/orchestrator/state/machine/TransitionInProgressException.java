package com.vmturbo.action.orchestrator.state.machine;

import javax.annotation.Nonnull;

/**
 * Thrown when a {@link StateMachine} client attempts to send the state machine an event
 * while the state machine from within the callback for a transition.
 */
public class TransitionInProgressException extends RuntimeException {
    private final StateMachineEvent processingEvent;
    private final StateMachineEvent receivedEvent;
    private final Object state;

    /**
     * Create a new TransitionInProgressException.
     *
     * @param state The state of the {@link StateMachine }at the time the TransitionInProgressException was applied.
     * @param processingEvent The event being processed at the time another event was sent to the {@link StateMachine}.
     * @param receivedEvent The event sent to the {@link StateMachine} from within a callback for another event transition.
     */
    public TransitionInProgressException(@Nonnull final Object state,
                                         @Nonnull final StateMachineEvent processingEvent,
                                         @Nonnull final StateMachineEvent receivedEvent) {
        this.state = state;
        this.processingEvent = processingEvent;
        this.receivedEvent = receivedEvent;
    }

    @Override
    public String getMessage() {
        return "StateMachine received event " + receivedEvent.getEventName() +
            " while processing transition for event " + processingEvent.getEventName() +
            " in state " + state;
    }

    /**
     * The event being processed at the time another event was sent to the {@link StateMachine}.
     *
     * @return The event being processed at the time another event was sent to the {@link StateMachine}.
     */
    public StateMachineEvent getProcessingEvent() {
        return processingEvent;
    }

    /**
     * The event sent to the {@link StateMachine} from within a callback for another event transition.
     *
     * @return The event sent to the {@link StateMachine} from within a callback for another event transition.
     */
    public StateMachineEvent getReceivedEvent() {
        return receivedEvent;
    }

    /**
     * Get the state to which the unknown event was applied.
     *
     * @return The state on which the unexpected event was applied
     */
    public Object getState() {
        return state;
    }
}