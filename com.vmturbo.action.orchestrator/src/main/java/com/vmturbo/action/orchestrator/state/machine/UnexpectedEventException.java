package com.vmturbo.action.orchestrator.state.machine;

/**
 * Thrown when a state receives an event for which it has no transition.
 *
 * STATE and EVENT types are not parameterized because generic classes
 * are not permitted to extend Throwable.
 */
public class UnexpectedEventException extends RuntimeException {
    private final StateMachineEvent event;
    private final Object state;

    /**
     * Create a new UnexpectedEventException.
     *
     * @param state The state on which the unexpected event was applied.
     * @param event The event type of the unexpected event
     */
    public UnexpectedEventException(Object state, StateMachineEvent event) {
        this.state = state;
        this.event = event;
    }

    @Override
    public String getMessage() {
        return "State " + state + " received event " + event.getEventName() +
            " for which it has no transition.";
    }

    /**
     * Get the state to which the unknown event was applied.
     *
     * @return The state on which the unexpected event was applied
     */
    public Object getState() {
        return state;
    }

    /**
     * Get the event that was not expected by the state machine's current state.
     *
     * @return The event type of the unexpected event
     */
    public StateMachineEvent getEvent() {
        return event;
    }
}
