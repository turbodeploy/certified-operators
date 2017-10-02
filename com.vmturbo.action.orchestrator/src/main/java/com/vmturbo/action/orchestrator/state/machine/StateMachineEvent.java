package com.vmturbo.action.orchestrator.state.machine;

/**
 * An event that can trigger a transition in a {@link StateMachine}.
 * {@link StateMachine}s dispatch on events by their type.
 *
 * Contains a method to get the event's name.
 */
public interface StateMachineEvent {
    /**
     * Get the name of the event. By default this is the event class's simple name.
     *
     * @return The name of the event.
     */
    default String getEventName() {
        return getClass().getSimpleName();
    }
}
