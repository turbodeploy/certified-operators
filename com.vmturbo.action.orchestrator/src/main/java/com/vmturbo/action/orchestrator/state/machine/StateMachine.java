package com.vmturbo.action.orchestrator.state.machine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;

/**
 * A state machine that transitions between states based on events.
 * Construct a {@link StateMachine} using the associated {@link Builder}.
 * <p/>
 * Callbacks can be added to be called before and after a transition is taken.
 * Listeners can also be added that will be called when receiving events.
 * <p/>
 * {@link TransitionGuard}s can be applied to check if a transition should be taken when
 * an event is received.
 * <p/>
 * Permits self-transitions.
 * <p/>
 * Threading model:
 * {@link this#receive(StateMachineEvent)}:
 * Attempts to ask a {@link StateMachine} to {@link #receive(StateMachineEvent)} block until ongoing
 * event processing completes. All transition guards, onEnter, onExit, and eventListener callbacks
 * must return control before a transition is considered complete and the next event may be processed.
 *
 * {@link this#getState()}: Calls made to this method concurrent to an in-progress transition
 * may return the state from the transition being applied prior to the transition formally completing.
 * Note that if a transition is in-progress and {@link this#getState()} returns the state after the transition,
 * that means that any applicable transition guard has accepted the transition and the new state is
 * the correct one on transition completion, but perhaps some onEnter or eventListener callbacks are
 * still being executed.
 *
 * If this is not the desired behavior when obtaining the state, callers may block until any in-progress
 * transition completes by calling {@link this#getStateBlocking()}
 *
 * Callbacks and ordering:
 * beforeListeners are called first, and are called regardless of whether or not the transition will be taken.
 * {@link TransitionGuard}s are called next, again before the transition is taken. If any guard returns false,
 * the transition is not taken.
 * afterListeners are next, only when the transition is actually taken.
 * eventListeners are called after all other callbacks are called, regardless of the transition result.
 *
 * Do not attempt to trigger a state transition from within a {@link StateMachine} callback of any sort.
 * Doing so would could violate the guarantees about what the state of the {@link StateMachine}
 * will be at the time the callback is made, so a {@link TransitionInProgressException} will be thrown if
 * a callback makes such an attempt. Instead consider using {@link TransitionResult#andThen} or
 * {@link TransitionResult#orElse} and sending an appropriate follow-up event after the prior transition
 * has completed.
 *
 * Things that make sense to do in a callback: state transition side-effects that CANNOT result in further
 * state transitions. Examples: log a message, tick the 'lastUpdatedTime' field, etc.
 *
 * Things that SHOULD NOT be done in a callback: state transition side-effects that CAN result in further
 * state transitions. Example: Call out to a service where the call can fail which should result in a transition
 * to a failure state.
 *
 * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
 *                Use of an enum to represent the set of possible states is encouraged.
 * @param <EVENT> A class of events handled by the state machine. All valid event types to be processed
 *                by the {@link StateMachine} should inherit from this common class.
 *                <p/>
 *                Events are dispatched by their {@link Class}. Consider for example, a CarEngine state machine
 *                that has states STOPPED and RUNNING. The EVENT superclass might be an EngineEvent, with
 *                SPECIFIC_EVENT subclasses IgnitionEvent (STOPPED -> RUNNING) and
 *                TurnOffEvent (RUNNING -> STOPPED). You could construct this example StateMachine as follows:
 *                <p/>
 *                StateMachine.<EngineState, EngineEvent>.newBuilder(EngineState.STOPPED)
 *                    .addTransition(from(EngineState.STOPPED).to(EngineState.RUNNING).onEvent(IgnitionEvent.class))
 *                    .addTransition(from(EngineState.RUNNING).to(EngineState.STOPPED).onEvent(TurnOffEvent.class))
 *                    .build();
 */
@ThreadSafe
public class StateMachine<STATE, EVENT extends StateMachineEvent> {
    /**
     * Force currentState to be read from main memory rather than cache to guarantee a transition
     * triggered by one thread influences calls to getState.
     */
    private volatile Node<STATE, EVENT> currentState;
    private List<EventListener<STATE, EVENT>> eventListeners;

    /**
     * A member that tracks an event in progress. Used to prevent transitions while a transition associated with
     * another event is also in progress. Used to protect guarantees made about callback order.
     */
    private Optional<EVENT> inProgressEvent;

    /**
     * A listener to be called after the state machine receives an event.
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    @FunctionalInterface
    public interface EventListener<STATE, EVENT extends StateMachineEvent> {
        /**
         * A listener to be called after receiving all state machine events.
         *
         * @param preState The state prior to the event being received.
         * @param postState The state after the event being received.
         * @param event The event that was received.
         * @param performedTransition Whether a transition was actually performed.
         *                            It is possible for preState.equals(postState) == true && performedTransition == true
         *                            (on a self-transition), but it is not possible for
         *                            preState.equals(postState) == false && performedTransition == true
         */
        void onEvent(@Nonnull final STATE preState,
                     @Nonnull final STATE postState,
                     @Nonnull final EVENT event,
                     boolean performedTransition);
    }

    /**
     * A guard to check if a transition should be performed.
     * If the guard returns true, the state transition will be performed.
     * If the guard returns false, the state transition will NOT be performed.
     *
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    @FunctionalInterface
    public interface TransitionGuard<EVENT extends StateMachineEvent> {
        boolean shouldTransition(EVENT event);
    }

    /**
     * A TransitionListener will listen be called before or after a transition is taken. It receives the event
     * that triggered the transition.
     *
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    @FunctionalInterface
    public interface TransitionListener<EVENT extends StateMachineEvent> {
        void onTransition(EVENT event);
    }

    /**
     * Create a new state machine. It has private access because it
     * should not be called directly. Use the related builder instead.
     *
     * @param initialState The initial state for the state machine.
     * @param listeners The event listeners to call on receiving events.
     */
    private StateMachine(@Nonnull final Node<STATE, EVENT> initialState,
                         @Nonnull final List<EventListener<STATE, EVENT>> listeners) {
        this.currentState = Objects.requireNonNull(initialState);
        this.eventListeners = Objects.requireNonNull(listeners);
        this.inProgressEvent = Optional.empty();
    }

    /**
     * Receive an event and apply the appropriate transition.
     *
     * @param event The event the state machine should receive
     * @return The result of the transition attempted upon receiving the event.
     * @throws UnexpectedEventException If there is no transition associated with the event for the current state.
     * @throws TransitionInProgressException If this method is called from within the callback for another transition.
     */
    public synchronized TransitionResult<STATE> receive(EVENT event) {
        return currentState.getTransition(event)
            .map(transition -> applyTransition(transition, event))
            .orElseThrow(() -> new UnexpectedEventException(currentState.getState(), event));
    }

    /**
     * Get the current state. Does NOT block on any in-progress transitions.
     *
     * @return The current state of the state machine
     */
    public STATE getState() {
        return currentState.getState();
    }

    /**
     * Get the current state. Blocks until any in-progress transition completes.
     *
     * @return The current state of the state machine
     */
    public synchronized STATE getStateBlocking() {
        return currentState.getState();
    }

    /**
     * Apply a transition to a new state if the transition's guard permits.
     * If the transition is not guarded, always perform the transition.
     *
     * @param transition The state to transition to.
     * @param event The event that triggered the transition.
     * @return The result of attempting the transition.
     * @throws TransitionInProgressException if a transition is already in progress.
     */
    private TransitionResult<STATE> applyTransition(@Nonnull final Transition<STATE, EVENT> transition,
                                  @Nonnull final EVENT event) {
        Objects.requireNonNull(transition);
        beginTransition(event);

        try {
            boolean shouldTransition = transition.getGuard()
                .map(guard -> guard.shouldTransition(event))
                .orElse(true);

            STATE preState = currentState.getState();
            if (shouldTransition) {
                transition.before(event);
                currentState = transition.getDestination().get();
                transition.after(event);
            }

            eventListeners.forEach(
                listener -> listener.onEvent(preState, currentState.getState(), event, shouldTransition)
            );

            return new TransitionResult<>(preState, currentState.getState(), shouldTransition);
        } finally {
            // All callbacks are complete. Mark the transition as complete.
            endTransition();
        }
    }

    public boolean isInFinalState() {
        return currentState.transitions.isEmpty();
    }

    /**
     * Mark the beginning of a transition operation..
     *
     * @param receivedEvent The event received to begin the transition.
     * @throws TransitionInProgressException if a transition is already in progress.
     */
    private void beginTransition(@Nonnull final EVENT receivedEvent) {
        inProgressEvent.ifPresent(inProgress -> {
            throw new TransitionInProgressException(getState(), inProgress, receivedEvent);
        });

        inProgressEvent = Optional.of(receivedEvent);
    }

    /**
     * Mark the ending of a transition. This allows a new transition to begin without throwing
     * a {@link TransitionInProgressException}
     */
    private void endTransition() {
        inProgressEvent = Optional.empty();
     }

    @Override
    public String toString() {
        return "StateMachine{" + "currentState=" + currentState + ", inProgressEvent="
                + inProgressEvent.map(Object::toString).orElse("<none>") + '}';
    }

    /**
     * A node in the graph of states and transitions in the state machine.
     * Package-private because this class should not be used outside of the StateMachine itself and associated builders.
     *
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    static class Node<STATE, EVENT extends StateMachineEvent> {
        private final Map<Class<? extends EVENT>, Transition<STATE, EVENT>> transitions;
        private final STATE state;

        /**
         * Create a new state machine node. It has package-private access because it
         * should not be called directly. Use the related builder instead.
         *
         * @param state The state of this node.
         */
        Node(@Nonnull final STATE state) {
            this.state = Objects.requireNonNull(state);
            transitions = new HashMap<>();
        }

        @Nonnull
        public STATE getState() {
            return state;
        }

        Optional<Transition<STATE, EVENT>> getTransition(@Nonnull final EVENT event) {
            Objects.requireNonNull(event);
            return Optional.ofNullable(transitions.get(event.getClass()));
        }

        /**
         * Add a transition from this state to the destination on the given event.
         * If the guard is present, check it before applying the transition. That is,
         * when this state receives the event, first check the guard method to see if
         * the transition should be performed. If the guard returns true, perform the
         * transition, otherwise do not perform the transition.
         *
         * @param eventClass The class of event on which to transition from this state to the destination state.
         * @param transition The transition to add.
         */
        void addTransition(Class<? extends EVENT> eventClass,
                                  @Nonnull final Transition<STATE, EVENT> transition) {
            transitions.put(eventClass, transition);
        }

        @Override
        public String toString() {
            return state + "(" + transitions.size() + " transitions)";
        }
    }

    public static <STATE, EVENT extends StateMachineEvent>
    Builder<STATE, EVENT> newBuilder(@Nonnull final STATE initialState) {
        return new Builder<>(initialState);
    }

    /**
     * A builder for state machines.
     *
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    public static class Builder<STATE, EVENT extends StateMachineEvent> {
        private final Map<STATE, Node<STATE, EVENT>> nodes;
        private final Node<STATE, EVENT> initialState;
        private List<EventListener<STATE, EVENT>> listeners;

        /**
         * Create a new {@link Builder} to build a {@link StateMachine}.
         *
         * @param initialState the initial state of the state machine
         */
        private Builder(@Nonnull final STATE initialState) {
            Objects.requireNonNull(initialState);

            nodes = new HashMap<>();
            listeners = new ArrayList<>();
            this.initialState = new Node<>(initialState);
            nodes.put(initialState, this.initialState);
        }

        /**
         * Create a new state machine using the configuration supplied to the builder.
         *
         * @return A state machine capturing the configuration supplied to the builder.
         */
        @Nonnull
        public StateMachine<STATE, EVENT> build() {
            return new StateMachine<>(initialState, listeners);
        }

        /**
         * Configure a possible state transition in the state machine.
         *
         * @param transitionBuilder A builder for the transition to add.
         * @param <SPECIFIC_EVENT> The specific event that will trigger this transition.
         * @return {@link this} to support method chaining.
         */
        public <SPECIFIC_EVENT extends EVENT> Builder<STATE, EVENT> addTransition(
            @Nonnull final Transition.Builder<STATE, EVENT, SPECIFIC_EVENT> transitionBuilder) {

            Objects.requireNonNull(transitionBuilder);

            Transition<STATE, EVENT> transition = transitionBuilder.build(nodes);
            nodes.put(transition.getSource().getState(), transition.getSource());
            return this;
        }

        /**
         * Add a listener to be called when the state machine receives any event.
         *
         * @param listener The listener to be called on all events.
         * @return {@link this} for method chaining.
         */
        public Builder<STATE, EVENT> addEventListener(
            @Nonnull final EventListener<STATE, EVENT> listener) {
            listeners.add(listener);
            return this;
        }
    }
}
