package com.vmturbo.action.orchestrator.state.machine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.state.machine.StateMachine.Node;
import com.vmturbo.action.orchestrator.state.machine.StateMachine.TransitionGuard;
import com.vmturbo.action.orchestrator.state.machine.StateMachine.TransitionListener;

/**
 * Represents a transition to a destination node when a particular eventType is received.
 * The guard can be used to protect the transition.
 *
 * Callbacks can be added to be called before and after the transition has been taken. These
 * callbacks will only be called if the transition will actually be taken (ie the before
 * callbacks will never be called if the transition guard fails.)
 *
 * All {@link #before(StateMachineEvent)} callbacks are guaranteed to be called
 * prior to all {@link #after(StateMachineEvent)} callbacks.
 *
 * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
 *                Use of an enum to represent the set of possible states is encouraged.
 * @param <EVENT> A class of events handled by the state machine. All valid event types to be processed
 *                by the {@link StateMachine} should inherit from this common class.
 *                Events are dispatched by their {@link Class}.
 */
public class Transition<STATE, EVENT extends StateMachineEvent> {
    private final Node<STATE, EVENT> source;
    private final Supplier<Node<STATE, EVENT>> destination;
    private final Optional<TransitionGuard<EVENT>> guard;
    private final List<TransitionListener<EVENT>> beforeListeners;
    private final List<TransitionListener<EVENT>> afterListeners;

    /**
     * Create a new {@link Transition}. Do not call directly, instead create
     * and add transitions through the associated {@link Builder}.
     *
     * @param source The source node for the transition.
     * @param destination The destination node.
     * @param guard The guard to apply.
     * @param beforeListeners A list of listeners to be called before the transition is taken.
     * @param afterListeners A list of listeners to be called after the transition is taken.
     */
    Transition(@Nonnull final Node<STATE, EVENT> source,
               @Nonnull final Supplier<Node<STATE, EVENT>> destination,
               @Nullable final TransitionGuard<EVENT> guard,
               @Nonnull final List<TransitionListener<EVENT>> beforeListeners,
               @Nonnull final List<TransitionListener<EVENT>> afterListeners) {
        this.source = Objects.requireNonNull(source);
        this.destination = Objects.requireNonNull(destination);
        this.guard = Optional.ofNullable(guard);
        this.beforeListeners = beforeListeners;
        this.afterListeners = afterListeners;
    }

    /**
     * A static factory method. Called as the entry point into the chain of builders used to build
     * up a {@link Transition} instance.
     *
     * @param source The source state for the transition.
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     * @return A {@link SourceBuilder} to be used to build a transition.
     */
    public static <STATE, EVENT extends StateMachineEvent>
    SourceBuilder<STATE, EVENT> from(@Nonnull final STATE source) {
        return new SourceBuilder<>(source);
    }

    Node<STATE, EVENT> getSource() {
        return source;
    }

    Supplier<Node<STATE, EVENT>> getDestination() {
        return destination;
    }

    Optional<TransitionGuard<EVENT>> getGuard() {
        return guard;
    }

    /**
     * Call the callbacks intended to be called prior to the transition being taken.
     *
     * @param event The event that triggered the transition.
     */
    void before(@Nonnull final EVENT event) {
        Objects.requireNonNull(event);
        beforeListeners.forEach(listener -> listener.onTransition(event));
    }

    /**
     * Call the callbacks intended to be called after the transition has been taken.
     *
     * @param event The event that triggered the transition.
     */
    void after(@Nonnull final EVENT event) {
        Objects.requireNonNull(event);
        afterListeners.forEach(listener -> listener.onTransition(event));
    }

    @Override
    public String toString() {
        return source.getState() + "->" + destination.get().getState();
    }

    /**
     * A builder for transitions that captures the source state.
     * Destination and eventType information must be configured to generate a valid transition.
     *
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    public static class SourceBuilder<STATE, EVENT extends StateMachineEvent> {
        private final STATE source;

        SourceBuilder(@Nonnull final STATE source) {
            this.source = Objects.requireNonNull(source);
        }

        /**
         * Configure the destination state to transition to the from the source state.
         *
         * @param destination The destination state that will be transitioned to from the source state.
         * @return A {@link RequiringEventBuilder} that should be configured with an eventType.
         */
        public RequiringEventBuilder<STATE, EVENT> to(@Nonnull final STATE destination) {
            return new RequiringEventBuilder<>(source, () -> destination);
        }

        /**
         * Configure the destination state supplier that will be transitioned from the source state.
         *
         * @param destination destination supplier. Will be calculated at the point in time
         *         when transition si applied
         * @return A {@link RequiringEventBuilder} that should be configured with an eventType.
         */
        public RequiringEventBuilder<STATE, EVENT> to(@Nonnull final Supplier<STATE> destination) {
            return new RequiringEventBuilder<>(source, destination);
        }

        STATE getSource() {
            return source;
        }
    }

    /**
     * A builder for transitions that captures the source and destination states.
     * Event information must be configured to generate a valid transition.
     *
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     */
    public static class RequiringEventBuilder<STATE, EVENT extends StateMachineEvent> {
        private final STATE source;
        private final Supplier<STATE> destination;

        private RequiringEventBuilder(final STATE source, final Supplier<STATE> destination) {
            Objects.requireNonNull(source);
            Objects.requireNonNull(destination);

            this.source = source;
            this.destination = destination;
        }

        /**
         * Configure the source state to transition to the destination state when presented
         * with the input eventType.
         *
         * @param eventType The eventType on which to perform the transition.
         * @param <SPECIFIC_EVENT> A specific subclass of EVENT on which this transition will be triggered.
         *                         Consider for example, a CarEngine state machine that has states
         *                         STOPPED and RUNNING. The EVENT superclass might be an EngineEvent, with
         *                         SPECIFIC_EVENT subclasses IgnitionEvent (STOPPED -> RUNNING)
         *                         and TurnOffEvent (RUNNING -> STOPPED)
         * @return A {@link Builder} for building events.
         */
        public <SPECIFIC_EVENT extends EVENT> Builder<STATE, EVENT, SPECIFIC_EVENT> onEvent(Class<SPECIFIC_EVENT> eventType) {
            return new Builder<>(source, destination, eventType);
        }

        STATE getSource() {
            return source;
        }

        Supplier<STATE> getDestination() {
            return destination;
        }

        @Override
        public String toString() {
            return source + "->" + destination.get();
        }
    }

    /**
     * A builder for possible transitions in a {@link StateMachine}.
     *
     * The {@link SPECIFIC_EVENT} parameter is used to provide type-safe dispatch on specific EVENT subclasses
     * to the {@link TransitionGuard} and {@link TransitionListener} callbacks. This permits guard and listener
     * callbacks to receive SPECIFIC_EVENT types without having to cast the general EVENT type into the
     * SPECIFIC_EVENT type they wish to operate on.
     *
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     * @param <EVENT> A class of events handled by the state machine. Dispatched by the class of the event.
     * @param <SPECIFIC_EVENT> A specific subclass of EVENT on which the transition this builder is building
     *                         will be triggered. Consider for example, a CarEngine state machine that has states
     *                         STOPPED and RUNNING. The EVENT superclass might be an EngineEvent, with
     *                         SPECIFIC_EVENT subclasses IgnitionEvent (STOPPED -> RUNNING)
     *                         and TurnOffEvent (RUNNING -> STOPPED)
     */
    public static class Builder<STATE, EVENT extends StateMachineEvent, SPECIFIC_EVENT extends EVENT> {
        private final STATE source;
        private final Supplier<STATE> destination;
        private final Class<SPECIFIC_EVENT> eventClass;
        private TransitionGuard<SPECIFIC_EVENT> guard;
        private Supplier<Boolean> condition;
        private final List<TransitionListener<EVENT>> beforeListeners;
        private final List<TransitionListener<EVENT>> afterListeners;

        /**
         * Create a {@link Builder} used to build a transition in the state machine.
         * Cannot be directly invoked. Instead use the {@link Transition#from}
         *  and related sub-builders to build up a Builder.
         *
         * @param source The source state to transition from.
         * @param destination The destination state to transition to.
         * @param eventClass The class of event on which the transition should be performed.
         */
        private Builder(@Nonnull final STATE source,
                        @Nonnull final Supplier<STATE> destination,
                        @Nonnull final Class<SPECIFIC_EVENT> eventClass) {
            this.source = Objects.requireNonNull(source);
            this.destination = Objects.requireNonNull(destination);
            this.eventClass = Objects.requireNonNull(eventClass);
            this.guard = null;

            this.beforeListeners = new ArrayList<>();
            this.afterListeners = new ArrayList<>();
        }

        /**
         * Apply a guard to the transition. The guard method will be called when the eventType is received
         * to check if the transition should actually be applied. If the guard method returns true,
         * the transition will be taken. If the guard method returns false, the transition will NOT be taken.
         * By default, no guard is applied. Call this method to add a guard.
         *
         * @param guard The guard method to check if a transition should be taken.
         * @return {@link this} to support method chaining.
         */
        public Builder<STATE, EVENT, SPECIFIC_EVENT> guardedBy(@Nonnull final TransitionGuard<SPECIFIC_EVENT> guard) {
            this.guard = Objects.requireNonNull(guard);
            return this;
        }

        /**
         * Add a listener to be called before a given transition is performed
         * (ie calls to the {@link StateMachine#getState()} method will return the transition's
         * <strong>from</strong> state rather than its to state.
         *
         * @param beforeCallback The method to be called before taking a given transition.
         * @return {@link this} for method chaining.
         */
        public Builder<STATE, EVENT, SPECIFIC_EVENT> before(
            @Nonnull final TransitionListener<SPECIFIC_EVENT> beforeCallback) {

            /**
             * While this sort of cast is generally unsafe to do, because of the invariants
             * provided by the builders and how transitions are looked up by class, we can guarantee
             * this will not lead to runtime errors.
             */
            @SuppressWarnings("unchecked")
            TransitionListener<EVENT> castBeforeCallback =
                (TransitionListener<EVENT>)Objects.requireNonNull(beforeCallback);

            beforeListeners.add(castBeforeCallback);
            return this;
        }

        /**
         * Add a listener to be called after a given transition is performed
         * (ie calls to the getState() method will return the transition's <strong>to</strong> state
         * rather than its from state.
         *
         * @param afterCallback The method to be called when the given state is exited.
         * @return {@link this} for method chaining.
         */
        public Builder<STATE, EVENT, SPECIFIC_EVENT> after(
            @Nonnull final TransitionListener<SPECIFIC_EVENT> afterCallback) {

            /**
             * While this sort of cast is generally unsafe to do, because of the invariants
             * provided by the builders and how transitions are looked up by class, we can guarantee
             * this will not lead to runtime errors.
             */
            @SuppressWarnings("unchecked")
            TransitionListener<EVENT> castAfterCallback =
                (TransitionListener<EVENT>)Objects.requireNonNull(afterCallback);

            afterListeners.add(castAfterCallback);
            return this;
        }

        STATE getSource() {
            return source;
        }

        Supplier<STATE> getDestination() {
            return destination;
        }

        Class<? extends EVENT> getEventClass() {
            return eventClass;
        }

        TransitionGuard<SPECIFIC_EVENT> getGuard() {
            return guard;
        }

        /**
         * Build a transition connecting the configured source to the configured destination
         * along the configured eventType. If the source and destination node are already connected
         * along the eventType, that setting will be overridden.
         *
         * @param nodes The known nodes. If the source and destination states already have a node,
         *              that node is retrieved and used in the transition. If they are not known
         *              in the map of nodes, new nodes are created but they are NOT inserted into
         *              the list of known nodes.
         * @return A transition edge connecting the configured source to the configured destination
         *         along the configured eventType.
         */
        Transition<STATE, EVENT> build(@Nonnull final Map<STATE, Node<STATE, EVENT>> nodes) {
            Node<STATE, EVENT> sourceNode = nodes.get(source);

            if (sourceNode == null) {
                sourceNode = new Node<>(source);
            }
            final Supplier<Node<STATE, EVENT>> destinationNode =
                    () -> nodes.computeIfAbsent(destination.get(), Node::new);

            /**
             * While this sort of cast is generally unsafe to do, because of the invariants
             * provided by the builders and how transitions are looked up by class, we can guarantee
             * this will not lead to runtime errors.
             */
            @SuppressWarnings("unchecked")
            Transition<STATE, EVENT> transition =
                new Transition<>(sourceNode,
                    destinationNode,
                    (TransitionGuard<EVENT>)guard,
                    beforeListeners,
                    afterListeners);

            sourceNode.addTransition(eventClass, transition);
            return transition;
        }
    }

    /**
     * Expresses the result of a transition. Note that if a StateMachine is given an invalid event,
     * rather than returning a {@link TransitionResult}, it will throw an
     * {@link UnexpectedEventException}.
     *
     * @param <STATE> The class of states possible in the state machine. MUST support #hashCode() and #equals().
     *                Use of an enum to represent the set of possible states is encouraged.
     */
    public static class TransitionResult<STATE> {
        private final STATE beforeState;
        private final STATE afterState;
        private final boolean transitionTaken;

        /**
         * Create a new TransitionResult.
         *
         * @param beforeState The state before the transition caused by the event received by the state machine..
         * @param afterState The state after the transition caused by the event received by the state machine..
         * @param transitionTaken Whether or not the transition associated with th event was actually taken.
         */
        public TransitionResult(@Nonnull final STATE beforeState,
                         @Nonnull final STATE afterState,
                         boolean transitionTaken) {
            this.beforeState = beforeState;
            this.afterState = afterState;
            this.transitionTaken = transitionTaken;
        }

        public STATE getBeforeState() {
            return beforeState;
        }

        public STATE getAfterState() {
            return afterState;
        }

        public boolean transitionTaken() {
            return transitionTaken;
        }

        public boolean transitionNotTaken() {
            return !transitionTaken();
        }
    }
}