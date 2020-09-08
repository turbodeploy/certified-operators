package com.vmturbo.action.orchestrator.state.machine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.action.orchestrator.state.machine.StateMachine.Node;
import com.vmturbo.action.orchestrator.state.machine.StateMachine.TransitionGuard;
import com.vmturbo.action.orchestrator.state.machine.Transition.Builder;
import com.vmturbo.action.orchestrator.state.machine.Transition.RequiringEventBuilder;

/**
 * Tests for {@link Builder}.
 */
public class TransitionBuilderTest {
    /**
     * States.
     */
    private enum States {
        FIRST,
        SECOND,
        THIRD
    }

    /** Test event class. */
    private class FooEvent implements StateMachineEvent { }

    /** Test event class. */
    private class BarEvent implements StateMachineEvent { }

    @Test
    public void testTransitionFrom() {
        assertEquals(
            States.FIRST,
            Transition.from(States.FIRST).getSource()
        );
    }

    @Test
    public void testTransitionFromTo() {
        RequiringEventBuilder<States, StateMachineEvent> builder = Transition.from(States.THIRD).to(States.FIRST);
        assertEquals(States.THIRD, builder.getSource());
        assertEquals(States.FIRST, builder.getDestination().get());
    }

    @Test
    public void testNoGuard() {
        Builder<States, StateMachineEvent, FooEvent> builder = Transition.from(States.THIRD)
            .to(States.FIRST)
            .onEvent(FooEvent.class);

        assertEquals(States.THIRD, builder.getSource());
        assertEquals(States.FIRST, builder.getDestination().get());
        assertEquals(FooEvent.class, builder.getEventClass());
        assertNull(builder.getGuard());
    }

    @Test
    public void testWithGuard() {
        TransitionGuard<FooEvent> guard = (event) -> true;
        Builder<States, StateMachineEvent, FooEvent> builder = Transition.from(States.THIRD)
            .to(States.FIRST)
            .onEvent(FooEvent.class)
            .guardedBy(guard);

        assertEquals(States.THIRD, builder.getSource());
        assertEquals(States.FIRST, builder.getDestination().get());
        assertEquals(FooEvent.class, builder.getEventClass());
        assertEquals(guard, builder.getGuard());
    }

    @Test
    public void testSettingGuardMultipleTimesOverrides() {
        TransitionGuard<FooEvent> trueGuard = (event) -> true;
        TransitionGuard<FooEvent> falseGuard = (event) -> false;

        Builder<States, StateMachineEvent, FooEvent> builder = Transition.from(States.THIRD)
            .to(States.FIRST)
            .onEvent(FooEvent.class)
            .guardedBy(trueGuard)
            .guardedBy(falseGuard);

        assertEquals(States.THIRD, builder.getSource());
        assertEquals(States.FIRST, builder.getDestination().get());
        assertEquals(FooEvent.class, builder.getEventClass());
        assertEquals(falseGuard, builder.getGuard());
    }

    @Test
    public void testBuildNodesNotPresent() {
        Map<States, Node<States, StateMachineEvent>> nodes = new HashMap<>();
        TransitionGuard<FooEvent> guard = (event) -> true;

        Transition<States, StateMachineEvent> transition = Transition.from(States.THIRD)
            .to(States.FIRST)
            .onEvent(FooEvent.class)
            .guardedBy(guard)
            .build(nodes);

        assertEquals(States.THIRD, transition.getSource().getState());
        assertEquals(States.FIRST, transition.getDestination().get().getState());
        assertEquals(
            transition.getDestination(),
            transition.getSource().getTransition(new FooEvent()).get().getDestination()
        );
        assertEquals(
            guard,
            transition.getSource().getTransition(new FooEvent()).get().getGuard().get()
        );
    }

    @Test
    public void testBuildSourcePresent() {
        Map<States, Node<States, StateMachineEvent>> nodes = new HashMap<>();
        Node<States, StateMachineEvent> expectedSource = new Node<>(States.FIRST);
        nodes.put(expectedSource.getState(), expectedSource);

        Transition<States, StateMachineEvent> transition = Transition.from(expectedSource.getState())
            .to(States.SECOND)
            .onEvent(BarEvent.class)
            .build(nodes);

        assertEquals(expectedSource, transition.getSource());
    }

    @Test
    public void testBuildDestinationPresent() {
        Map<States, Node<States, StateMachineEvent>> nodes = new HashMap<>();
        Node<States, StateMachineEvent> expectedDestination = new Node<>(States.SECOND);
        nodes.put(expectedDestination.getState(), expectedDestination);

        Transition<States, StateMachineEvent> transition = Transition.from(States.FIRST)
            .to(expectedDestination.getState())
            .onEvent(BarEvent.class)
            .build(nodes);

        assertEquals(expectedDestination, transition.getDestination().get());
    }

    @Test
    public void testBuildWithSelfEdge() {
        Map<States, Node<States, StateMachineEvent>> nodes = new HashMap<>();
        Node<States, StateMachineEvent> expected = new Node<>(States.FIRST);
        nodes.put(expected.getState(), expected);

        Transition<States, StateMachineEvent> transition = Transition.from(expected.getState())
            .to(expected.getState())
            .onEvent(BarEvent.class)
            .build(nodes);

        assertEquals(expected, transition.getSource());
        assertEquals(expected, transition.getDestination().get());
    }
}
