package com.vmturbo.action.orchestrator.state.machine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import com.vmturbo.action.orchestrator.state.machine.StateMachine.Node;

/**
 * Tests for {@link Node}.
 */
public class StateMachineNodeTest {
    /**
     * States.
     */
    enum States {
        FIRST,
        SECOND
    }

    /**
     * Test event class.
     */
    private class FooEvent implements StateMachineEvent { }

    /**
     * Test event class.
     */
    private class BarEvent implements StateMachineEvent { }

    @Test
    public void testGetState() {
        assertEquals(
            States.FIRST,
            new Node<>(States.FIRST).getState()
        );
    }

    @Test
    public void testGetTransition() {
        Transition<States, StateMachineEvent> transition = Transition.from(States.FIRST)
            .to(States.SECOND)
            .onEvent(FooEvent.class)
            .build(new HashMap<>());

        Node<States, StateMachineEvent> node = transition.getSource();
        assertTrue(node.getTransition(new FooEvent()).isPresent());
        assertFalse(node.getTransition(new BarEvent()).isPresent());
    }
}
