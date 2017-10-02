package com.vmturbo.action.orchestrator.state.machine;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.state.machine.StateMachine.EventListener;
import com.vmturbo.action.orchestrator.state.machine.StateMachine.TransitionGuard;
import com.vmturbo.action.orchestrator.state.machine.StateMachine.TransitionListener;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;

/**
 * Tests for {@link StateMachine}.
 */
public class StateMachineTest {
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

    /** Test event class. */
    private class BazEvent implements StateMachineEvent { }

    private static Transition.SourceBuilder<States, StateMachineEvent> from(@Nonnull final States initialState) {
        return Transition.from(initialState);
    }

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testInitialState() {
        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST).build();
        assertEquals(States.FIRST, machine.getState());
    }

    @Test
    public void testTransition() {
        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class))
            .build();
        assertEquals(States.SECOND, machine.receive(new FooEvent()).getAfterState());
    }

    @Test
    public void testSelfTransition() {
        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.FIRST).onEvent(FooEvent.class))
            .build();
        assertEquals(States.FIRST, machine.receive(new FooEvent()).getAfterState());
    }

    @Test
    public void testBefore() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).before(before))
            .build();

        FooEvent event = new FooEvent();
        machine.receive(event);
        Mockito.verify(before).onTransition(event);
    }

    @Test
    public void testInitialStateDoesNotTriggerBefore() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).before(before))
            .build();

        Mockito.verify(before, never()).onTransition(any());
    }

    @Test
    public void testAfter() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> after =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).after(after))
            .build();

        FooEvent event = new FooEvent();
        machine.receive(event);
        Mockito.verify(after).onTransition(event);
    }

    @Test
    public void testBeforeCalledEarlierThanAfter() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> after =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).after(after).before(before))
            .build();

        FooEvent event = new FooEvent();
        machine.receive(event);

        //create inOrder object passing any mocks that need to be verified in order
        InOrder order = inOrder(before, after);
        order.verify(before).onTransition(event);
        order.verify(after).onTransition(event);
    }

    @Test
    public void testListenersCalledOnSelfTransition() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> after =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.FIRST).onEvent(FooEvent.class).before(before).after(after))
            .build();

        FooEvent event = new FooEvent();
        machine.receive(event);
        Mockito.verify(before).onTransition(event);
        Mockito.verify(after).onTransition(event);
    }

    @Test
    public void testGuardPreventsTransition() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> after =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        TransitionGuard<FooEvent> guard = (event) -> false;
        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST)
                    .to(States.SECOND)
                    .onEvent(FooEvent.class)
                    .guardedBy(guard)
                    .before(before)
                    .after(before)
            ).build();

        assertEquals(States.FIRST, machine.getState());
        assertEquals(States.FIRST, machine.receive(new FooEvent()).getAfterState());
        Mockito.verify(before, never()).onTransition(any());
        Mockito.verify(after, never()).onTransition(any());
    }

    @Test
    public void testGuardAllowsTransition() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> onExit =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);

        TransitionGuard<FooEvent> guard = (event) -> true;
        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST)
                    .to(States.SECOND)
                    .onEvent(FooEvent.class)
                    .guardedBy(guard)
                    .before(before)
                    .after(onExit)
            ).build();

        FooEvent event = new FooEvent();
        assertEquals(States.FIRST, machine.getState());
        assertEquals(States.SECOND, machine.receive(event).getAfterState());
        Mockito.verify(before).onTransition(event);
        Mockito.verify(onExit).onTransition(event);
    }

    @Test
    public void testEventListenerCalledOnTransition() {
        @SuppressWarnings("unchecked")
        EventListener<States, StateMachineEvent> listener =
            (EventListener<States, StateMachineEvent>)Mockito.mock(EventListener.class);

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class))
            .addEventListener(listener)
            .build();

        FooEvent event = new FooEvent();
        machine.receive(event);
        Mockito.verify(listener).onEvent(States.FIRST, States.SECOND, event, true);
    }

    @Test
    public void testEventListenerCalledOnNoTransition() {
        @SuppressWarnings("unchecked")
        EventListener<States, StateMachineEvent> listener =
            (EventListener<States, StateMachineEvent>)Mockito.mock(EventListener.class);
        TransitionGuard<FooEvent> guard = (event) -> false;

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).guardedBy(guard))
            .addEventListener(listener)
            .build();

        FooEvent event = new FooEvent();
        machine.receive(event);
        Mockito.verify(listener).onEvent(States.FIRST, States.FIRST, event, false);
    }

    @Test
    public void testMultipleTransitions() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> beforeFirst =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        @SuppressWarnings("unchecked")
        TransitionListener<BarEvent> beforeSecond =
            (TransitionListener<BarEvent>)Mockito.mock(TransitionListener.class);
        @SuppressWarnings("unchecked")
        TransitionListener<BazEvent> beforeThird =
            (TransitionListener<BazEvent>)Mockito.mock(TransitionListener.class);

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).before(beforeFirst))
            .addTransition(from(States.SECOND).to(States.THIRD).onEvent(BarEvent.class).before(beforeSecond))
            .addTransition(from(States.THIRD).to(States.FIRST).onEvent(BazEvent.class).before(beforeThird))
            .build();

        final FooEvent fooEvent = new FooEvent();
        final BarEvent barEvent = new BarEvent();
        final BazEvent bazEvent = new BazEvent();

        assertEquals(States.FIRST, machine.getState());
        assertEquals(States.SECOND, machine.receive(fooEvent).getAfterState());
        assertEquals(States.THIRD, machine.receive(barEvent).getAfterState());
        assertEquals(States.FIRST, machine.receive(bazEvent).getAfterState());

        Mockito.verify(beforeFirst).onTransition(fooEvent);
        Mockito.verify(beforeSecond).onTransition(barEvent);
        Mockito.verify(beforeThird).onTransition(bazEvent);
    }

    @Test
    public void testIllegalTransitionThrowsUnexpectedEventException() {
        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .build();

        expectedException.expect(UnexpectedEventException.class);
        expectedException.expectMessage("State FIRST received event FooEvent for which it has no transition.");

        machine.receive(new FooEvent());
    }

    @Test
    public void testExceptionThrownByListener() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        doThrow(new RuntimeException("error")).when(before).onTransition(any(FooEvent.class));

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).before(before))
            .build();

        expectedException.expect(RuntimeException.class);
        machine.receive(new FooEvent());
    }

    @Test
    public void testStateMachineAllowsTransitionsAfterAnException() {
        @SuppressWarnings("unchecked")
        TransitionListener<FooEvent> before =
            (TransitionListener<FooEvent>)Mockito.mock(TransitionListener.class);
        doThrow(new RuntimeException("error")).when(before).onTransition(any(FooEvent.class));

        StateMachine<States, StateMachineEvent> machine = StateMachine.newBuilder(States.FIRST)
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(FooEvent.class).before(before))
            .addTransition(from(States.FIRST).to(States.SECOND).onEvent(BarEvent.class))
            .build();

        expectedException.expect(RuntimeException.class);
        machine.receive(new FooEvent());
        assertEquals(States.FIRST, machine.getState());

        machine.receive(new BarEvent());
        assertEquals(States.SECOND, machine.getState());
    }

    @Test
    public void testSendingEventWithinCallbackNotAllowed() {
        class StateMachineContainer {
            private final StateMachine<States, StateMachineEvent> machine;

            StateMachineContainer() {
                this.machine = StateMachine.newBuilder(States.FIRST)
                    .addTransition(from(States.FIRST)
                        .to(States.SECOND)
                        .onEvent(FooEvent.class)
                        // Illegal! Do not send an event from within the transition callback for another event.
                        .after(fooEvent -> sendBarEvent()))
                    .addTransition(from(States.SECOND)
                        .to(States.THIRD)
                        .onEvent(BarEvent.class)
                    ).build();
            }

            private void sendBarEvent() {
                machine.receive(new BarEvent());
            }

            StateMachine<States, StateMachineEvent> getMachine() {
                return machine;
            }
        }

        expectedException.expect(TransitionInProgressException.class);
        expectedException.expectMessage(
            "StateMachine received event BarEvent while processing transition for event FooEvent in state SECOND");
        new StateMachineContainer().getMachine().receive(new FooEvent());
    }
}
