package com.vmturbo.action.orchestrator.execution;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution.SkippedAction;

/**
 * Unit tests for {@link ActionExecutionStore}.
 */
public class ActionExecutionStoreTest {

    /**
     * Tests creating new action execution and getting it from the store.
     */
    @Test
    public void testCreateAndGetExecutions() {
        final ActionExecutionStore store = new ActionExecutionStore();

        // Create the first execution and check that it is stored
        final Action action1 = mockAction(1L, 2L);
        final Action action2 = mockAction(3L, 4L);
        final ActionExecution execution1 = store.createExecution(ImmutableList.of(action1, action2),
                Collections.emptyList());
        checkThatExecutionIsStored(execution1, store);

        // Create the second execution and check that it is stored
        final Action action3 = mockAction(5L, 6L);
        final Action action4 = mockAction(7L, 8L);
        final ActionExecution execution2 = store.createExecution(ImmutableList.of(action3, action4),
                Collections.emptyList());
        checkThatExecutionIsStored(execution2, store);

        // Check that executions have different IDs
        Assert.assertNotEquals(execution1.getId(), execution2.getId());

        // Check that executions list contains both executions and nothing else
        final Set<ActionExecution> expectedAllExecutions = ImmutableSet.of(execution1, execution2);
        final Set<ActionExecution> allExecutions = new HashSet<>(store.getAllActionExecutions());
        Assert.assertEquals(expectedAllExecutions, allExecutions);
    }

    /**
     * Tests that skipped executions are returned by {@link ActionExecutionStore#createExecution}
     * method.
     */
    @Test
    public void testSkippedActions() {
        final ActionExecutionStore store = new ActionExecutionStore();

        final List<SkippedAction> rejectedActions = ImmutableList.of(
                SkippedAction.newBuilder().setActionId(3L).build(),
                SkippedAction.newBuilder().setActionId(4L).build());
        final Action action1 = mockAction(1L, 2L);
        final Action action2 = mockAction(3L, 4L);
        final ActionExecution execution = store.createExecution(ImmutableList.of(action1, action2),
                rejectedActions);

        Assert.assertEquals(rejectedActions, execution.getSkippedActionList());
    }

    /**
     * Tests that completed actions and executions are removed from the store.
     */
    @Test
    public void testRegisterCompleted() {
        final ActionExecutionStore store = new ActionExecutionStore();

        // Create an execution consisting of 2 actions
        final long action1Id = 1L;
        final long recommendation1Id = 2L;
        final Action action1 = mockAction(action1Id, recommendation1Id);
        final long action2Id = 3L;
        final long recommendation2Id = 4L;
        final Action action2 = mockAction(action2Id, recommendation2Id);
        final long executionId = store.createExecution(ImmutableList.of(action1, action2),
                Collections.emptyList()).getId();

        // The first action is completed: execution should contain a single action now
        store.removeCompletedAction(action1Id);
        final Optional<ActionExecution> execution = store.getActionExecution(executionId);
        Assert.assertTrue(execution.isPresent());
        final Set<Long> actionIds = new HashSet<>(execution.get().getActionIdList());
        Assert.assertEquals(ImmutableSet.of(recommendation2Id), actionIds);

        // The second action is completed: execution should be removed
        store.removeCompletedAction(action2Id);
        Assert.assertFalse(store.getActionExecution(executionId).isPresent());
    }

    /**
     * Tests creating new action execution with empty action list.
     */
    @Test
    public void testCreateEmptyExecution() {
        final ActionExecutionStore store = new ActionExecutionStore();

        final ActionExecution execution = store.createExecution(Collections.emptyList(),
                Collections.emptyList());
        Assert.assertFalse(store.getActionExecution(execution.getId()).isPresent());
    }

    private static void checkThatExecutionIsStored(
            final ActionExecution execution,
            final ActionExecutionStore store) {
        final Optional<ActionExecution> executionOpt = store.getActionExecution(
                execution.getId());
        Assert.assertTrue(executionOpt.isPresent());
        Assert.assertEquals(execution, executionOpt.get());
    }

    private static Action mockAction(final long actionId, final long recommendationOid) {
        final Action action = Mockito.mock(Action.class);
        Mockito.when(action.getId()).thenReturn(actionId);
        Mockito.when(action.getRecommendationOid()).thenReturn(recommendationOid);
        return action;
    }
}
