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
        final ActionExecution execution1 = store.createExecution(ImmutableList.of(1L, 2L),
                Collections.emptyList());
        checkThatExecutionIsStored(execution1, store);

        // Create the second execution and check that it is stored
        final ActionExecution execution2 = store.createExecution(ImmutableList.of(3L, 4L),
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
        final ActionExecution execution = store.createExecution(ImmutableList.of(1L, 2L),
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
        final long actionId1 = 1L;
        final long actionId2 = 2L;
        final long executionId = store.createExecution(ImmutableList.of(actionId1, actionId2),
                Collections.emptyList()).getId();

        // The first action is completed: execution should contain a single action now
        store.removeCompletedAction(actionId1);
        final Optional<ActionExecution> execution = store.getActionExecution(executionId);
        Assert.assertTrue(execution.isPresent());
        final Set<Long> actionIds = new HashSet<>(execution.get().getActionIdList());
        Assert.assertEquals(ImmutableSet.of(actionId2), actionIds);

        // The second action is completed: execution should be removed
        store.removeCompletedAction(actionId2);
        Assert.assertFalse(store.getActionExecution(executionId).isPresent());
    }

    private static void checkThatExecutionIsStored(
            final ActionExecution execution,
            final ActionExecutionStore store) {
        final Optional<ActionExecution> executionOpt = store.getActionExecution(
                execution.getId());
        Assert.assertTrue(executionOpt.isPresent());
        Assert.assertEquals(execution, executionOpt.get());
    }
}
