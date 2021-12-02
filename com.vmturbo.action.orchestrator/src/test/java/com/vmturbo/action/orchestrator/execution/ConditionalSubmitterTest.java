package com.vmturbo.action.orchestrator.execution;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalFuture;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalTask;

/**
 * Test class ConditionalSubmitter.
 */
public class ConditionalSubmitterTest {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Exception rule.
     */
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    /**
     * Test that the tasks with the same condition are executed in the same order as submitted and
     * that there is a delay between executing the tasks from one group (tasks/actions with the same
     * condition).
     * In the test configuration we have 9 different tasks divided into 3 groups (3 task with the same condition per group).
     * Tasks from the one group shouldn't be executed simultaneously.
     */
    @Test
    public void testExecutionOrderAndDelay() {
        // ARRANGE
        final AtomicInteger taskIdGenerator = new AtomicInteger(1);
        int tasksCount = 3;
        int conditionsCount = 3;
        final MockScheduledExecutorService executorService = new MockScheduledExecutorService();
        final ConditionalSubmitter executor = new ConditionalSubmitter(executorService, 300);
        final Map<Integer, ConditionalFuture> futureTasks = new HashMap<>();

        // ACT 0 - submit 9 tasks/actions from 3 groups for execution
        for (int i = 0; i < tasksCount; i++) {
            for (int conditionId = 1; conditionId <= conditionsCount; conditionId++) {
                final int taskId = taskIdGenerator.getAndIncrement();
                final ConditionalFuture future = new ConditionalFuture(
                        new BaseTestTask(taskId, conditionId));
                executor.execute(future);
                futureTasks.put(taskId, future);
            }
        }

        // ASSERT 0
        // First 3 tasks(one from each group) were sent for execution and
        // other tasks were added to the queue for the later/subsequent execution
        Assert.assertEquals(3, executor.getRunningFuturesCount());
        Assert.assertEquals(6, executor.getQueuedFuturesCount());

        // ACT 1 - execute first patch of task (one task from each group)
        int executedTasks = executorService.executeSubmittedTasks();

        // ASSERT 1
        Assert.assertEquals(3, executedTasks);
        verifyCompletedAndNotCompletedTasks(futureTasks, Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5, 6, 7, 8, 9));
        Assert.assertEquals(3, executor.getRunningFuturesCount());
        Assert.assertEquals(3, executor.getQueuedFuturesCount());

        // ACT 2 - execute second patch of task (one task from each group) with delay
        executedTasks = executorService.executeScheduledTasks();

        // ASSERT 2
        Assert.assertEquals(3, executedTasks);
        verifyCompletedAndNotCompletedTasks(futureTasks, Arrays.asList(4, 5, 6),
                Arrays.asList(7, 8, 9));
        Assert.assertEquals(3, executor.getRunningFuturesCount());
        Assert.assertEquals(0, executor.getQueuedFuturesCount());

        // ACT 3 - execute third patch of tasks (one task from each group) with delay
        executedTasks = executorService.executeScheduledTasks();

        // ASSERT 3
        Assert.assertEquals(3, executedTasks);
        verifyCompletedAndNotCompletedTasks(futureTasks, Arrays.asList(7, 8, 9),
                Collections.emptyList());
        Assert.assertEquals(0, executor.getRunningFuturesCount());
        Assert.assertEquals(0, executor.getQueuedFuturesCount());
    }

    private void verifyCompletedAndNotCompletedTasks(@Nonnull Map<Integer, ConditionalFuture> tasks,
            @Nonnull Collection<Integer> completedTasks,
            @Nonnull Collection<Integer> notCompletedTasks) {
        for (Integer completedTaskId : completedTasks) {
            Assert.assertTrue(tasks.get(completedTaskId).isDone());
        }

        for (Integer notCompletedTaskId : notCompletedTasks) {
            Assert.assertFalse(tasks.get(notCompletedTaskId).isDone());
        }
    }

    /**
     * Test if one of the actions from the group of related actions (with the same condition that
     * shouldn't be executed simultaneously) failed to execute, then continue execution of other
     * actions.
     *
     * @throws ExecutionException expected exception
     * @throws InterruptedException if the thread was interrupted
     */
    @Test
    public void testExceptions() throws ExecutionException, InterruptedException {
        // ARRANGE
        final MockScheduledExecutorService executorService = new MockScheduledExecutorService();
        final ConditionalSubmitter executor = new ConditionalSubmitter(executorService, 300);
        // ACT 0 - submit 2 tasks for execution
        final ConditionalFuture future1 = new ConditionalFuture(new ExceptionTask(1, 1));
        final ConditionalFuture future2 = new ConditionalFuture(new ExceptionTask(2, 1));
        executor.execute(future1);
        executor.execute(future2);

        // ASSERT 0 - first task was sent for execution and second was added to the queue for the later/subsequent execution
        Assert.assertEquals(1, executor.getQueuedFuturesCount());
        Assert.assertEquals(1, executor.getRunningFuturesCount());

        // ACT 1 - executing the first task
        final int executedTasks = executorService.executeSubmittedTasks();

        // ASSERT 0
        Assert.assertEquals(1, executedTasks);
        Assert.assertEquals(0, executor.getQueuedFuturesCount());
        Assert.assertEquals(1, executor.getRunningFuturesCount());
        exceptionRule.expect(ExecutionException.class);
        future1.get();

        // ACT 2 - executing the second task with a delay
        final int executedTasksWithDelay = executorService.executeScheduledTasks();
        // ASSERT 2
        Assert.assertEquals(1, executedTasksWithDelay);
        Assert.assertEquals(0, executor.getQueuedFuturesCount());
        Assert.assertEquals(0, executor.getRunningFuturesCount());
    }

    /**
     * Task that throws exception.
     */
    private static class ExceptionTask extends BaseTestTask implements ConditionalTask {

        ExceptionTask(int taskId, int conditionId) {
            super(taskId, conditionId);
        }

        @Override
        public ExceptionTask call() throws Exception {
            throw new NullPointerException();
        }

        @Override
        public int compareTo(ConditionalTask o) {
            return this.getConditionId() - ((ExceptionTask)o).getConditionId();
        }

        @Override
        public List<Action> getActionList() {
            return Collections.emptyList();
        }
    }

    /**
     * Test task with an integer condition.
     */
    private static class BaseTestTask implements ConditionalTask {

        private final int taskId;
        private final int conditionId;

        /**
         * Test task.
         *
         * @param taskId task ID
         * @param conditionId condition ID
         */
        BaseTestTask(int taskId, int conditionId) {
            this.taskId = taskId;
            this.conditionId = conditionId;
        }

        /**
         * Condition ID.
         *
         * @return condition ID
         */
        public int getConditionId() {
            return conditionId;
        }

        @Override
        public String toString() {
            return "Task [taskId=" + taskId + ", conditionId=" + conditionId + "]";
        }

        @Override
        public List<Action> getActionList() {
            return Collections.emptyList();
        }

        @Override
        public int compareTo(ConditionalTask o) {
            return this.getConditionId() - ((BaseTestTask)o).getConditionId();
        }

        @Override
        public ConditionalTask call() throws Exception {
            logger.info("Executed: {}", this);
            return this;
        }
    }
}
