package com.vmturbo.action.orchestrator.execution;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
     * Test that the tasks with the same condition are executed in the same
     * order as submitted.
     */
    @Test
    public void testExecutionOrder() {
        int tasksCount = 100;
        int conditionsCount = 3;
        CountDownLatch countDownLatch = new CountDownLatch(tasksCount * conditionsCount);
        try (ConditionalSubmitter executor = new CountDownSubmitter(4, countDownLatch)) {
            List<ConditionalFuture> futures = new ArrayList<>();

            for (int i = 0; i < tasksCount; i++) {
                for (int conditionId = 1; conditionId <= conditionsCount; conditionId++) {
                    ConditionalFuture future = new ConditionalFuture(
                            new TimingTask(conditionId * 100 + i, conditionId));
                    executor.execute(future);
                    futures.add(future);
                }
            }

            // Wait for all tasks to be executed
            countDownLatch.await();

            for (ConditionalFuture future1 : futures) {
                for (ConditionalFuture future2 : futures) {
                    TimingTask task1 = ((TimingTask)future1.getOriginalTask());
                    TimingTask task2 = ((TimingTask)future2.getOriginalTask());

                    if (task1.getConditionId() != task2.getConditionId()) {
                        continue;
                    }

                    if (task1.getTaskId() == task2.getTaskId()) {
                        continue;
                    }

                    if (task1.getTaskId() > task2.getTaskId()) {
                        Assert.assertTrue(task1.getStartTime().compareTo(task2.getEndTime()) >= 0);
                    }

                    if (task2.getTaskId() > task1.getTaskId()) {
                        Assert.assertTrue(task2.getStartTime().compareTo(task1.getEndTime()) >= 0);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.error("Failed test", e);
        }
    }

    /**
     * Test exceptions in tasks.
     *
     * @throws ExecutionException expected exception
     */
    @Test
    public void testExceptions() throws ExecutionException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (ConditionalSubmitter executor = new CountDownSubmitter(2, countDownLatch)) {
            ConditionalFuture future = new ConditionalFuture(new ExceptionTask(1, 1));
            executor.execute(future);
            countDownLatch.await();

            exceptionRule.expect(ExecutionException.class);
            future.get();

            Assert.assertEquals(0, executor.getRunningFuturesCount());
            Assert.assertEquals(0, executor.getQueuedFuturesCount());
        } catch (IOException | InterruptedException e) {
            logger.error("Failed test", e);
        }
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
     * Execute multiple tasks with multiple conditions.
     */
    @Test
    public void testConditionalExecutorService() {
        Set<TimingTask> tasks = new HashSet<>();

        int conditionsCount = 10;
        int tasksCount = 10;
        int totalTaskCount = tasksCount * conditionsCount;
        CountDownLatch countDownLatch = new CountDownLatch(totalTaskCount);

        for (int conditionId = 1; conditionId <= conditionsCount; conditionId++) {
            for (int i = 0; i < tasksCount; i++) {
                tasks.add(new TimingTask(conditionId * 100 + i, conditionId));
            }
        }

        try (ConditionalSubmitter executor = new CountDownSubmitter(5, countDownLatch)) {
            List<ConditionalFuture> futures = new ArrayList<>();

            for (TimingTask task : tasks) {
                ConditionalFuture future = new ConditionalFuture(task);
                executor.execute(future);
                futures.add(future);
            }

            Assert.assertEquals(totalTaskCount, futures.size());

            // Wait for all tasks to be executed
            countDownLatch.await();

            Assert.assertEquals(0, executor.getRunningFuturesCount());
            Assert.assertEquals(0, executor.getQueuedFuturesCount());

            // Check that the tasks with the same condition were executed one
            // at a time
            for (TimingTask task1 : tasks) {
                Assert.assertEquals(1, task1.getCallCount());

                for (TimingTask task2 : tasks) {
                    if (task1.getTaskId() == task2.getTaskId()) {
                        continue;
                    }

                    if (task1.getConditionId() != task2.getConditionId()) {
                        continue;
                    }

                    Assert.assertTrue("Task 1: " + task1 + ", Task 2: " + task2,
                            task1.getStartTime().compareTo(task2.getEndTime()) >= 0
                                    || task2.getStartTime().compareTo(task1.getEndTime()) >= 0);
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.error("Failed test", e);
        }
    }

    /**
     * Test task with an integer condition.
     */
    private static class TimingTask extends BaseTestTask implements ConditionalTask {

        private Instant startTime;
        private Instant endTime;
        private int callCount = 0;

        TimingTask(int taskId, int conditionId) {
            super(taskId, conditionId);
        }

        /**
         * Task execution logic.
         */
        @Override
        public TimingTask call() throws Exception {
            startTime = Instant.now();
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                logger.error("Interrupted " + this, e);
                throw new InterruptedException();
            }

            callCount++;
            endTime = Instant.now();
            logger.info("Executed: {}", this);
            return this;
        }

        @Override
        public int compareTo(ConditionalTask o) {
            return this.getConditionId() - ((TimingTask)o).getConditionId();
        }

        /**
         * Task start time.
         *
         * @return start time
         */
        public Instant getStartTime() {
            return startTime;
        }

        /**
         * Task end time.
         *
         * @return end time
         */
        public Instant getEndTime() {
            return endTime;
        }

        public int getCallCount() {
            return callCount;
        }

        @Override
        public String toString() {
            return "Task [taskId=" + getTaskId() + ", conditionId=" + getConditionId()
                    + ", startTime=" + startTime + ", endTime=" + endTime + ", callCount="
                    + callCount + "]";
        }

        @Override
        public List<Action> getActionList() {
            return Collections.emptyList();
        }
    }

    /**
     * Test task with an integer condition.
     */
    private static class BaseTestTask {

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
         * Task ID.
         *
         * @return task ID
         */
        public int getTaskId() {
            return taskId;
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
    }

    /**
     * ConditionalSubmitter that tracks the number of executed calls.
     */
    public static class CountDownSubmitter extends ConditionalSubmitter {

        private final CountDownLatch countDownLatch;

        CountDownSubmitter(int poolSize, CountDownLatch countDownLatch) {
            super(poolSize, new ThreadFactoryBuilder().setNameFormat("auto-act-exec-%d").build(),
                    0);
            this.countDownLatch = countDownLatch;
        }

        @Override
        protected void onFutureCompletion(@Nonnull Runnable r, @Nullable Throwable t) {
            super.onFutureCompletion(r, t);
            countDownLatch.countDown();
        }
    }
}
