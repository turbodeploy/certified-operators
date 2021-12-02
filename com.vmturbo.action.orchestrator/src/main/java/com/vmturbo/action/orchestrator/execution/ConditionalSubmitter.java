package com.vmturbo.action.orchestrator.execution;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;

/**
 * Submit futures based on the condition for execution. If a future with the
 * same condition is running, queue the future. On future completion, submit a
 * future with the same condition from the queue.
 */
public class ConditionalSubmitter implements Executor, Closeable {

    private static final Logger logger = LogManager.getLogger();

    private static final int MAX_QUEUE_SIZE = 1000;

    private final ScheduledExecutorService executor;
    private final int conditionalSubmitterDelaySecs;

    /**
     * Currently executing futures.
     */
    private final Set<ConditionalFuture> runningFutures = new HashSet<>();

    /**
     * Queued futures waiting for execution based on their conditions.
     */
    private final Queue<ConditionalFuture> queuedFutures = new LinkedList<>();

    /**
     * Conditional submitter.
     *
     * @param executor scheduled executor service
     * @param conditionalSubmitterDelaySecs delay between the conditional
     *         actions
     */
    public ConditionalSubmitter(ScheduledExecutorService executor,
            int conditionalSubmitterDelaySecs) {
        this.executor = executor;
        this.conditionalSubmitterDelaySecs = conditionalSubmitterDelaySecs;
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
    }

    private void submitInternal(@Nonnull ConditionalFuture future)
            throws RejectedExecutionException {
        executor.execute(future);
        runningFutures.add(future);
    }

    private void submitInternalWithDelay(@Nonnull ConditionalFuture future)
            throws RejectedExecutionException {
        logger.info("Delaying submitting from queue {} for {} secs", future.getOriginalTask(),
                conditionalSubmitterDelaySecs);
        executor.schedule(future, conditionalSubmitterDelaySecs, TimeUnit.SECONDS);
        queuedFutures.remove(future);
        runningFutures.add(future);
    }

    /**
     * Try to submit the future for the conditional task. Queue this future, if
     * a future with the same condition is already running.
     *
     * @param command the conditional future to submit
     *
     * @throws RejectedExecutionException when executor rejected the future
     */
    @Override
    public synchronized void execute(@Nonnull Runnable command)
            throws RejectedExecutionException {
        if (!(command instanceof ConditionalFuture)) {
            logger.error("Cannot execute. Runnable is not a future. {}", command);
            return;
        }

        ConditionalFuture future = (ConditionalFuture)command;
        future.injectCallback(() -> onFutureCompletion(future));

        if (!findFuture(runningFutures, future).isPresent()) {
            logger.info("Submitting without queuing {}", future.getOriginalTask());
            submitInternal(future);
        } else {
            logger.info("Queueing {}", future.getOriginalTask());

            if (queuedFutures.size() >= MAX_QUEUE_SIZE) {
                logger.error("Queue reached the maximum size {}. Cannot add more futures.",
                        queuedFutures.size());
                return;
            }

            queuedFutures.add(future);
        }
    }

    /**
     * This callback method is executed upon the future completion.
     *
     * @param runnable completed runnable
     * @throws RejectedExecutionException when executor rejected the future
     */
    protected synchronized void onFutureCompletion(@Nonnull Runnable runnable)
            throws RejectedExecutionException {

        if (!(runnable instanceof ConditionalFuture)) {
            logger.error("Runnable is not a future. {}", runnable);
            return;
        }

        ConditionalFuture future = (ConditionalFuture)runnable;
        logger.info("Done executing {}", future.getOriginalTask());
        runningFutures.remove(future);

        submitFromQueue(future);
    }

    /**
     * Submit from the queue a new future with the same condition as the
     * given future.
     *
     * @param futureToCompare the future with the condition to compare
     *
     * @throws RejectedExecutionException when executor rejected the future
     */
    private void submitFromQueue(@Nonnull ConditionalFuture futureToCompare)
            throws RejectedExecutionException {
        Optional<ConditionalFuture> futureOpt = findFuture(queuedFutures, futureToCompare);

        if (!futureOpt.isPresent()) {
            logger.info("Done with conditions {}", futureToCompare.getOriginalTask());
            return;
        }

        ConditionalFuture future = futureOpt.get();

        if (findFuture(runningFutures, future).isPresent()) {
            logger.error(
                    "Error submitting from queue {}. Queue should be unblocked for condition {}",
                    future, futureToCompare);
            return;
        }

        submitInternalWithDelay(future);
    }

    private Optional<ConditionalFuture> findFuture(
            @Nonnull Collection<ConditionalFuture> collection, @Nonnull ConditionalFuture future) {
        return collection.stream()
                .filter(t -> t.getOriginalTask().compareTo(future.getOriginalTask()) == 0)
                .findFirst();
    }

    /**
     * Get running futures count.
     *
     * @return running futures count
     */
    public synchronized int getRunningFuturesCount() {
        logger.debug("Running features: {}", runningFutures);
        return runningFutures.size();
    }

    /**
     * Get queued futures count.
     *
     * @return queued futures count
     */
    public synchronized int getQueuedFuturesCount() {
        logger.debug("Queued features: {}", queuedFutures);
        return queuedFutures.size();
    }

    /**
     * The future that keeps the reference to the originally submitted task.
     */
    public static class ConditionalFuture extends FutureTask<ConditionalTask> {

        private final ConditionalTask originalTask;

        private volatile boolean started = false;
        private ConditionalTaskCallback callback;

        /**
         * Create conditional future.
         *
         * @param task task to be submitted
         */
        public ConditionalFuture(Callable<ConditionalTask> task) {
            super(task);
            this.originalTask = (ConditionalTask)task;
        }

        @Override
        public void run() {
            started = true;
            super.run();
            if (callback != null) {
                callback.onTaskCompleted();
            }
        }

        public boolean isStarted() {
            return started;
        }

        /**
         * Injects callback which will be called after completing the future.
         *
         * @param callback afterExecution callback
         */
        public void injectCallback(@Nonnull ConditionalTaskCallback callback) {
            this.callback = callback;
        }

        /**
         * Get original task.
         *
         * @return original task
         */
        @Nonnull
        public ConditionalTask getOriginalTask() {
            return originalTask;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " [originalTask=" + originalTask + "]";
        }
    }

    /**
     * The conditional task for submission by ConditionalSubmitter.
     */
    public interface ConditionalTask
            extends Callable<ConditionalTask>, Comparable<ConditionalTask> {

        /**
         * Get action list.
         *
         * @return action list.
         */
        List<Action> getActionList();
    }

    /**
     * The callback to execute after conditional task completed.
     */
    public interface ConditionalTaskCallback {
        /**
         * Executed when futureTask completed.
         */
        void onTaskCompleted();
    }
}
