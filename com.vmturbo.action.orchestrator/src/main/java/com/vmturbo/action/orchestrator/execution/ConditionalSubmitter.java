package com.vmturbo.action.orchestrator.execution;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    private final ThreadPoolExecutor executor;

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
     * @param poolSize thread tool size
     * @param threadFactory thread factory
     */
    public ConditionalSubmitter(int poolSize, @Nonnull ThreadFactory threadFactory) {
        this.executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory) {
            @Override
            protected void afterExecute(@Nonnull Runnable r, @Nullable Throwable t) {
                onFutureCompletion(r, t);
            }
        };
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

    /**
     * Try to submit the future for the conditional task. Queue this future, if
     * a future with the same condition is already running.
     *
     * @param command the conditional future to submit
     *
     * @throws RejectedExecutionException when executor rejected the future
     */
    @Nonnull
    @Override
    public synchronized void execute(Runnable command)
            throws RejectedExecutionException {
        if (!(command instanceof ConditionalFuture)) {
            logger.error("Cannot execute. Runnable is not a future. {}", command);
            return;
        }

        ConditionalFuture future = (ConditionalFuture)command;

        if (!findFuture(runningFutures, future).isPresent()) {
            logger.info("Submitting without queuing {}", future);
            submitInternal(future);
        } else {
            logger.info("Queueing {}", future);

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
     * @param r completed runnable
     * @param t if not null, exception that caused the task termination
     *
     * @throws RejectedExecutionException when executor rejected the future
     */
    protected synchronized void onFutureCompletion(@Nonnull Runnable r, @Nullable Throwable t)
            throws RejectedExecutionException {
        if (t != null) {
            logger.error("Error executing runnable " + r, t);
        }

        if (!(r instanceof ConditionalFuture)) {
            logger.error("Runnable is not a future. {}", r);
            return;
        }

        ConditionalFuture future = (ConditionalFuture)r;
        runningFutures.remove(future);
        submitFromQueue(future);
    }

    /**
     * Submit from the queue a new future with with the same condition as the
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

        logger.info("Submitting from queue {}", future.getOriginalTask());
        queuedFutures.remove(future);
        submitInternal(future);
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
    @Nonnull
    public synchronized int getRunningFuturesCount() {
        logger.debug("Running features: {}", runningFutures);
        return runningFutures.size();
    }

    /**
     * Get queued futures count.
     *
     * @return queued futures count
     */
    @Nonnull
    public synchronized int getQueuedFuturesCount() {
        logger.debug("Queued features: {}", queuedFutures);
        return queuedFutures.size();
    }

    /**
     * The future that keeps the reference to the originally submitted task.
     */
    public static class ConditionalFuture extends FutureTask<ConditionalTask> {

        private final ConditionalTask originalTask;

        /**
         * Create conditional future.
         *
         * @param task task to be submitted
         */
        public ConditionalFuture(Callable<ConditionalTask> task) {
            super(task);
            this.originalTask = (ConditionalTask)task;
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
         * Get action.
         *
         * @return action
         */
        Action getAction();
    }
}
