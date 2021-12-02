package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Scheduling service that is not scheduling any executions in fact. Instead, all the scheduled
 * executions are recorded and could be triggered all by once by using method {@link
 * #executeScheduledTasks()}. Immediate operations are submitted to internal wrapped thread pool.
 */
public class MockScheduledExecutorService implements ScheduledExecutorService {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final Collection<Runnable> scheduledTasks = new CopyOnWriteArrayList<>();
    private final Collection<Runnable> submittedTasks = new CopyOnWriteArrayList<>();

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        scheduledTasks.add(command);
        return new FakeScheduledFuture<Void>();
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        scheduledTasks.add(() -> {
            try {
                callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return new FakeScheduledFuture<V>();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
            TimeUnit unit) {
        scheduledTasks.add(command);
        return new FakeScheduledFuture<Void>();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
            long delay, TimeUnit unit) {
        scheduledTasks.add(command);
        return new FakeScheduledFuture<Void>();
    }

    @Override
    public void shutdown() {
        threadPool.shutdownNow();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return threadPool.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return threadPool.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return threadPool.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return threadPool.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        submittedTasks.add(() -> {
            try {
                task.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return new FakeFuture();
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        submittedTasks.add(task);
        return new FakeFuture();
    }

    @Override
    public Future<?> submit(Runnable task) {
        submittedTasks.add(task);
        return new FakeFuture();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return threadPool.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
            TimeUnit unit) throws InterruptedException {
        return threadPool.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        return threadPool.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return threadPool.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        submittedTasks.add(command);
    }

    /**
     * Execute already schedule tasks. All tasks will be executed one-by-one
     * for each call of this method. And after execution they will be removed.
     *
     * @return the number of scheduled tasks which were run
     */
    public int executeScheduledTasks() {
        final Collection<Runnable> scheduledTasksToExecute = new ArrayList(scheduledTasks);
        scheduledTasks.clear();

        int executedTasksCount = 0;
        for (Runnable task : scheduledTasksToExecute) {
            try {
                task.run();
                executedTasksCount++;
            } catch (Exception e) {
                logger.info("Scheduled operation failed with exception", e);
            }
        }
        return executedTasksCount;
    }

    /**
     * Execute already submitted runnable and callable tasks. All tasks will be executed one-by-one
     * for each call of this method. And after execution they will be removed.
     *
     * @return the number of submitted tasks which were run
     */
    public int executeSubmittedTasks() {
        final List<Runnable> submittedTasksToExecute = new ArrayList<>(submittedTasks);
        submittedTasks.clear();

        int executedTasksCount = 0;
        for (Runnable task : submittedTasksToExecute) {
            try {
                task.run();
                executedTasksCount++;
            } catch (Exception e) {
                logger.info("Submitted task failed with exception");
            }
        }
        return executedTasksCount;
    }

    /**
     * Fake scheduled future.
     *
     * @param <T> type of future result
     */
    private static class FakeScheduledFuture<T> extends CompletableFuture<T>
            implements ScheduledFuture<T> {

        @Override
        public long getDelay(@Nonnull TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(@Nonnull Delayed o) {
            return 0;
        }
    }

    /**
     * Fake future.
     *
     * @param <T> type of future result
     */
    private static class FakeFuture<T> implements Future<T> {

        private boolean isCancelled = false;

        private FakeFuture() {
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            isCancelled = true;
            return false;
        }

        @Override
        public boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public boolean isDone() {
            return isCancelled;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public T get(long timeout, @Nonnull TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }
}
