package com.vmturbo.topology.processor.actions;

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
public class MockScheduledService implements ScheduledExecutorService {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final Collection<Runnable> scheduledTasks = new CopyOnWriteArrayList<>();
    private final Collection<Runnable> runnableTasks = new CopyOnWriteArrayList<>();
    private final Collection<Callable> callableTasks = new CopyOnWriteArrayList<>();

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
        callableTasks.add(task);
        return new FakeFuture();
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        runnableTasks.add(task);
        return new FakeFuture();
    }

    @Override
    public Future<?> submit(Runnable task) {
        runnableTasks.add(task);
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
        runnableTasks.add(command);
    }

    /**
     * Executes all the scheduled operations. There is no real schedule implemented in this class.
     * All the scheduled tasks are just executed one-by-one for each call of this method.
     */
    public void executeScheduledTasks() {
        for (Runnable task : scheduledTasks) {
            try {
                task.run();
            } catch (Exception e) {
                logger.info("Scheduled operation failed with exception", e);
            }
        }
    }

    /**
     * Execute all submitted runnable and callable tasks. All tasks will be executed one-by-one
     * for each call of this method.
     */
    public void executeTasks() {
        runnableTasks.forEach(task -> {
            try {
                task.run();
            } catch (Exception ex) {
                logger.info("Runnable task failed with exception");
            }
        });
        callableTasks.forEach(el -> {
            try {
                el.call();
            } catch (Exception ex) {
                logger.info("Callable task failed with exception");
            }
        });
    }

    /**
     * Remove all tasks which were submitted or schedule earlier.
     */
    public void removeAllTasks() {
        runnableTasks.clear();
        callableTasks.clear();
        scheduledTasks.clear();
    }

    /**
     * Fake scheduled future.
     *
     * @param <T> type of a future result
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
     * @param <T> type of a future result
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
