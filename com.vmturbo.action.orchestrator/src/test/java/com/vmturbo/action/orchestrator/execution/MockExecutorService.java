package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mock for executor service. Populate all submitted tasks and executing could be triggered all by
 * once by using method {@link #executeTasks()}.
 */
public class MockExecutorService implements ExecutorService {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ExecutorService threadPool = Executors.newFixedThreadPool(5);
    private final Collection<Callable<?>> callableTasks = new CopyOnWriteArrayList<>();
    private final Collection<Runnable> runnableTasks = new CopyOnWriteArrayList<>();

    @Override
    public void shutdown() {
        threadPool.shutdown();
    }

    @Nonnull
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
    public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
            throws InterruptedException {
        return threadPool.awaitTermination(timeout, unit);
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@NotNull Callable<T> task) {
        callableTasks.add(task);
        return new FakeFuture<>();
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@NotNull Runnable task, T result) {
        runnableTasks.add(task);
        return new FakeFuture<>();
    }

    @Nonnull
    @Override
    public Future<?> submit(@NotNull Runnable task) {
        runnableTasks.add(task);
        return new FakeFuture<>();
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks) {
        callableTasks.addAll(tasks);
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
            long timeout, @NotNull TimeUnit unit) {
        callableTasks.addAll(tasks);
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks) {
        callableTasks.addAll(tasks);
        return null;
    }

    @Nonnull
    @Override
    public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks, long timeout,
            @NotNull TimeUnit unit) {
        callableTasks.addAll(tasks);
        return null;
    }

    @Override
    public void execute(@NotNull Runnable command) {
        runnableTasks.add(command);
    }

    /**
     * Execute previously submitted tasks.
     */
    public void executeTasks() {
        for (Callable<?> task : callableTasks) {
            try {
                task.call();
            } catch (Exception e) {
                logger.info("Callable task failed with exception", e);
            }
        }
        for (Runnable task : runnableTasks) {
            try {
                task.run();
            } catch (Exception e) {
                logger.info("Runnable task failed with exception", e);
            }
        }
    }

    /**
     * Fake future.
     *
     * @param <T> type of a future result
     */
    private static class FakeFuture<T> implements Future<T> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Nullable
        @Override
        public T get() {
            return null;
        }

        @Nullable
        @Override
        public T get(long timeout, @NotNull TimeUnit unit) {
            return null;
        }
    }
}
