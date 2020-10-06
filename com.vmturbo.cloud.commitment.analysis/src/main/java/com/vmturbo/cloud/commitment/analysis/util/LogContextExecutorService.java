package com.vmturbo.cloud.commitment.analysis.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.ThreadContext;
import org.jetbrains.annotations.NotNull;

/**
 * A wrapper around a delegate {@link ExecutorService}, in which each {@link Runnable} and
 * {@link Callable} instance is wrapped to pass on the current log4j thread context to the child
 * thread.
 */
public class LogContextExecutorService implements ExecutorService {

    private final ExecutorService delegate;

    private LogContextExecutorService(ExecutorService delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean awaitTermination(final long timeout, @NotNull final TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull final Callable<T> task) {
        return delegate.submit(wrapCallable(task));
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull final Runnable task, final T result) {
        return delegate.submit(wrapRunnable(task), result);
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public Future<?> submit(@NotNull final Runnable task) {
        return delegate.submit(wrapRunnable(task));
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull final Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(
                tasks.stream()
                        .map(this::wrapCallable)
                        .collect(Collectors.toList()));
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull final Collection<? extends Callable<T>> tasks,
                                         final long timeout,
                                         @NotNull final TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(
                tasks.stream().map(this::wrapCallable).collect(Collectors.toList()),
                timeout,
                unit);
    }

    /**
     * {@inheritDoc}.
     */
    @NotNull
    @Override
    public <T> T invokeAny(@NotNull final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(
                tasks.stream()
                        .map(this::wrapCallable)
                        .collect(Collectors.toList()));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public <T> T invokeAny(@NotNull final Collection<? extends Callable<T>> tasks,
                           final long timeout,
                           @NotNull final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(
                tasks.stream().map(this::wrapCallable).collect(Collectors.toList()),
                timeout,
                unit);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void execute(@NotNull final Runnable command) {
        delegate.execute(wrapRunnable(command));
    }

    /**
     * Creates a new {@link LogContextExecutorService} instance, wrapping the {@code delegate}.
     * @param delegate The delegate {@link ExecutorService} to wrap.
     * @return The new {@link LogContextExecutorService} instance.
     */
    @Nonnull
    public static LogContextExecutorService newExecutorService(@Nonnull ExecutorService delegate) {
        return new LogContextExecutorService(delegate);
    }

    private <T> Callable<T> wrapCallable(@Nonnull Callable<T> callable) {

        final Map<String, String> values = ThreadContext.getImmutableContext();
        return () -> {

            try (CloseableThreadContext.Instance ctc = CloseableThreadContext.putAll(values)) {
                return callable.call();
            }
        };
    }

    private Runnable wrapRunnable(@Nonnull Runnable runnable) {
        final Map<String, String> values = ThreadContext.getImmutableContext();
        return () -> {

            try (CloseableThreadContext.Instance ctc = CloseableThreadContext.putAll(values)) {
                runnable.run();
            }
        };
    }
}
