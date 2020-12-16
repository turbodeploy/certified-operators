package com.vmturbo.history.db.bulk;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * This class implements an {@link Executor} that uses an underlying {@link ExecutorService} to
 * execute tasks based on that service's characteristics. The class adds throttling of task
 * submission as well as eager completion of tasks via callbacks.
 *
 * <p>The throttling feature means that new task submission becomes a blocking operation when a
 * limit on queued and executing tasks has been reached.</p>
 *
 * <p>The completion feature means that when a task completes, its future is immediately
 * passed back to the submitter via a consumer method provided in the submission.</p>
 *
 * <p>The {@link Executor#execute(Runnable)} method is incapable of performing completion
 * processing becasue the interface does not involve a {@link Future}. However, the method {@link
 * #execute(Runnable, Object, Consumer)} takes its cue from {@link CompletionService#submit(Runnable,
 * Object)} by scheduling a {@link Runnable} for execution and returning a {@link Future} holding a
 * value supplied by the submittor upon completion.</p>
 *
 * @param <T> type of task result
 */
public class ThrottlingCompletingExecutor<T> implements Executor {
    private static final Logger logger = LogManager.getLogger();

    private static final ThreadFactory completerThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("completer-%d")
            .setDaemon(true)
            .build();

    private int highWater = 0;

    private final ExecutorCompletionService<T> completer;
    private final Semaphore semaphore;

    private final Map<Future<T>, Consumer<Future<T>>> pendingExecutions = new ConcurrentHashMap<>();
    private boolean closed;

    /**
     * Create a new instance.
     *
     * @param pool       underlying {@link ExecutorService} to execute tasks
     * @param maxPending maximum number of queued+active tasks allowed before attempts task
     *                   submission becomes a blocking operation
     */
    public ThrottlingCompletingExecutor(ExecutorService pool, int maxPending) {
        this.completer = new ExecutorCompletionService<>(pool);
        this.semaphore = new Semaphore(maxPending);
        startCompleterThread();
    }

    /**
     * Submit a task for execution.
     *
     * <p>The handler will be used to convey the {@link Future} representing the task result
     * once it has completed.</p>
     *
     * @param task    the {@link Callable} to be executed
     * @param handler the {@link Consumer} to receive the {@link Future} upon completion
     * @return The (probably not net completed) {@link Future} potentially useful for bookkeeping in
     * the caller
     * @throws InterruptedException if interrutped
     */
    public Future<T> submit(Callable<T> task, Consumer<Future<T>> handler) throws InterruptedException {
        semaphore.acquire();
        synchronized (this) {
            if (!closed) {
                final Future<T> future = completer.submit(task);
                pendingExecutions.put(future, handler);
                highWater = Math.max(highWater, pendingExecutions.size());
                return future;
            } else {
                throw new IllegalStateException("Batch completer is closed");
            }
        }
    }

    @Override
    public void execute(@NotNull final Runnable command) {
        try {
            execute(command, null, value -> {
            });
        } catch (InterruptedException e) {
            logger.warn("Interrupted while scheduling a Runnable for execution", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Submit a {@link Runnable} for execution.
     *
     * @param command     the {@link Runnable} to be executed
     * @param returnValue return value to be conveyed back to submitter (as a {@link Future}) upon
     *                    completion
     * @param handler     the {@link Consumer} to receive the {@link Future} upon completion
     * @return the (probably not yet completed) future containing the return value
     * @throws InterruptedException if interrupted
     */
    public Future<T> execute(@Nonnull final Runnable command, final T returnValue,
            Consumer<Future<T>> handler) throws InterruptedException {
        semaphore.acquire();
        synchronized (this) {
            if (!closed) {
                final Future<T> future = completer.submit(command, returnValue);
                pendingExecutions.put(future, handler);
                highWater = Math.max(highWater, pendingExecutions.size());
                return future;
            } else {
                throw new IllegalStateException("Batch completer is closed");
            }
        }
    }

    private void startCompleterThread() {
        final Thread thread = completerThreadFactory.newThread(() -> {
            while (true) {
                try {
                    Future<T> next = completer.take();
                    semaphore.release();
                    final Consumer<Future<T>> handler = pendingExecutions.remove(next);
                    if (handler != null) {
                        handler.accept(next);
                    } else {
                        logger.warn("No handler registered for completion of future: {}", next);
                    }
                    synchronized (this) {
                        this.notifyAll();
                    }
                } catch (InterruptedException e) {
                    logger.warn("Completer thread interrupted {}, exiting",
                            Thread.currentThread().getName());
                    break;
                }
            }
        });
        thread.start();
    }

    /**
     * Wait for the pool to become mepty.
     *
     * <p>N.B. This does not block further task submission, so if that continues, this method
     * could never return.</p>
     *
     * @throws InterruptedException if interrupted
     */
    public void drain() throws InterruptedException {
        while (true) {
            synchronized (this) {
                if (pendingExecutions.isEmpty()) {
                    this.notify();
                    return;
                }
                this.wait();
            }
        }
    }

    /**
     * Close the executor and wait for existing tasks to complete.
     *
     * <p>Further attempts to submit tasks will fail.</p>
     *
     * @throws InterruptedException if interrupted
     */
    public void close() throws InterruptedException {
        synchronized (this) {
            this.closed = true;
        }
        drain();
        logger.debug("BatchCompleter high-water: {}", highWater);
    }
}
