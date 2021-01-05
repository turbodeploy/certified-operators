package com.vmturbo.history.db.bulk;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.StackTrace;

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
 * @param <T> type of task result
 */
public class ThrottlingCompletingExecutor<T> {
    private static final Logger logger = LogManager.getLogger();

    private static final ThreadFactory completerThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("completer-%d")
            .setDaemon(true)
            .build();

    private int highWater = 0;

    private final ExecutorService threadPool;
    private final int maxPending;
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
        this.threadPool = pool;
        this.maxPending = maxPending;
        this.semaphore = new Semaphore(this.maxPending);
    }

    /**
     * Submit a task for execution.
     *
     * <p>The handler will be used to convey the {@link Future} representing the task result
     * once it has completed.</p>
     *
     * @param task    the {@link Callable} to be executed
     * @param handler the {@link Consumer} to receive the {@link Future} upon completion
     *                We use a future here to allow the handler to catch any exceptions.
     * @throws InterruptedException if interrutped
     */
    public void submit(Callable<T> task, Consumer<Future<T>> handler) throws InterruptedException {
        semaphore.acquire();
        synchronized (this) {
            if (!closed) {
                threadPool.execute(() -> {
                    // Wrap the result of calling the task in a future that will get passed back
                    // to the handler.
                    final CompletableFuture<T> f = new CompletableFuture<>();
                    try {
                        T result = task.call();
                        f.complete(result);
                    } catch (Exception e) {
                        // An exception by the task gets passed to the handler.
                        f.completeExceptionally(e);
                    }

                    try {
                        handler.accept(f);
                    } catch (RuntimeException e) {
                        // An exception
                        logger.error("Handler passed to executor ({}) encountered an error.",
                                StackTrace.getCallerOutsideClass(), e);
                    } finally {
                        // Release the semaphore now that the thread is done processing the task.
                        semaphore.release();
                        // Unblock any threads waiting for handlers to complete.
                        synchronized (this) {
                            this.notifyAll();
                        }
                    }
                });
                highWater = Math.max(highWater, pendingExecutions.size());
            } else {
                throw new IllegalStateException("Batch completer is closed");
            }
        }
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
                if (semaphore.availablePermits() == maxPending) {
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
