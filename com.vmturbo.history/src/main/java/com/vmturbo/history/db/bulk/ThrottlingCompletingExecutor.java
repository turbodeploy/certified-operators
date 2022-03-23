package com.vmturbo.history.db.bulk;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
    private final long drainTimeoutMsecs;

    private int highWater = 0;

    private final ExecutorService threadPool;
    private final int maxPending;
    private final Semaphore semaphore;
    private final Object drainSync = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new instance.
     *
     * @param pool             underlying {@link ExecutorService} to execute tasks
     * @param maxPending       maximum number of queued+active tasks allowed before attempts task
     *                         submission becomes a blocking operation
     * @param drainTimeoutSecs max time to wait for drain operation to complete, in seconds
     */
    public ThrottlingCompletingExecutor(ExecutorService pool, int maxPending,
            long drainTimeoutSecs) {
        this.threadPool = pool;
        this.maxPending = maxPending;
        this.semaphore = new Semaphore(this.maxPending);
        this.drainTimeoutMsecs = TimeUnit.SECONDS.toMillis(drainTimeoutSecs);
    }

    /**
     * Submit a task for execution.
     *
     * <p>The handler will be used to convey the {@link Future} representing the task result
     * once it has completed.</p>
     *
     * @param task          the {@link Callable} to be executed
     * @param beforeExecute the {@link Consumer} that prepares the {@link Future} to be handled
     * @param handler       the {@link Consumer} to receive the {@link Future} upon completion We
     *                      use a future here to allow the handler to catch any exceptions.
     * @throws InterruptedException if interrutped
     * @throws TimeoutException     if we wait too long to acquire a semaphore permit for a new
     *                              task
     */
    public void submit(Callable<T> task, Consumer<Future<T>> beforeExecute,
            Consumer<Future<T>> handler)
            throws InterruptedException, TimeoutException {
        if (closed.get()) {
            throw new IllegalStateException("Batch completer is closed");
        }
        if (!semaphore.tryAcquire(drainTimeoutMsecs, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Failed to obtain semaphore permit");
        }
        final CompletableFuture<T> f = new CompletableFuture<>();
        // Wrap the result of calling the task in a future that will get passed back
        // to the handler.
        Runnable wrappedTask = () -> {
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
                synchronized (drainSync) {
                    drainSync.notifyAll();
                }
            }
        };
        try {
            beforeExecute.accept(f);
            threadPool.execute(wrappedTask);
        } catch (RuntimeException e) {
            // this is either the beforeExecute hook or the execute method, which can throw
            // unchecked exceptions
            logger.error("Failed to schedule task", e);
            semaphore.release();
        }
        highWater = Math.max(highWater, maxPending - semaphore.availablePermits());
    }

    /**
     * Wait for the pool to become empty.
     *
     * <p>N.B. This does not block further task submission, so if that continues, this method
     * could never return.</p>
     *
     * @throws InterruptedException if interrupted
     */
    public void drain() throws InterruptedException {
        long timeBarrier = System.currentTimeMillis() + drainTimeoutMsecs;
        synchronized (drainSync) {
            while (true) {
                if (semaphore.availablePermits() == maxPending) {
                    return;
                }
                long remainingTime = timeBarrier - System.currentTimeMillis();
                if (remainingTime > 0L) {
                    drainSync.wait(remainingTime);
                } else {
                    logger.error(
                            "Timed out waiting for throttling completion service to finish work, "
                                    + "with {} tasks outstanding",
                            maxPending - semaphore.availablePermits());
                    return;
                }
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
        if (closed.getAndSet(true)) {
            // already closed
            return;
        }
        drain();
        threadPool.shutdownNow();
    }
}
