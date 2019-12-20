package com.vmturbo.components.api;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility to help run operations with non-failing errors in a loop until a timeout or maximum
 * number of tries is reached.
 *
 * <p>The default workflow is:
 * <ol>
 * <li> Create the operation with {@link RetriableOperation#newOperation(Operation)}.
 * <li> Customize the output or exception predicates via {@link RetriableOperation#retryOnOutput(Predicate)}
 * and {@link RetriableOperation#retryOnException(Predicate)}. At least one predicate should be
 * provided, or else there is no benefit to using a retriable operation.
 * <li> (optional) Customize the backoff strategy.
 * <li> Run - {@link RetriableOperation#run(long, TimeUnit)}.
 * </ol>
 *
 * @param <T> The output of the operation.
 */
public class RetriableOperation<T> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * By default, we back off by 10s every failed attempt. This is reasonable for RPCs between
     * components, or when waiting for components/services to become available, which is the common
     * expected use case for this class.
     */
    private static final long DEFAULT_BACKOFF_MS = 10_000L;

    private final Operation<T> operation;

    private Predicate<T> outputPredicate = (o) -> false;

    private Predicate<Exception> exceptionPredicate = (e) -> false;

    private BackoffStrategy backoffStrategy = (curTry) -> curTry * DEFAULT_BACKOFF_MS;

    private RetriableOperation(@Nonnull final Operation<T> operation) {
        this.operation = operation;
    }

    /**
     * Run the operation.
     *
     * @param timeout The timeout. The amount of tries before hitting the timeout will be determined
     *                by the backoff strategy. You can configure it via
     *                {@link RetriableOperation#backoffStrategy(BackoffStrategy)}, or use the default,
     *                which backs off by 10s on every failed attempt.
     * @param timeUnit The time units for the timeout.
     * @return The first successful result of the operation.
     * @throws InterruptedException If the thread was interrupted while sleeping between tries.
     * @throws RetriableOperationFailedException If the operation failed with an exception that
     * didn't pass the predicate provided in {@link RetriableOperation#retryOnException(Predicate)}.
     * @throws TimeoutException If there is no successful result after the specified timeout.
     */
    @Nonnull
    public T run(final long timeout,
                 final TimeUnit timeUnit)
            throws InterruptedException, RetriableOperationFailedException, TimeoutException {
        Preconditions.checkArgument(timeout > 0);
        OperationResult<T> latestResult = null;
        final Stopwatch stopwatch = Stopwatch.createStarted();
        for (int curTry = 1; ; curTry++) {
            try {
                T curTryOutput = operation.call();
                latestResult = new OperationResult<>(curTryOutput, null);
            } catch (RetriableOperationFailedException | RuntimeException e) {
                latestResult = new OperationResult<>(null, e);
            }

            if (shouldRetry(latestResult)) {
                final long nextDelayMs = backoffStrategy.getNextDelayMs(curTry);
                final long timeRemaining = timeUnit.toMillis(timeout) - stopwatch.elapsed(TimeUnit.MILLISECONDS);
                final long timeToSleep = Math.min(timeRemaining, nextDelayMs);
                if (timeRemaining <= 0) {
                    break;
                } else if (timeToSleep > 0) {
                    logger.debug("Retriable operation at {} sleeping for {} ms after failed attempt.",
                        StackTrace.getCallerOutsideClass(),
                        timeToSleep);
                    Thread.sleep(timeToSleep);
                }
            } else {
                break;
            }
        }

        if (shouldRetry(latestResult)) {
            throw new TimeoutException("Retriable operation at " +
                StackTrace.getCallerOutsideClass() + " timed out after " +
                stopwatch.elapsed(timeUnit) + " " + timeUnit);
        } else if (latestResult.getException().isPresent()) {
            throw new RetriableOperationFailedException(latestResult.getException().get());
        } else if (latestResult.getOutput().isPresent()) {
            return latestResult.getOutput().get();
        } else {
            throw new IllegalStateException("Unexpected - neither exception nor " +
                "output set in latest result.");
        }
    }

    private boolean shouldRetry(@Nonnull final OperationResult<T> opResult) {
        return opResult.getOutput()
            .map(outputPredicate::test)
            .orElseGet(() -> opResult.getException()
                .map(exceptionPredicate::test)
                .orElse(false));
    }

    /**
     * Configure the operation to retry on exception if the provided exception predicate returns true.
     * By default any exception will cause the operation to halt.
     *
     * @param exceptionPredicate Predicate for an exception that may be thrown by the callable passed
     *                           to {@link RetriableOperation#newOperation(Operation)}.
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> retryOnException(@Nonnull final Predicate<Exception> exceptionPredicate) {
        this.exceptionPredicate = exceptionPredicate;
        return this;
    }

    /**
     * Configure the operation to retry if the provided result predicate returns true.
     * By default any output will cause the operation to halt.
     *
     * @param outputPredicate Predicate for the output of the callable passed
     *                        to {@link RetriableOperation#newOperation(Operation)}.
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> retryOnOutput(@Nonnull final Predicate<T> outputPredicate) {
        this.outputPredicate = outputPredicate;
        return this;
    }

    /**
     * Provide a custom {@link BackoffStrategy} to determine intervals between retries.
     * The default strategy increases the delay by a constant factor (10s) every time.
     *
     * @param backoffStrategy The {@link BackoffStrategy} to use.
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> backoffStrategy(@Nonnull final BackoffStrategy backoffStrategy) {
        this.backoffStrategy = backoffStrategy;
        return this;
    }

    /**
     * Factory method to create a new operation.
     *
     * @param operation The {@link Callable} containing the operation to try.
     * @param <T> The output of the provided {@link Operation}.
     * @return The {@link RetriableOperation}.
     */
    @Nonnull
    public static <T> RetriableOperation<T> newOperation(@Nonnull final Operation<T> operation) {
        return new RetriableOperation<>(operation);
    }

    /**
     * The operation to call in a retrying manner.
     * Used instead of a {@link Callable} to avoid generic {@link Exception} handling.
     *
     * @param <T> The output of the operation.
     */
    @FunctionalInterface
    public interface Operation<T> {

        /**
         *  Call the operation.
         *
         * @return The output of the operation.
         * @throws RetriableOperationFailedException If there is an exception when executing the
         *         operation.
         */
        T call() throws RetriableOperationFailedException;
    }

    /**
     * The backoff strategy to use when determining how long to sleep between attempts at the
     * operation.
     */
    @FunctionalInterface
    public interface BackoffStrategy {

        /**
         * Get the ms to wait before trying the operation again.
         * @param curTry The number of the current try (i.e. the one that just failed). Starts at 1.
         * @return The ms to wait before the next try.
         */
        long getNextDelayMs(int curTry);
    }

    /**
     * Operation throws when a {@link RetriableOperation} fails due to a fatal exception thrown
     * by the internal callable. To control which exceptions count as fatal use
     * {@link RetriableOperation#retryOnException(Predicate)}.
     */
    public static class RetriableOperationFailedException extends Exception {

        RetriableOperationFailedException(@Nonnull final Exception cause) {
            super(cause);
        }

    }

    /**
     * Internal utility to help represent the output of calling the retriable operation -
     * either an exception or a result.
     *
     * @param <T> The type returned by the operation.
     */
    private static class OperationResult<T> {
        private final T output;
        private final Exception exception;

        private OperationResult(@Nullable final T output, @Nullable final Exception exception) {
            Preconditions.checkArgument(output != null || exception != null);
            this.output = output;
            this.exception = exception;
        }

        private Optional<T> getOutput() {
            return Optional.ofNullable(output);
        }

        private Optional<Exception> getException() {
            return Optional.ofNullable(exception);
        }
    }
}