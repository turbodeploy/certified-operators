package com.vmturbo.components.api;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

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

    private Iterator<Long> backoffIterator = Iterators.cycle(DEFAULT_BACKOFF_MS);

    private RetriableOperation(@Nonnull final Operation<T> operation) {
        this.operation = operation;
    }

    /**
     * Run the operation.
     *
     * <p>The max number of retries is determined by the configured backoff strategy/iterator and
     * the time limit provided in this method:</p>
     *
     * <ul>
     *     <li>If the time limit value is Long.MAX_VALUE, it is treated as infinite, and the time unit
     *     is ignored. The retries continue until the configured backoff iterator runs out, and they
     *     never stop if that doesn't happen</li>
     *
     *     <li>For any other time limit value, retries will continue until until either the backoff
     *     iterator runs out or the total time exceeds the given limit.</li>
     * </ul>
     *
     * <p>At least one try is always attempted.</p>
     *
     * <p>You can configure retries via {@link #backoffStrategy(BackoffStrategy)}, or
     * {@link #backoffStrategy(Iterator)}, or stick with the default, which backs off by 10s on
     * every failed attempt.</p>
     *
     * @param timeout  The timeout, or null to retry indefinitely until the backoff iterator
     *                 finishes.
     * @param timeUnit The time units for the timeout.
     * @return The first successful result of the operation.
     * @throws InterruptedException              If the thread was interrupted while sleeping
     *                                           between tries.
     * @throws RetriableOperationFailedException If the operation failed with an exception that
     *                                           didn't pass the predicate provided in {@link
     *                                           RetriableOperation#retryOnException(Predicate)}.
     * @throws TimeoutException                  If there is no successful result after the
     *                                           specified timeout.
     */
    @Nonnull
    public T run(final long timeout,
            final TimeUnit timeUnit)
            throws InterruptedException, RetriableOperationFailedException, TimeoutException {
        Preconditions.checkArgument(timeout > 0);
        OperationResult<T> latestResult = null;
        final Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            try {
                T curTryOutput = operation.call();
                latestResult = new OperationResult<>(curTryOutput, null);
            } catch (RetriableOperationFailedException | RuntimeException e) {
                // This is a warning-level log, because the error may be expected by the caller.
                logger.warn("Retriable operation at {} encountered {} error.",
                        StackTrace.getCallerOutsideClass(),
                        exceptionPredicate.test(e) ? "expected" : "unexpected",
                        e);
                latestResult = new OperationResult<>(null, e);
            }

            if (backoffIterator.hasNext() && shouldRetry(latestResult)) {
                // note that shouldTry returns false if the iterator is depleted
                final long nextDelayMs = backoffIterator.next();
                // treat MAX_VALUE as inifinite (wait til backoff iterator is finished)
                final long timeRemaining = timeout == Long.MAX_VALUE ? Long.MAX_VALUE
                        : timeUnit.toMillis(timeout) - stopwatch.elapsed(TimeUnit.MILLISECONDS);
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
            throw new TimeoutException(String.format("Retriable operation at %s timed out after %s %s",
                    StackTrace.getCallerOutsideClass(), stopwatch.elapsed(timeUnit), timeUnit));
        } else if (latestResult.getException().isPresent()) {
            throw new RetriableOperationFailedException(latestResult.getException().get());
        } else if (latestResult.getOutput().isPresent()) {
            return latestResult.getOutput().get();
        } else {
            throw new IllegalStateException("Unexpected - neither exception nor "
                    + "output set in latest result.");
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
     * Configure the operation to retry on exception if the provided exception predicate returns
     * true. By default any exception will cause the operation to halt.
     *
     * @param exceptionPredicate Predicate for an exception that may be thrown by the callable
     *                           passed to {@link RetriableOperation#newOperation(Operation)}.
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> retryOnException(@Nonnull final Predicate<Exception> exceptionPredicate) {
        this.exceptionPredicate = exceptionPredicate;
        return this;
    }

    /**
     * Configure the operation to retry on gRPC status runtime exception.
     *
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> retryOnUnavailable() {
        this.exceptionPredicate = this.exceptionPredicate.or(e ->
                (e instanceof StatusRuntimeException)
                        && ((StatusRuntimeException)e).getStatus().getCode() == Code.UNAVAILABLE);
        return this;
    }

    /**
     * Configure the operation to retry if the provided result predicate returns true. By default
     * any output will cause the operation to halt.
     *
     * @param outputPredicate Predicate for the output of the callable passed to {@link
     *                        RetriableOperation#newOperation(Operation)}.
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> retryOnOutput(@Nonnull final Predicate<T> outputPredicate) {
        this.outputPredicate = outputPredicate;
        return this;
    }

    /**
     * Provide a custom {@link BackoffStrategy} to determine intervals between retries. The default
     * strategy increases the delay by a constant factor (10s) every time.
     *
     * @param backoffStrategy The {@link BackoffStrategy} to use.
     * @return The {@link RetriableOperation}, for method chaining.
     */
    @Nonnull
    public RetriableOperation<T> backoffStrategy(@Nonnull final BackoffStrategy backoffStrategy) {
        // translate BackoffStrategy to an Iterator<Long>
        this.backoffIterator = new Iterator<Long>() {
            int curTry = 1;

            @Override
            public boolean hasNext() {
                // a BackoffStrategy is not permitted to capable of running out of values
                return true;
            }

            @Override
            public Long next() {
                return backoffStrategy.getNextDelayMs(curTry++);
            }
        };
        return this;
    }

    /**
     * Provide an {@link Iterator} to yield backoff periods for successive retries.
     *
     * <p>The iterator need never terminate, but if it does, the retry attempts will cease when
     * it does.</p>
     *
     * @param backoffIterator iterator delivering backoff periods (long msec)
     * @return this {@link RetriableOperation instance}
     */
    @Nonnull
    public RetriableOperation<T> backoffStrategy(@Nonnull final Iterator<Long> backoffIterator) {
        this.backoffIterator = backoffIterator;
        return this;
    }

    /**
     * Factory method to create a new operation.
     *
     * @param operation The {@link Callable} containing the operation to try.
     * @param <T>       The output of the provided {@link Operation}.
     * @return The {@link RetriableOperation}.
     */
    @Nonnull
    public static <T> RetriableOperation<T> newOperation(@Nonnull final Operation<T> operation) {
        return new RetriableOperation<>(operation);
    }

    /**
     * The operation to call in a retrying manner. Used instead of a {@link Callable} to avoid
     * generic {@link Exception} handling.
     *
     * @param <T> The output of the operation.
     */
    @FunctionalInterface
    public interface Operation<T> {

        /**
         * Call the operation.
         *
         * @return The output of the operation.
         * @throws RetriableOperationFailedException If there is an exception when executing the
         *                                           operation.
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
         *
         * @param curTry The number of the current try (i.e. the one that just failed). Starts at
         *               1.
         * @return The ms to wait before the next try.
         */
        long getNextDelayMs(int curTry);
    }

    /**
     * Provides a configurable backoff strategy. The following settings can be configured.
     * <ul>
     *     <li>Base Delay: the duration of the first retry delay, in milliseconds. Must be greater than zero.</li>
     *     <li>Max Delay: the maximum delay for any given retry, also in milliseconds. Must be greater than or
     *     equal to zero. If set to zero, then the max delay will only be capped to the max long value.</li>
     *     <li>Jitter Factor: A fractional number that will be used to apply a random +/- adjustment
     *     to each delay calculation. The intention is to scatter retry attempts when many processes
     *     may be attempting to retry the same operation on the same schedule. This will default to 0.1,
     *     which will produce a 10% jitter.</li>
     *     <li>BackoffType: Can be FIXED, LINEAR or EXPONENTIAL.
     *     <ul>FIXED: Each retry will be delayed by the base delay amount</ul>
     *     <ul>LINEAR: The retry delay will be increased by the base delay on each successive attempt.</ul>
     *     <ul>EXPONENTIAL: The retry delay will be doubled on each successive attempt.</ul>
     *     </li>
     * </ul>
     *
     * <p>The default config is an Exponential backoff with an initial delay of 50 ms, max delay
     * of 30 seconds, and jitter factor of 20%.
     */
    public static class ConfigurableBackoffStrategy implements BackoffStrategy {
        public enum BackoffType {
            FIXED, // retry with a fixed interval
            LINEAR, // retry with a linearly increasing interval
            EXPONENTIAL // retry with an exponentially increasing interval
        }

        private final BackoffType backoffType;
        private final long baseDelayMs;
        private final long maxDelayMs; // the maximum delay period
        private final double jitterFactor; // amount of random delay to be applied

        public ConfigurableBackoffStrategy(Builder builder) {
            this.backoffType = builder.backoffType;
            this.baseDelayMs = builder.baseDelayMs;
            this.maxDelayMs = builder.maxDelayMs;
            this.jitterFactor = builder.jitterFactor;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        @Override
        public long getNextDelayMs(int curTry) {
            long nextCalculatedDuration;
            try {
                switch (backoffType) {
                    case FIXED:
                        nextCalculatedDuration = baseDelayMs;
                        break;
                    case LINEAR:
                        nextCalculatedDuration = Math.multiplyExact(baseDelayMs, curTry);
                        break;
                    case EXPONENTIAL:
                    default:
                        // we can only power up so many times due to overflow.
                        if (curTry < 63) {
                            nextCalculatedDuration = Math.multiplyExact(baseDelayMs, (1L << (curTry - 1)));
                        } else {
                            nextCalculatedDuration = Long.MAX_VALUE;
                        }
                        logger.trace("Next exponential retry is {} ms", nextCalculatedDuration);
                }
                // apply jitter of +/- percentage
                double jitter = (Math.random() * 2 * jitterFactor) - jitterFactor;
                logger.trace("Applying jitter {}", jitter);
                // only apply if safe to do so
                if ((Long.MAX_VALUE - nextCalculatedDuration - jitter) > 0) {
                    nextCalculatedDuration = nextCalculatedDuration + (Math.round((double)nextCalculatedDuration * jitter));
                } else {
                    // max value
                    nextCalculatedDuration = Long.MAX_VALUE;
                }
            } catch (ArithmeticException ae) {
                // any overflow errors will land here, and we'll just go with the max possible duration.
                nextCalculatedDuration = Long.MAX_VALUE;
            }
            // check against the max, if necessary
            if (maxDelayMs > 0) {
                // return the smaller of the two delays
                nextCalculatedDuration = Math.min(nextCalculatedDuration, maxDelayMs);
            }
            logger.trace("Returning retry delay of {} ms", nextCalculatedDuration);
            return nextCalculatedDuration;
        }

        public static class Builder {
            private BackoffType backoffType = BackoffType.EXPONENTIAL;
            private long baseDelayMs = 50;
            private long maxDelayMs = 30000;
            private double jitterFactor = 0.2; // default to 20% jitter

            public ConfigurableBackoffStrategy build() {
                return new ConfigurableBackoffStrategy(this);
            }

            public Builder withBackoffType(BackoffType backoffType) {
                this.backoffType = backoffType;
                return this;
            }

            public Builder withBaseDelay(long newBaseDelay) {
                if (newBaseDelay <= 0) {
                    throw new IllegalArgumentException("Base delay must be greater than zero.");
                }
                this.baseDelayMs = newBaseDelay;
                return this;
            }

            public Builder withMaxDelay(long newMaxDelay) {
                if (newMaxDelay < 0) {
                    throw new IllegalArgumentException("If specified, Max delay must be greater than or equal to zero.");
                }
                this.maxDelayMs = newMaxDelay;
                return this;
            }

            public Builder withJitterFactor(double newJitterFactor) {
                this.jitterFactor = newJitterFactor;
                return this;
            }
        }

    }

    /**
     * Operation throws when a {@link RetriableOperation} fails due to a fatal exception thrown by
     * the internal callable. To control which exceptions count as fatal use {@link
     * RetriableOperation#retryOnException(Predicate)}.
     */
    public static class RetriableOperationFailedException extends Exception {

        /**
         * Create a new instance of the exception.
         *
         * @param cause The underlying cause.
         */
        public RetriableOperationFailedException(@Nonnull final Exception cause) {
            super(cause);
        }

    }

    /**
     * Internal utility to help represent the output of calling the retriable operation - either an
     * exception or a result.
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
