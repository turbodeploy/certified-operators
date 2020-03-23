package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.api.RetriableOperation.ConfigurableBackoffStrategy;
import com.vmturbo.components.api.RetriableOperation.ConfigurableBackoffStrategy.BackoffType;
import com.vmturbo.components.api.RetriableOperation.Operation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;

/**
 * Unit tests for {@link RetriableOperation}.
 */
public class RetriableOperationTest {

    /**
     * Test success on first try.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRetryOperationSuccess() throws Exception {
        final Operation<Long> callable = () -> 5L;
        assertThat(RetriableOperation.newOperation(callable)
            .run(1, TimeUnit.MILLISECONDS), is(5L));
    }

    /**
     * Test retry on non-fatal exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRetryOperationOnNonFatalException() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        final IllegalArgumentException thrownEx = new IllegalArgumentException("BAH");
        final Operation<Long> callable = () -> {
            if (count.getAndIncrement() == 0) {
                throw thrownEx;
            } else {
                return 5L;
            }
        };

        assertThat(RetriableOperation.newOperation(callable)
            // Wait 1 ms between tries.
            .backoffStrategy(curTry -> 1)
            .retryOnException(e -> e.equals(thrownEx))
            .run(1, TimeUnit.MINUTES), is(5L));
    }

    /**
     * Test retry on non-fatal and non-satisfactory output.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRetryOperationOnNonFatalOutput() throws Exception {
        final long expectedOutcome = 2;
        final AtomicLong count = new AtomicLong(0);
        // Returning an incrementing number each time.
        final Operation<Long> callable = count::getAndIncrement;

        assertThat(RetriableOperation.newOperation(callable)
            // Wait 1 ms between tries.
            .backoffStrategy(curTry -> 1)
            // Retry on output less than 2
            .retryOnOutput(output -> output < expectedOutcome)
            .run(1, TimeUnit.MINUTES), is(expectedOutcome));
    }

    /**
     * Test timeout while trying to retry.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRetryOperationTimeout() throws Exception {
        final long timeoutMs = 1;
        final long unreachableOutcome = 100;
        final AtomicLong count = new AtomicLong(0);
        // Returning an incrementing number each time.
        final Operation<Long> callable = count::getAndIncrement;


        try {
            RetriableOperation.newOperation(callable)
                // Wait 1 ms between tries.
                .backoffStrategy(curTry -> 1)
                // Retry on output less than 100.
                .retryOnOutput(output -> output < unreachableOutcome)
                // The timeout is 1ms, so we shouldn't be able to sleep for 100ms.
                .run(timeoutMs, TimeUnit.MILLISECONDS);
            Assert.fail("Expected timeout exception.");
        } catch (TimeoutException e) {
            assertThat(e.getMessage(), containsString(TimeUnit.MILLISECONDS.toString()));
            assertThat(e.getMessage(), containsString(Long.toString(timeoutMs)));
        }
    }

    /**
     * Test rethrowing on a fatal error.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRetryOperationFatalError() throws Exception {
        final IllegalArgumentException thrownEx = new IllegalArgumentException("BAH");
        final Operation<Long> callable = () -> {
            throw thrownEx;
        };

        try {
            RetriableOperation.newOperation(callable)
                .run(1, TimeUnit.MINUTES);
            Assert.fail("Expected exception");
        } catch (RetriableOperationFailedException e) {
            assertThat(e.getCause(), is(thrownEx));
        }
    }

    /**
     * Test the "Fixed" ConfigurableBackoffStrategy. Validate that the retry delay is around the same
     * on each try.
     */
    @Test
    public void testConfigurableBackoffStrategyFixed() {
        ConfigurableBackoffStrategy backoffStrategy = ConfigurableBackoffStrategy.newBuilder()
                .withBackoffType(BackoffType.FIXED)
                .withBaseDelay(100)
                .withJitterFactor(0.20)
                .build();
        for (int x = 1; x < 10; x++) {
            validateDelayWithJitter(100, backoffStrategy.getNextDelayMs(x), 0.2);
        }
    }

    /**
     * Test the "Linear" ConfigurableBackoffStrategy. This retry delay should increase linearly on
     * each retry.
     */
    @Test
    public void testConfigurableBackoffStrategyLinear() {
        ConfigurableBackoffStrategy backoffStrategy = ConfigurableBackoffStrategy.newBuilder()
                .withBackoffType(BackoffType.LINEAR)
                .withBaseDelay(100)
                .withMaxDelay(1000) // it should take us 10 tries to reach max delay
                .withJitterFactor(0.20)
                .build();
        for (int x = 1; x < 10; x++) {
            validateDelayWithJitter(x * 100, backoffStrategy.getNextDelayMs(x), 0.2);
        }
        // try #11 should still be capped at 1000 ms
        validateDelayWithJitter(1000, backoffStrategy.getNextDelayMs(11), 0.2);
    }

    /**
     * Test the "Exponential" ConfigurableBackoffStrategy. This delay should double each time.
     */
    @Test
    public void testConfigurableBackoffStrategyExponential() {
        ConfigurableBackoffStrategy backoffStrategy = ConfigurableBackoffStrategy.newBuilder()
                .withBackoffType(BackoffType.EXPONENTIAL)
                .withBaseDelay(100)
                .withMaxDelay(1000) // it should take us 10 tries to reach max delay
                .withJitterFactor(0.20)
                .build();
        for (int x = 1; x < 4; x++) {
            validateDelayWithJitter(100 << (x - 1), backoffStrategy.getNextDelayMs(x), 0.2);
        }
        // try #5 should be capped at 1000 ms
        validateDelayWithJitter(1000, backoffStrategy.getNextDelayMs(5), 0.2);
    }

    /**
     * Create a config that would trigger an overfow during calculation. Verify that a max long
     * duration is returned instead of an error.
     */
    @Test
    public void testConfigurableBackoffStrategyOverflow() {
        ConfigurableBackoffStrategy backoffStrategy = ConfigurableBackoffStrategy.newBuilder()
                .withBackoffType(BackoffType.EXPONENTIAL)
                .withBaseDelay(100)
                .withMaxDelay(0) // disable the max delay
                .withJitterFactor(0.20)
                .build();
        // we would overflow at about iteration #58 with this config.
        validateDelayWithJitter(Long.MAX_VALUE, backoffStrategy.getNextDelayMs(10000), 0.2);
    }


    private boolean validateDelayWithJitter(long expected, long actual, double jitterFactor) {
        // valid as long as actual is within the jitter factor of the expected value.
        long difference = actual - expected;
        if (difference == 0) {
            return true;
        }
        double differenceRatio = (double)difference / (double)expected;
        assertTrue("Difference ratio " + differenceRatio + " should be within expected jitter factor of " + jitterFactor + ".",
                Math.abs(differenceRatio) <= Math.abs(jitterFactor));
        return differenceRatio < jitterFactor;
    }
}
