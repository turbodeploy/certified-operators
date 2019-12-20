package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

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
}
