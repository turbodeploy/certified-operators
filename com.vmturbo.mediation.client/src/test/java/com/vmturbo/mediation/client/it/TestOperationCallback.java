package com.vmturbo.mediation.client.it;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.Assert;

import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;

/**
 * Class to capture messages, received from remote mediation server.
 *
 * @param <T> type of a result of operation
 */
public class TestOperationCallback<T> implements OperationCallback<T> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private T message = null;
    private String error = null;

    @Override
    public void onSuccess(@Nonnull T response) {
        this.message = response;
        latch.countDown();
    }

    @Override
    public void onFailure(@Nonnull String error) {
        this.error = error;
        latch.countDown();
    }

    public T getMessage() {
        return message;
    }

    public String getError() {
        return this.error;
    }

    /**
     * Awaits the callback for any response.
     *
     * @param timeSec time in seconds to await
     * @throws InterruptedException in current thread is interrupted
     */
    public void await(long timeSec) throws InterruptedException {
        Assert.assertTrue(latch.await(timeSec, TimeUnit.SECONDS));
    }
}
