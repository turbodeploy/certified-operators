package com.vmturbo.mediation.client.it;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage.MediationClientMessageCase;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Class to capture messages, received from remote mediation server.
 */
public class RmMessageReceipient<O extends Operation> extends OperationMessageHandler<O> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private boolean expired = false;
    private boolean transportClosed = false;
    private MediationClientMessage message = null;

    protected RmMessageReceipient(Class<O> operationClass) {
        super(Mockito.mock(OperationManager.class), Mockito.mock(operationClass), Clock.systemUTC(), Long.MAX_VALUE);
    }

    @Override
    public void onExpiration() {
        expired = true;
        latch.countDown();
    }

    @Nonnull
    @Override
    public HandlerStatus onMessage(@Nonnull MediationClientMessage receivedMessage) {
        if (receivedMessage
                        .getMediationClientMessageCase() == MediationClientMessageCase.KEEPALIVE) {
            return HandlerStatus.IN_PROGRESS;
        }
        this.message = receivedMessage;
        latch.countDown();
        return HandlerStatus.COMPLETE;
    }

    @Override
    public void onTransportClose() {
        this.transportClosed = true;
        latch.countDown();
    }

    public boolean isExpired() {
        return expired;
    }

    public boolean isTransportClosed() {
        return transportClosed;
    }

    public MediationClientMessage getMessage() {
        return message;
    }

    public void await(long timeSec) throws InterruptedException {
        Assert.assertTrue(latch.await(timeSec, TimeUnit.SECONDS));
    }
}
