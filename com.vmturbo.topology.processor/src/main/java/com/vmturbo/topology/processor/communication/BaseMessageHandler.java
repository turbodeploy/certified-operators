package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;

/**
 * Base class for expiring message handlers.
 * Handles the details of calculating when a message handler expires in a thread-safe
 * fashion so that expiration timeouts can be checked and refreshed from separate threads.
 */
@ThreadSafe
public abstract class BaseMessageHandler implements ExpiringMessageHandler {

    private final Clock clock;
    private final long timeoutMilliseconds;
    private long lastMessageTime;

    public BaseMessageHandler(@Nonnull Clock clock, long timeoutMilliseconds) {
        this.clock = Objects.requireNonNull(clock);
        this.timeoutMilliseconds = timeoutMilliseconds;
        this.lastMessageTime = clock.millis();
    }

    @Override
    public synchronized long expirationTime() {
        return lastMessageTime + timeoutMilliseconds;
    }


    @Override
    @Nonnull
    public final HandlerStatus onReceive(@Nonnull final MediationClientMessage receivedMessage) {
        refreshLastMessageTime();
        return onMessage(receivedMessage);
    }

    @Nonnull
    protected abstract HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage);

    /**
     * Refresh the time at which the last message intended for a message handler
     * was received.
     */
    protected synchronized void refreshLastMessageTime() {
        lastMessageTime = clock.millis();
    }
}