package com.vmturbo.sample.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.sample.api.EchoListener;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;

/**
 * Receiver for Sample Component nofication messages.
 *
 * Other components that want to receive notifications from the sample component create an
 * instance for this class, providing the connection information to the sample component, and
 * register listeners using {@link SampleComponentNotificationReceiver#addListener(EchoListener)}.
 */
public class SampleComponentNotificationReceiver
        extends MulticastNotificationReceiver<SampleNotification, EchoListener> {

    private final Logger logger = LogManager.getLogger();

    /**
     * {@inheritDoc}
     */
    public SampleComponentNotificationReceiver(
            @Nonnull final IMessageReceiver<SampleNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService, 0);
    }

    @Override
    protected void processMessage(@Nonnull final SampleNotification message)
            throws ApiClientException {
        switch (message.getTypeCase()) {
            case ECHO_RESPONSE:
                invokeListeners(listener -> listener.onEchoResponse(message.getEchoResponse()));
                break;
            default:
                logger.error("Invalid notification.");
        }
    }
}
