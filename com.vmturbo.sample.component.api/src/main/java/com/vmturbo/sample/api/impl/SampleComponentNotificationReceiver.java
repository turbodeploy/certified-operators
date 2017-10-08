package com.vmturbo.sample.api.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.sample.api.EchoListener;
import com.vmturbo.sample.api.SampleComponent;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;

/**
 * This is the implementation of {@link SampleComponent}.
 *
 * Other components that want to receive notifications from the sample component create an
 * instance for this class, providing the connection information to the sample component, and
 * register listeners using {@link SampleComponent#addEchoListener(EchoListener)}.
 */
public class SampleComponentNotificationReceiver
        extends ComponentNotificationReceiver<SampleNotification> implements SampleComponent {

    // The WEBSOCKET_PATH is used, in addition to the address and port, to establish a websocket
    // connection to the Sample Component.
    public static final String WEBSOCKET_PATH = "/sample";

    private final Logger logger = LogManager.getLogger();

    private final Set<EchoListener> listeners = Collections.synchronizedSet(new HashSet<>());

    /**
     * {@inheritDoc}
     */
    public SampleComponentNotificationReceiver(
            @Nonnull final IMessageReceiver<SampleNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull final SampleNotification message)
            throws ApiClientException {
        switch (message.getTypeCase()) {
            case ECHO_RESPONSE:
                notifyEchoListeners(message.getEchoResponse());
                break;
            default:
                logger.error("Invalid notification.");
        }
    }

    private void notifyEchoListeners(@Nonnull final EchoResponse echoResponse) {
        listeners.forEach(listener -> getExecutorService().submit(() -> {
            try {
                listener.onEchoResponse(echoResponse);
            } catch (RuntimeException e) {
                logger.error("Error processing echo response.", e);
            }
        }));
    }

    @Override
    public void addEchoListener(@Nonnull final EchoListener listener) {
        listeners.add(listener);
    }
}
