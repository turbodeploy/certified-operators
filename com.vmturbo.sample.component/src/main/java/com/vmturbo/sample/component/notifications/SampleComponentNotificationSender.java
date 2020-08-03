package com.vmturbo.sample.component.notifications;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;
import com.vmturbo.sample.api.impl.SampleComponentNotificationReceiver;
import com.vmturbo.sample.component.echo.EchoRpcConfig;

/**
 * This is the server-side backend that's responsible for sending notifications to
 * all {@link SampleComponentNotificationReceiver} clients.
 *
 * {@link SampleComponentNotificationSender} opens a websocket endpoint. When clients
 * (i.e. other components) create a {@link SampleComponentNotificationReceiver} the implementation
 * creates a websocket connection to this endpoint.
 *
 * We inject the {@link SampleComponentNotificationSender} into the classes in the sample component
 * that generate notifications (see {@link EchoRpcConfig}). Those classes
 * trigger methods on {@link SampleComponentNotificationSender} to send notifications to all open
 * websocket connections.
 */
public class SampleComponentNotificationSender extends
        ComponentNotificationSender<SampleNotification> {

    private final IMessageSender<SampleNotification> sender;

    protected SampleComponentNotificationSender(
            @Nonnull IMessageSender<SampleNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * Other classes in the sample component can call this method to send a notification to
     * registered listeners.
     * @throws InterruptedException if sending thread has been interrupted. This does not
     *      guarantee, that message has ben sent nor it has not been sent
     * @throws CommunicationException if persistent communication error occurred (message could
     *      not be sent in future).
     */
    public void notifyEchoResponse(@Nonnull final EchoResponse response)
            throws CommunicationException, InterruptedException {
        final SampleNotification notification = newNotification()
            .setEchoResponse(Objects.requireNonNull(response))
            .build();

        sendMessage(sender, notification);
    }

    private SampleNotification.Builder newNotification() {
        return SampleNotification.newBuilder()
                .setBroadcastId(newMessageChainId());
    }

    @Override
    protected String describeMessage(@Nonnull SampleNotification sampleNotification) {
        return EchoResponse.class.getSimpleName();
    }
}
