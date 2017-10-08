package com.vmturbo.sample.component.notifications;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.sample.api.SampleComponent;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;
import com.vmturbo.sample.api.impl.SampleComponentNotificationReceiver;
import com.vmturbo.sample.component.echo.EchoRpcConfig;

/**
 * This is the server-side backend that's responsible for sending notifications to
 * all {@link SampleComponent} clients.
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

    protected SampleComponentNotificationSender(@Nonnull final ExecutorService executorService,
            @Nonnull IMessageSender<SampleNotification> sender) {
        super(executorService);
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * Other classes in the sample component can call this method to send a notification to
     * registered listeners.
     */
    public void notifyEchoResponse(@Nonnull final EchoResponse response) {
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
