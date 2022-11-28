package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.history.component.api.ApplicationServiceHistoryListener;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification;

/**
 * The message receiver for application service history notifications.  Listeners use this class to
 * add themselves as a listener to this receiver.
 */
public class ApplicationServiceHistoryNotificationReceiver
        extends MulticastNotificationReceiver<ApplicationServiceHistoryNotification, ApplicationServiceHistoryListener> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The notification topic of this receiver.
     */
    public static final String NOTIFICATION_TOPIC = "application-service-history-notification";

    /**
     * Creates a new instance of the ApplicationServiceHistoryNotificationReceiver.
     *
     * @param messageReceiver The message receiver to use.
     * @param executorService The executor service to use.
     * @param kafkaReceiverTimeoutSeconds The timeout seconds.
     */
    public ApplicationServiceHistoryNotificationReceiver(
            @Nonnull final IMessageReceiver<ApplicationServiceHistoryNotification> messageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(messageReceiver, executorService, kafkaReceiverTimeoutSeconds,
                msg -> routeMessage(msg));
    }

    private static Consumer<ApplicationServiceHistoryListener> routeMessage(
            @Nonnull final ApplicationServiceHistoryNotification msg) {
        return listener -> {
            logger.info("Invoking ApplicationServiceHistoryNotification listener, "
                            + "listener={} type={}, daysInfoListSize={}, topologyId={}",
                    listener.getClass().getSimpleName(), msg.getType(),
                    msg.getDaysEmptyInfoList().size(), msg.getTopologyId());
            listener.onApplicationServiceHistoryNotification(msg);
        };
    }
}
