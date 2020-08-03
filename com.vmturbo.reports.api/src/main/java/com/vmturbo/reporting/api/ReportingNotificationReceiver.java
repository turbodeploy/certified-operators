package com.vmturbo.reporting.api;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportNotification;

/**
 * Notification receiver for repots statuses.
 */
@ThreadSafe
public class ReportingNotificationReceiver extends
        MulticastNotificationReceiver<ReportNotification, ReportListener> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Topic used for reports notifications.
     */
    public static final String REPORT_GENERATED_TOPIC = "report-generation-notifications";

    /**
     * Constructs reporting notification receiver.
     *
     * @param messageReceiver message receiver (from underlying communication layer).
     * @param threadPool thread pool to use
     */
    public ReportingNotificationReceiver(
            @Nonnull IMessageReceiver<ReportNotification> messageReceiver,
            @Nonnull ExecutorService threadPool, int kafkaReceiverTimeoutSeconds) {
        super(Objects.requireNonNull(messageReceiver), threadPool, kafkaReceiverTimeoutSeconds,
                ReportingNotificationReceiver::createNotificator);
    }

    @Nonnull
    private static Consumer<ReportListener> createNotificator(@Nonnull ReportNotification message) {
        logger.trace("Received notification {} for report {}", message::getNotificationCase,
                message::getReportId);
        switch (message.getNotificationCase()) {
            case GENERATED:
                return listener -> listener.onReportGenerated(message.getReportId());
            case FAILED:
                return listener -> listener.onReportFailed(message.getReportId(),
                        message.getFailed());
            default:
                logger.error("Notification type not recognized: " + message.toString());
                return listener -> { };
        }
    }
}
