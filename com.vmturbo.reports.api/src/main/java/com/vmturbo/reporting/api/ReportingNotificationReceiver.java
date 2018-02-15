package com.vmturbo.reporting.api;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportNotification;

/**
 * Notification receiver for repots statuses.
 */
@ThreadSafe
public class ReportingNotificationReceiver extends
        ComponentNotificationReceiver<ReportNotification> {

    /**
     * Topic used for reports notifications.
     */
    public static final String REPORT_GENERATED_TOPIC = "report-generation-notifications";
    /**
     * Set of registered listeners.
     */
    private final Set<ReportListener> listeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Constructs reporting notification receiver.
     *
     * @param messageReceiver message receiver (from underlying communication layer).
     * @param threadPool thread pool to use
     */
    public ReportingNotificationReceiver(
            @Nonnull IMessageReceiver<ReportNotification> messageReceiver,
            @Nonnull ExecutorService threadPool) {
        super(Objects.requireNonNull(messageReceiver), threadPool);
    }

    @Override
    protected void processMessage(@Nonnull ReportNotification message)
            throws ApiClientException, InterruptedException {
        final long reportId = message.getReportId();
        getLogger().trace("Received notification {} for report {}", message::getNotificationCase,
                message::getReportId);
        final Consumer<ReportListener> notification = createNotificator(message);
        for (final ReportListener listener : listeners) {
            getExecutorService().execute(() -> notification.accept(listener));
        }
    }

    @Nonnull
    private static Consumer<ReportListener> createNotificator(@Nonnull ReportNotification message)
            throws ApiClientException {
        switch (message.getNotificationCase()) {
            case GENERATED:
                return listener -> listener.onReportGenerated(message.getReportId());
            case FAILED:
                return listener -> listener.onReportFailed(message.getReportId(),
                        message.getFailed());
            default:
                throw new ApiClientException(
                        "Notification type not recognized: " + message.toString());
        }
    }

    /**
     * Registers a listener to receive reports status notifications.
     *
     * @param listener listener to add
     */
    public void addListener(@Nonnull ReportListener listener) {
        listeners.add(Objects.requireNonNull(listener));
    }
}
