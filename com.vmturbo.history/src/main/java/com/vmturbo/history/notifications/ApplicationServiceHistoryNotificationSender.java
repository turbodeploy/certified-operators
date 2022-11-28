package com.vmturbo.history.notifications;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification.DaysEmptyInfo;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification.Type;
import com.vmturbo.history.stats.readers.ApplicationServiceDaysEmptyReader;

/**
 * Class which enables the history component to send app service days empty notifications to the
 * topology processor.
 */
public class ApplicationServiceHistoryNotificationSender extends
        ComponentNotificationSender<ApplicationServiceHistoryNotification> {

    private final IMessageSender<ApplicationServiceHistoryNotification> msgSender;
    private final ApplicationServiceDaysEmptyReader reader;
    private final ExecutorService executorService;
    private final Logger logger = LogManager.getLogger();

    /**
     * The constructor of the app service days empty notification sender.
     *
     * @param notificationSender The sender.
     * @param executorService The executor service.
     */
    public ApplicationServiceHistoryNotificationSender(
            @Nonnull IMessageSender<ApplicationServiceHistoryNotification> notificationSender,
            @Nonnull ApplicationServiceDaysEmptyReader reader,
            @Nonnull ExecutorService executorService) {
        this.msgSender = Objects.requireNonNull(notificationSender);
        this.reader = Objects.requireNonNull(reader);
        this.executorService = Objects.requireNonNull(executorService);
    }

    /**
     * Sends app service days empty notification.
     *
     * @param  topologyId The topology id.
     */
    public void sendDaysEmptyNotification(final long topologyId) {
        executorService.submit(() -> {
            try {
                List<DaysEmptyInfo> daysEmptyInfos = reader.getApplicationServiceDaysEmptyInfo();
                ApplicationServiceHistoryNotification appSvcHistoryNotification =
                        ApplicationServiceHistoryNotification.newBuilder()
                        .setType(Type.DAYS_EMPTY)
                        .addAllDaysEmptyInfo(daysEmptyInfos)
                        .setTopologyId(topologyId)
                        .build();
                sendMessage(msgSender, appSvcHistoryNotification);
            } catch (Exception e) {
                logger.error("Error sending days empty notification for topology " + topologyId, e);
            }
        });
    }

    @Override
    protected String describeMessage(@NotNull ApplicationServiceHistoryNotification appHistoryNotification) {
        // This message is displayed in a log statement when the message is sent
        return String.format("[msg=%s, type=%s, daysEmptyInfoSize=%s topologyId=%s]",
                ApplicationServiceHistoryNotification.class.getSimpleName(),
                appHistoryNotification.getType(),
                appHistoryNotification.getDaysEmptyInfoList().size(),
                appHistoryNotification.getTopologyId());
    }
}
