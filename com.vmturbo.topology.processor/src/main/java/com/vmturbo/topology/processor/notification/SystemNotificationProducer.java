package com.vmturbo.topology.processor.notification;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.notification.api.NotificationSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Handles translating {@link NotificationDTO} to {@link SystemNotification} and forwarding it
 * to whatever message sender is configured.
 */
public class SystemNotificationProducer {

    private static final Logger logger = LogManager.getLogger(SystemNotificationProducer.class);

    private final NotificationSender systemNotificationSender;

    /**
     * Initializes class to use systemNotificationSender to send system notifications to the
     * system.
     *
     * @param systemNotificationSender the class that implements how we send system notifications.
     */
    public SystemNotificationProducer(
            @Nonnull NotificationSender systemNotificationSender) {
        this.systemNotificationSender = systemNotificationSender;
    }

    /**
     * Translates {@link NotificationDTO} to {@link SystemNotification} and forwards it
     * to whatever message sender is configured.
     *
     * @param notificationList the notifications that need to be translated.
     * @param target the target that the notifications come from.
     */
    public void sendSystemNotification(
            @Nullable List<NotificationDTO> notificationList,
            @Nonnull Target target) {
        if (notificationList == null) {
            logger.warn("sendSystemNotification received notificationList=null and target=" + target);
            return;
        }

        try {
            for (NotificationDTO notification : notificationList) {
                /* Fields not forwarded towards API:
                       .setShortDescription()
                   Unused fields from SDK:
                       notification.getCategory();
                       notification.getSubCategory();
                       notification.getEvent();
                   In NotificationDTO the category is enum string, but in SystemNotification the category
                   is an object with values. I've hardcoded it to the Target Category and filled the
                   category with the display name and oid of the target the notification came from.
                 */
                systemNotificationSender.sendNotification(
                    Category.newBuilder()
                        .setTarget(SystemNotification.Target.newBuilder()
                            .setOid(target.getId())
                            .setDisplayName(target.getDisplayName())
                            .build())
                        .build(),
                    notification.getDescription(),
                    "",
                    convertSeverity(notification.getSeverity()));
            }
        } catch (CommunicationException | InterruptedException e) {
            logger.error(() -> "Unable to send notification from target oid=" + target.getId()
                + "displayName=" + target.getDisplayName()
                + " notifications=" + notificationList.toString(),
                e);
        }
    }

    private ActionDTO.Severity convertSeverity(@Nonnull NotificationDTO.Severity notificationSeverity) {
        switch (notificationSeverity) {
            case MAJOR:
                return ActionDTO.Severity.MAJOR;
            case MINOR:
                return ActionDTO.Severity.MINOR;
            case CRITICAL:
                return ActionDTO.Severity.CRITICAL;
            default:
                // UNKNOWN, NORMAL both need to map to NORMAL because there is no
                // ActionDTO.Severity.UNKNOWN
                return ActionDTO.Severity.NORMAL;
        }
    }
}
