package com.vmturbo.history.notifications;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.Record3;

import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.history.stats.readers.VolumeAttachmentHistoryReader;

/**
 * It enables the history component to send notifications to.
 */
public class VolAttachmentDaysSender extends
        ComponentNotificationSender<VolAttachmentHistoryOuterClass.VolAttachmentHistory> {

    /**
     * The sender of the history volume unattached days notifications.
     */
    private final IMessageSender<VolAttachmentHistoryOuterClass.VolAttachmentHistory> notificationSender;
    /**
     * The reader for reading volume attachment history.
     */
    private final VolumeAttachmentHistoryReader reader;
    private final ExecutorService executorService;
    private Logger logger = LogManager.getLogger();

    /**
     * The constructor of the history notification sender.
     *
     * @param notificationSender The sender of history volume with last attachment time notification messages.
     * @param executorService executor service.
     */
    public VolAttachmentDaysSender(
            @Nonnull IMessageSender<VolAttachmentHistoryOuterClass.VolAttachmentHistory> notificationSender,
            VolumeAttachmentHistoryReader reader, ExecutorService executorService) {
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.reader = reader;
        this.executorService = executorService;
    }

    /**
     * Sends volume attachment notification.
     *
     * @param volumeList list of unattached volumes Oids.
     * @param  topologyId .topology id.
     */
    public void sendNotification(List<Long> volumeList, final long topologyId) {
        executorService.submit(() -> {
            readAndSendVolumeNotification(volumeList, topologyId);
        });
    }

    /**
     * read and sends volume attachment notification.
     *
     * @param volumeList list of unattached volumes Oids.
     * @param  topologyId .topology id.
     */
    public void readAndSendVolumeNotification(List<Long> volumeList, final long topologyId) {

        final HashMap<Long, Long> volIdToLastAttachmentTime = new HashMap<>();
        List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(volumeList);
        records.stream().forEach(record -> {
            Long volOid = record.component1();
            final java.sql.Date lastAttachedDate = record.component3();
            volIdToLastAttachmentTime.put(volOid, lastAttachedDate.getTime());
        });
        logger.info("Sending Volume Unattached Notification for " + volIdToLastAttachmentTime.size() + " volumes and Topology ID = " + topologyId);
        sendVolumeUnAttachedDays(volIdToLastAttachmentTime, topologyId);
    }

    /**
     * Sends a history volumes notification.
     *
     * @param value The volumes unattached days notification
     */
    public void sendVolumeUnAttachedDays(final  HashMap<Long, Long> value, final long topologyId) {
        try {
            sendMessage(notificationSender, formMessage(value, topologyId));
        } catch (InterruptedException | CommunicationException e) {
            getLogger().error("An error happened while sending number of days unattached."
                    + "notification.", e);
        }
    }

    /**
     * Forms a history volumes notification message.
     *
     * @param value The map of volumes oids with unattached days notification
     */
    public VolAttachmentHistoryOuterClass.VolAttachmentHistory formMessage(final HashMap<Long, Long> value, long topologyId) {
        final VolAttachmentHistoryOuterClass.VolAttachmentHistory.Builder volDaysNotificationBuilder =
                VolAttachmentHistoryOuterClass.VolAttachmentHistory.newBuilder().putAllVolIdToLastAttachedDate(value)
                        .setTopologyId(topologyId);
        return volDaysNotificationBuilder.build();
    }

    @Override
    protected String describeMessage(@NotNull VolAttachmentHistoryOuterClass.VolAttachmentHistory volUnAttachmentDays) {
        return VolAttachmentHistoryOuterClass.VolAttachmentHistory.class.getSimpleName() + "["
                + volUnAttachmentDays.getTopologyId() + "]";
    }
}
