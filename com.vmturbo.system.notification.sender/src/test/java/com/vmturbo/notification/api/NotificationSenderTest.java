package com.vmturbo.notification.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.License;

/**
 * test case for {@link NotificationSender}
 */
public class NotificationSenderTest {

    public static final String DESCRIPTION = "description";
    public static final String SHORT_DESCRIPTION = "short description";
    public static final String TARGETNAME = "targetname";
    public static final long VALUE = 111l;
    private IMessageSender<SystemNotification> sender = mock(IMessageSender.class);
    private NotificationSender notificationSender = new NotificationSender(sender, Clock.systemUTC());

    @Test
    public void sendNotification() throws CommunicationException, InterruptedException {
        notificationSender.sendNotification(
                Category.newBuilder().setLicense(License.newBuilder().build()).build(),
                DESCRIPTION,
                SHORT_DESCRIPTION,
                Severity.CRITICAL,
                State.NOTIFY,
                Optional.of(VALUE));
        final SystemNotification systemNotification = SystemNotification.newBuilder()
                .setBroadcastId(1)
                .setCategory(Category.newBuilder().setLicense(License.newBuilder().build()).build())
                .setDescription(DESCRIPTION)
                .setShortDescription(SHORT_DESCRIPTION)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(VALUE)
                .build();
        verify(sender).sendMessage(systemNotification);
    }
}