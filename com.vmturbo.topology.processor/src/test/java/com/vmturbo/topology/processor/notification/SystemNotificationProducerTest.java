package com.vmturbo.topology.processor.notification;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.notification.api.NotificationSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO.Severity;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Tests that SystemNotificationProducer translates and sends the notifications.
 */
public class SystemNotificationProducerTest {

    private static final List<NotificationDTO> NOTIFICATION_DTOS = Arrays.asList(
        createNotificationDTO("CRITICAL notification", Severity.CRITICAL),
        createNotificationDTO("MAJOR notification", Severity.MAJOR),
        createNotificationDTO("MINOR notification", Severity.MINOR),
        createNotificationDTO("NORMAL notification", Severity.NORMAL),
        createNotificationDTO("UNKNOWN notification", Severity.UNKNOWN));

    private static final long TARGET_OID = 0L;
    private static final String TARGET_NAME = "AWS Development";

    private NotificationSender notificationSender;

    private SystemNotificationProducer systemNotificationProducer;

    private Target target;

    private static NotificationDTO createNotificationDTO(String description, Severity severity) {
        return NotificationDTO.newBuilder()
            .setCategory("UNUSED")
            .setDescription(description)
            .setEvent("UNUSED")
            .setSeverity(severity)
            .setSubCategory("UNUSED")
            .build();
    }

    /**
     * Sets up the mocks used by all the tests.
     */
    @Before
    public void before() {
        notificationSender = mock(NotificationSender.class);
        systemNotificationProducer = new SystemNotificationProducer(notificationSender);
        target = mock(Target.class);
        when(target.getId()).thenReturn(TARGET_OID);
        when(target.getDisplayName()).thenReturn(TARGET_NAME);
    }

    /**
     * Null notification dto list should not throw an exception.
     */
    @Test
    public void testNull() {
        systemNotificationProducer.sendSystemNotification(null, target);
    }

    /**
     * When {@link NotificationSender} throws {@link CommunicationException}
     * or {@link InterruptedException}, then {@link SystemNotificationProducer#sendSystemNotification(List, Target)}
     * should not throw an exception. Instead it should log and return to prevent the discovery thread
     * from dying. If the discovery thread dies, all the entities discovered by that target will not
     * make it into the topology.
     *
     * @throws CommunicationException should not be thrown.
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testExceptionThrown() throws CommunicationException, InterruptedException {
        doThrow(new CommunicationException("don't crash"))
            .when(notificationSender).sendNotification(any(), any(), any(), any());
        systemNotificationProducer.sendSystemNotification(NOTIFICATION_DTOS, target);

        doThrow(new InterruptedException("don't crash"))
            .when(notificationSender).sendNotification(any(), any(), any(), any());
        systemNotificationProducer.sendSystemNotification(NOTIFICATION_DTOS, target);
    }

    /**
     * An exception should not be thrown when an empty list is used.
     */
    @Test
    public void testEmpty() {
        systemNotificationProducer.sendSystemNotification(Collections.emptyList(), target);
    }

    /**
     * Descriptions should be sent and the {@link NotificationDTO.Severity} should be translated to
     * {@link ActionDTO.Severity}.
     *
     * @throws CommunicationException should not be thrown.
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testNotificationsSent() throws CommunicationException, InterruptedException {
        ArgumentCaptor<Category> categoriesCaptor = ArgumentCaptor.forClass(Category.class);
        ArgumentCaptor<String> descriptionsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> shortDescriptionsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ActionDTO.Severity> severitiesCaptor = ArgumentCaptor.forClass(ActionDTO.Severity.class);
        doNothing().when(notificationSender).sendNotification(categoriesCaptor.capture(), descriptionsCaptor.capture(), shortDescriptionsCaptor.capture(), severitiesCaptor.capture());
        systemNotificationProducer.sendSystemNotification(NOTIFICATION_DTOS, target);

        List<SystemNotification> expectedDTOs = Arrays.asList(
            makeSystemNotification("CRITICAL notification", ActionDTO.Severity.CRITICAL),
            makeSystemNotification("MAJOR notification", ActionDTO.Severity.MAJOR),
            makeSystemNotification("MINOR notification", ActionDTO.Severity.MINOR),
            makeSystemNotification("NORMAL notification", ActionDTO.Severity.NORMAL),
            makeSystemNotification("UNKNOWN notification", ActionDTO.Severity.NORMAL));

        List<Category> actualCategories = categoriesCaptor.getAllValues();
        List<String> actualDescriptions = descriptionsCaptor.getAllValues();
        List<String> actualShortDescriptions = shortDescriptionsCaptor.getAllValues();
        List<ActionDTO.Severity> actualSeverities = severitiesCaptor.getAllValues();
        Assert.assertEquals(expectedDTOs.size(), actualCategories.size());
        for (int i = 0; i < expectedDTOs.size(); i++) {
            SystemNotification expectedDTO = expectedDTOs.get(i);
            SystemNotification.Target expectedTarget = expectedDTO.getCategory().getTarget();
            SystemNotification.Target actualTarget = actualCategories.get(i).getTarget();
            Assert.assertEquals(expectedTarget.getOid(), actualTarget.getOid());
            Assert.assertEquals(expectedTarget.getDisplayName(), actualTarget.getDisplayName());

            Assert.assertEquals(expectedDTO.getShortDescription(), actualShortDescriptions.get(i));
            Assert.assertEquals(expectedDTO.getDescription(), actualDescriptions.get(i));
            Assert.assertEquals(expectedDTO.getSeverity(), actualSeverities.get(i));
        }
    }

    private static SystemNotification makeSystemNotification(String description, ActionDTO.Severity severity) {
        return SystemNotification.newBuilder()
                .setCategory(Category.newBuilder()
                    .setTarget(SystemNotification.Target.newBuilder()
                        .setOid(TARGET_OID)
                        .setDisplayName(TARGET_NAME)
                        .build())
                    .build())
                .setShortDescription("")
                .setDescription(description)
                .setSeverity(severity)
                .buildPartial();
    }

}
