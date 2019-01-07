package com.vmturbo.notification;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;

import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.License;

/**
 * Test for {@link NotificationInMemoryStore}
 */
public class NotificationInMemoryStoreTest {

    private static final long OID1 = 111l;
    private static final long GENERATION_TIME1 = 111111111l;
    private static final String SHORT_DESCRIPTION1 = "short description";
    private static final String LONG_DESCRIPTION1 = "long description";
    private static final long BROADCAST_ID1 = 222l;

    private static final long OID2 = 2111l;
    private static final long GENERATION_TIME2 = 2111111111l;
    private static final String SHORT_DESCRIPTION2 = "2short description";
    private static final String LONG_DESCRIPTION2 = "2long description";
    private static final long BROADCAST_ID2 = 2222l;

    private static final SystemNotification notification1 = SystemNotification.newBuilder()
            .setGenerationTime(GENERATION_TIME1)
            .setState(State.NOTIFY)
            .setSeverity(Severity.CRITICAL)
            .setShortDescription(SHORT_DESCRIPTION1)
            .setDescription(LONG_DESCRIPTION1)
            .setCategory(Category.newBuilder().setLicense(License.newBuilder().build()))
            .setBroadcastId(BROADCAST_ID1)
            .build();

    private static final SystemNotification notification2 = SystemNotification.newBuilder()
            .setGenerationTime(GENERATION_TIME2)
            .setState(State.NOTIFY)
            .setSeverity(Severity.CRITICAL)
            .setShortDescription(SHORT_DESCRIPTION2)
            .setDescription(LONG_DESCRIPTION2)
            .setCategory(Category.newBuilder().setLicense(License.newBuilder().build()))
            .setBroadcastId(BROADCAST_ID2)
            .build();

    private final NotificationStore notificationStore =
            new NotificationInMemoryStore(1000, 1, TimeUnit.DAYS);

    @Before
    public void setUp() throws Exception {
        notificationStore.storeNotification(notification1);
        notificationStore.storeNotification(notification2);
    }

    @Test
    public void testStoreNotification() {
        assertThat(notificationStore.getNotification(BROADCAST_ID1).get(),
                samePropertyValuesAs(notification1));
    }

    @Test
    public void testGetNotificationNumberByDate() {
        assertThat(notificationStore.getNotificationCountAfterTimestamp(GENERATION_TIME2 - 1),
                is(1L));
    }

    @Test
    public void testGetAllNotifications() {
        assertThat(notificationStore.getAllNotifications(), hasItems(notification1, notification2));
    }

    @Test
    public void testGetNotification() {
        assertThat(notificationStore.getNotification(BROADCAST_ID1).get(),
                samePropertyValuesAs(notification1));
    }

    @Test
    public void testCacheInvitationByNumOfEntries() throws InterruptedException {
        final NotificationStore store =
                new NotificationInMemoryStore(1l, 1, TimeUnit.DAYS);
        store.storeNotification(notification1);
        store.storeNotification(notification2);
        assertEquals(1, store.getAllNotifications().size());
    }

    @Test
    public void testCacheInvalidationByTime() throws InterruptedException {
        final NotificationStore store =
                new NotificationInMemoryStore(100l, 2l, TimeUnit.NANOSECONDS);
        store.storeNotification(notification1);
        Thread.sleep(300);
        assertEquals(0, store.getAllNotifications().size());
    }
}