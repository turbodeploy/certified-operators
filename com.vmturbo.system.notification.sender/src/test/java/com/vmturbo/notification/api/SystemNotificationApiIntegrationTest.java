package com.vmturbo.notification.api;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.License;
import com.vmturbo.notification.api.impl.NotificationReceiver;

/**
 * Integration test for system notification API client and server.
 */
public class SystemNotificationApiIntegrationTest {

    private static final int TIMEOUT_MS = 30000;

    private static final Logger logger = LogManager.getLogger();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public TestName testName = new TestName();
    private IntegrationTestServer integrationTestServer;
    private NotificationSender notificationSender;
    private NotificationReceiver notificationReceiver;
    @Captor
    private ArgumentCaptor<SystemNotification> argumentCaptor;

    private IMessageReceiver<SystemNotification> messageReceiver;

    @Before
    public final void init() throws Exception {

        MockitoAnnotations.initMocks(this);

        Thread.currentThread().setName(testName.getMethodName() + "-main");
        logger.debug("Starting @Before");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("clt-" + testName.getMethodName() + "-%d").build();
        final ExecutorService threadPool = Executors.newCachedThreadPool(threadFactory);

        integrationTestServer = new IntegrationTestServer(testName, TestApiServerConfig.class);
        messageReceiver = integrationTestServer.getBean("notificationsChannel");
        notificationReceiver = new NotificationReceiver(messageReceiver, threadPool, 0);

        notificationSender = integrationTestServer.getBean(NotificationSender.class);
        logger.debug("Finished @Before");
    }

    @After
    public final void shutdown() throws Exception {
        logger.debug("Starting @After");
        integrationTestServer.close();
        logger.debug("Finished @After");
    }

    /**
     * Test that system notifications on the server-side
     * propagate to clients, and clients return acks.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testNotifySystemNotification() throws Exception {
        final NotificationsListener listener = Mockito.mock(NotificationsListener.class);
        notificationReceiver.addListener(listener);

        final SystemNotification systemNotification = SystemNotification.newBuilder()
                .setBroadcastId(1)
                .setCategory(Category.newBuilder().setLicense(License.newBuilder().build()).build())
                .setDescription(NotificationSenderTest.DESCRIPTION)
                .setShortDescription(NotificationSenderTest.SHORT_DESCRIPTION)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(NotificationSenderTest.VALUE)
                .build();
        notificationSender.sendNotification(
                Category.newBuilder().setLicense(License.newBuilder().build()).build(),
                NotificationSenderTest.DESCRIPTION,
                NotificationSenderTest.SHORT_DESCRIPTION,
                Severity.CRITICAL,
                State.NOTIFY,
                Optional.of(NotificationSenderTest.VALUE));

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1)).onNotificationAvailable(argumentCaptor.capture());

        final SystemNotification receivedNotification = argumentCaptor.getValue();
        Assert.assertEquals(systemNotification, receivedNotification);
    }
}