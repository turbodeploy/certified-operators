package com.vmturbo.sample.component.notifications;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.sample.api.EchoListener;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;
import com.vmturbo.sample.api.impl.SampleComponentNotificationReceiver;

/**
 * This is the unit test for the {@link SampleComponentNotificationSender}, although it really
 * straddles the line between unit test and integration test.
 */
public class SampleNotificationsTest {

    private static final String ECHO = "test";

    private static final int TIMEOUT_MS = 10000;

    /**
     * We stand up an "IntegrationTestServer" which creates a Spring web application using
     * the provided configuration (in this case {@link SampleNotificationsTestConfig}).
     * We can then point the {@link SampleComponentNotificationReceiver} at the test server, trigger
     * notifications on the server, and verify that the client receives them as expected.
     */
    private IntegrationTestServer server;

    private SampleComponentNotificationReceiver sampleComponentNotificationReceiver;

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    private SampleComponentNotificationSender notificationsBackend;

    private IMessageReceiver<SampleNotification> messageReceiver;

    @Rule
    public TestName testName = new TestName();

    @Captor
    private ArgumentCaptor<EchoResponse> notificationCaptor;

    @Before
    public void init() throws Exception {
        // Required to initialize the notification captor.
        MockitoAnnotations.initMocks(this);

        // Stand up the integration test server using the Spring configuration.
        // The test name is used for logging/debugging purposes.
        server = new IntegrationTestServer(testName, SampleNotificationsTestConfig.class);
        messageReceiver =
                (IMessageReceiver<SampleNotification>)server.getBean(IMessageReceiver.class);

        // Get the EchoNotificationsBackend initialized in the Spring context of the
        // IntegrationTestServer.
        notificationsBackend = server.getBean(SampleComponentNotificationSender.class);

        // Create a component client that connects to the test server.
        sampleComponentNotificationReceiver =
                new SampleComponentNotificationReceiver(messageReceiver, threadPool);
    }

    @After
    public void close() throws Exception {
        server.close();
        threadPool.shutdownNow();
    }

    @Test
    public void testEchoNotification() throws Exception {
        // First, add a mock listener to the client.
        final EchoListener listener = Mockito.mock(EchoListener.class);
        sampleComponentNotificationReceiver.addListener(listener);

        // Trigger a notification on the server.
        notificationsBackend.notifyEchoResponse(EchoResponse.newBuilder()
            .setEcho(ECHO)
            .build());

        // Verify that the listener we added receives the notification.
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS))
                   .onEchoResponse(notificationCaptor.capture());

        final EchoResponse response = notificationCaptor.getValue();
        Assert.assertEquals(ECHO, response.getEcho());
    }
}
