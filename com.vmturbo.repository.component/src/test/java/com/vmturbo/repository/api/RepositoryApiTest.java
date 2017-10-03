package com.vmturbo.repository.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.repository.RepositoryApiConfig;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.api.RepositoryDTO.RepositoryNotification;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

/**
 * Repository API notifications test.
 */
public class RepositoryApiTest {

    private static final long TIMEOUT = 30000;
    private static final long TOPOLOGY_ID = 23466L;
    private static final long CONTEXT_ID = 8798634L;

    @Rule
    public TestName testName = new TestName();
    private ExecutorService threadPool;
    private IntegrationTestServer server;
    private RepositoryNotificationReceiver client;
    private WebsocketNotificationReceiver messageReceiver;
    private RepositoryListener listener;

    @Before
    public void init() throws Exception {
        threadPool = Executors.newCachedThreadPool();
        server = new IntegrationTestServer(testName, RepositoryApiConfig.class);
        messageReceiver = new WebsocketNotificationReceiver(server.connectionConfig(),
                RepositoryNotificationReceiver.WEBSOCKET_PATH, threadPool,
                RepositoryNotification::parseFrom);
        client = new RepositoryNotificationReceiver(messageReceiver, threadPool);
        listener = Mockito.mock(RepositoryListener.class);
        client.addListener(listener);
        server.waitForRegisteredEndpoints(1, TIMEOUT);
    }

    @After
    public void shutdown() throws Exception {
        messageReceiver.close();
        server.close();
        threadPool.shutdownNow();
    }

    /**
     * Tests notification about topology is available.
     *
     * @throws Exception if errors occur.
     */
    @Test
    public void testTopologyAvailableNotification() throws Exception {
        final RepositoryNotificationSender notificationSender =
                server.getBean(RepositoryNotificationSender.class);
        notificationSender.onProjectedTopologyAvailable(TOPOLOGY_ID, CONTEXT_ID);
        final ArgumentCaptor<Long> topologyCaptor = ArgumentCaptor.forClass(Long.class);
        final ArgumentCaptor<Long> contextCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onProjectedTopologyAvailable(topologyCaptor.capture(), contextCaptor.capture());
        Mockito.verify(listener, Mockito.never())
                .onProjectedTopologyFailure(Mockito.anyLong(), Mockito.anyLong(),
                        Mockito.anyString());
        Assert.assertEquals(TOPOLOGY_ID, (long)topologyCaptor.getValue());
        Assert.assertEquals(CONTEXT_ID, (long)contextCaptor.getValue());
    }

    /**
     * Tests notification about topology is failed to store in repository.
     *
     * @throws Exception if errors occur.
     */
    @Test
    public void testTopologyFailureNotification() throws Exception {
        final RepositoryNotificationSender notificationSender =
                server.getBean(RepositoryNotificationSender.class);
        final String message = "Avada kevadra";
        notificationSender.onProjectedTopologyFailure(TOPOLOGY_ID, CONTEXT_ID, message);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onProjectedTopologyFailure(TOPOLOGY_ID, CONTEXT_ID, message);
        Mockito.verify(listener, Mockito.never())
                .onProjectedTopologyAvailable(Mockito.anyLong(), Mockito.anyLong());
    }
}
