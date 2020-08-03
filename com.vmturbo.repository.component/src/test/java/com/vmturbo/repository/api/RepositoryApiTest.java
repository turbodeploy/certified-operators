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

import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.repository.RepositoryNotificationSender;
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
    private RepositoryNotificationReceiver client;
    private RepositoryNotificationSender notificationSender;

    private RepositoryListener listener;

    @Before
    public void init() throws Exception {
        threadPool = Executors.newCachedThreadPool();
        final SenderReceiverPair<RepositoryNotification> notificationsChannel =
                new SenderReceiverPair<>();
        client = new RepositoryNotificationReceiver(notificationsChannel, threadPool, 0);
        notificationSender = new RepositoryNotificationSender(notificationsChannel);
        listener = Mockito.mock(RepositoryListener.class);
        client.addListener(listener);
    }

    @After
    public void shutdown() throws Exception {
        threadPool.shutdownNow();
    }

    /**
     * Tests notification about topology is available.
     *
     * @throws Exception if errors occur.
     */
    @Test
    public void testTopologyAvailableNotification() throws Exception {
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
        final String message = "Avada kevadra";
        notificationSender.onProjectedTopologyFailure(TOPOLOGY_ID, CONTEXT_ID, message);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onProjectedTopologyFailure(TOPOLOGY_ID, CONTEXT_ID, message);
        Mockito.verify(listener, Mockito.never())
                .onProjectedTopologyAvailable(Mockito.anyLong(), Mockito.anyLong());
    }
}
