package com.vmturbo.cost.component;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.cost.api.CostComponent;
import com.vmturbo.cost.api.impl.CostComponentImpl;
import com.vmturbo.cost.component.notification.TopologyCostSender;
import com.vmturbo.cost.component.util.IntegrationTestConfig;

/**
 * Test for message senders/receivers.
 */
public class CostApiIntegrationTest {

    private TopologyCostSender sender;

    private CostComponent costListener;

    /**
     * This makes the IntegrationTestServer work.
     */
    @Rule
    public TestName testName = new TestName();

    /**
     * Set up test sender.
     *
     * @throws Exception never
     */
    @Before
    public void setup() throws Exception {
        final IntegrationTestServer integrationTestServer =
                new IntegrationTestServer(testName, IntegrationTestConfig.class);
        sender = integrationTestServer.getBean("topologyCostSender");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().build();
        costListener = new CostComponentImpl(null, integrationTestServer.getBean("chunkSender"),
                Executors.newCachedThreadPool(threadFactory), 0);
    }

    /**
     * Test that the TopologyCostListener receives messages appropriately.
     *
     * @throws CommunicationException never
     * @throws InterruptedException never
     */
    @Test
    public void testTopologyCostListener() throws CommunicationException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final TestTopologyCostListener listener = new TestTopologyCostListener(latch);
        costListener.addTopologyCostListener(listener);
        sender.sendTopologyCostChunk(TopologyOnDemandCostChunk.getDefaultInstance());
        latch.await();
        assertEquals(1, listener.getChunks().size());
    }
}
