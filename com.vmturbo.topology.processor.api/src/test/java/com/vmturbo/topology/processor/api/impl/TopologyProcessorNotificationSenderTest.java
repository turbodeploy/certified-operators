package com.vmturbo.topology.processor.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;

/**
 * Test to cover listeners addition to {@link TopologyProcessorNotificationReceiver}.
 */
public class TopologyProcessorNotificationSenderTest {

    private SenderReceiverPair<TopologyProcessorNotification> notificationReceiver;
    private SenderReceiverPair<Topology> liveTopologyReceiver;
    private SenderReceiverPair<Topology> planTopologyReceiver;
    private SenderReceiverPair<TopologySummary> topologySummaryReceiver;
    private SenderReceiverPair<EntitiesWithNewState> entitiesWithNewStateReceiver;
    private ExecutorService threadPool;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void init() throws Exception {
        threadPool = Executors.newCachedThreadPool();
        notificationReceiver = new SenderReceiverPair<>();
        liveTopologyReceiver = new SenderReceiverPair<>();
        planTopologyReceiver = new SenderReceiverPair<>();
        topologySummaryReceiver = new SenderReceiverPair<>();
    }

    /**
     * Tests when all the subscriptions are available. All the listeners are expected to be added
     * without problems.
     *
     * @throws Exception if error occurred
     */
    @Test
    public void testAllSubscriptions() throws Exception {
        final TopologyProcessorNotificationReceiver receiver =
                new TopologyProcessorNotificationReceiver(notificationReceiver,
                        liveTopologyReceiver, planTopologyReceiver, topologySummaryReceiver,
                        entitiesWithNewStateReceiver, threadPool);
        checkNotification(receiver);
        checkLiveTopology(receiver);
        checkPlanTopology(receiver);
    }

    /**
     * Tests when there is not subscription to notifications. Notifications listener should fail
     * to be added.
     *
     * @throws Exception if error occurred
     */
    @Test
    public void testNoNotifications() throws Exception {
        final TopologyProcessorNotificationReceiver receiver =
                new TopologyProcessorNotificationReceiver(null, liveTopologyReceiver,
                        planTopologyReceiver, topologySummaryReceiver, entitiesWithNewStateReceiver,
                        threadPool);
        expectedException.expect(UnsupportedOperationException.class);
        checkNotification(receiver);
    }

    /**
     * Tests when there is not subscription to live topology. Live topology listener should fail
     * to be added.
     *
     * @throws Exception if error occurred
     */
    @Test
    public void testNoLiveTopology() throws Exception {
        final TopologyProcessorNotificationReceiver receiver =
                new TopologyProcessorNotificationReceiver(notificationReceiver, null,
                        planTopologyReceiver, topologySummaryReceiver, entitiesWithNewStateReceiver,
                        threadPool);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("no subscription");
        checkLiveTopology(receiver);
    }

    /**
     * Tests when there is not subscription to plan topology. Plan topology listener should fail
     * to be added.
     *
     * @throws Exception if error occurred
     */
    @Test
    public void testNoPlanTopology() throws Exception {
        final TopologyProcessorNotificationReceiver receiver =
                new TopologyProcessorNotificationReceiver(notificationReceiver,
                        liveTopologyReceiver, null, topologySummaryReceiver,
                        entitiesWithNewStateReceiver, threadPool);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("no subscription");
        checkPlanTopology(receiver);
    }

    private void checkNotification(@Nonnull TopologyProcessorNotificationReceiver receiver) {
        receiver.addProbeListener(new ProbeListener() {
            @Override
            public void onProbeRegistered(@Nonnull TopologyProcessorDTO.ProbeInfo probe) {
            }
        });
        receiver.addTargetListener(new TargetListener() {
        });
    }

    private void checkLiveTopology(@Nonnull TopologyProcessorNotificationReceiver receiver) {
        receiver.addLiveTopoListener((info, iterator) -> {});
    }

    private void checkPlanTopology(@Nonnull TopologyProcessorNotificationReceiver receiver) {
        receiver.addPlanTopoListener((info, iterator) -> {});
    }
}
