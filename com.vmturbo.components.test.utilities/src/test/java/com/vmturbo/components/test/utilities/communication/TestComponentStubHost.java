package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;

public class TestComponentStubHost {

    /**
     * Test that {@link ComponentStubHost} works properly with two websocket component stubs.
     */
    @Test
    public void testTwoStubs() throws TimeoutException, InterruptedException, ExecutionException {
        final MarketStub marketStub = new MarketStub();
        final TopologyProcessorStub topologyProcessorStub = new TopologyProcessorStub();

        try (ComponentStubHost server = ComponentStubHost.newBuilder()
                .withNotificationStubs(marketStub, topologyProcessorStub)
                .build()) {
            server.start();
            final ExecutorService threadPool = Executors.newCachedThreadPool();
            final ComponentApiConnectionConfig config = ComponentApiConnectionConfig.newBuilder()
                    .setHostAndPort("localhost", ComponentUtils.GLOBAL_HTTP_PORT)
                    .build();

            final IMessageReceiver<MarketComponentNotification> messageReceiver =
                    new WebsocketNotificationReceiver<>(config,
                            MarketComponentClient.WEBSOCKET_PATH, threadPool,
                            MarketComponentNotification::parseFrom);
            // Test that the market component stub works.
            final MarketComponent mkt =
                    MarketComponentClient.rpcAndNotification(config, threadPool, messageReceiver);

            final CompletableFuture<ActionPlan> actionPlanFuture = new CompletableFuture<>();
            mkt.addActionsListener(actionPlanFuture::complete);

            marketStub.waitForEndpoints(1, 10, TimeUnit.SECONDS);
            marketStub.getBackend().notifyActionsRecommended(
                    ActionPlan.newBuilder()
                        .setId(1)
                        .build());

            final ActionPlan actionPlan = actionPlanFuture.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(1, actionPlan.getId());

            // TODO add real message receivers here (OM-25222)
            if (true) {
                throw new UnsupportedOperationException("Implement real message receiver");
            }
            // Test that the topology processor stub works.
            final IMessageReceiver<TopologyProcessorNotification> tpMessageReceiver = null;
            final IMessageReceiver<Topology> tpTopologyReceiver = null;
//                    new WebsocketNotificationReceiver<>(config,
//                            TopologyProcessorClient.WEBSOCKET_PATH, threadPool,
//                            TopologyProcessorNotification::parseFrom);
            final TopologyProcessor tp =
                    TopologyProcessorClient.rpcAndNotification(config, threadPool,
                            tpMessageReceiver, tpTopologyReceiver);
            final CompletableFuture<TopologyInfo> topologyContextIdFuture = new CompletableFuture<>();
            tp.addEntitiesListener((topologyInfo, topologyDTOs) ->
                    topologyContextIdFuture.complete(topologyInfo));

            topologyProcessorStub.waitForEndpoints(1, 10, TimeUnit.SECONDS);

            final TopologyBroadcast broadcast =
                    topologyProcessorStub.getBackend().broadcastTopology(10, 7, TopologyType.REALTIME);
            broadcast.finish();

            Assert.assertEquals(Long.valueOf(10), Long.valueOf(topologyContextIdFuture
                    .get(10, TimeUnit.SECONDS).getTopologyContextId()));
        }
    }
}
