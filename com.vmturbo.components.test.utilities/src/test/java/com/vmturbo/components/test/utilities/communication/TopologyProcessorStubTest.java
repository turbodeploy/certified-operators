package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;

public class TopologyProcessorStubTest {

    @Test
    public void testTopologyProcessorStub()
            throws InterruptedException, TimeoutException, ExecutionException {
        final TopologyProcessorStub tpStub = new TopologyProcessorStub();
        try (final ComponentStubHost server = ComponentStubHost.newBuilder()
                .withNotificationStubs(tpStub)
                .build()) {

            server.start();

            final ComponentApiConnectionConfig connectionConfig =
                    ComponentApiConnectionConfig.newBuilder()
                            .setHostAndPort("localhost", ComponentUtils.GLOBAL_HTTP_PORT)
                            .build();
            final IMessageReceiver<TopologyProcessorNotification> messageReceiver =
                    new WebsocketNotificationReceiver<>(connectionConfig,
                            TopologyProcessorClient.WEBSOCKET_PATH, Executors.newCachedThreadPool(),
                            TopologyProcessorNotification::parseFrom);
            final TopologyProcessor tpClient =
                    TopologyProcessorClient.rpcAndNotification(connectionConfig,
                            Executors.newCachedThreadPool(), messageReceiver);

            final CompletableFuture<TopologyInfo> topologyContextIdFuture = new CompletableFuture<>();
            tpClient.addEntitiesListener((topologyInfo, topologyDTOs) ->
                    topologyContextIdFuture.complete(topologyInfo));

            tpStub.getBackend().waitForEndpoints(1, 10, TimeUnit.SECONDS);

            final TopologyBroadcast broadcast =
                    tpStub.getBackend().broadcastTopology(10, 7, TopologyType.REALTIME);
            broadcast.finish();

            Assert.assertEquals(Long.valueOf(10), Long.valueOf(topologyContextIdFuture
                    .get(10, TimeUnit.SECONDS).getTopologyContextId()));
        }
    }
}
