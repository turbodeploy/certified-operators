package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
import com.vmturbo.priceindex.api.impl.PriceIndexReceiver;

public class PriceIndexStubTest {
    @Test
    public void testPriceIndexStub() throws InterruptedException, ExecutionException, TimeoutException {
        final PriceIndexStub stub = new PriceIndexStub();
        final long topologyId = 12345L;
        final long creationTime = 23456L;

        try (final ComponentStubHost server = ComponentStubHost.newBuilder()
                .withNotificationStubs(stub)
                .build()) {

            server.start();

            final ComponentApiConnectionConfig connectionConfig =
                ComponentApiConnectionConfig.newBuilder()
                    .setHostAndPort("localhost", ComponentUtils.GLOBAL_HTTP_PORT)
                    .build();
            final IMessageReceiver<PriceIndexMessage> messageReceiver = new
                    WebsocketNotificationReceiver<>(connectionConfig, PriceIndexReceiver
                    .WEBSOCKET_PATH, Executors.newCachedThreadPool(),
                    PriceIndexMessage::parseFrom);
            final PriceIndexReceiver client = PriceIndexReceiver.rpcAndNotification(
                connectionConfig, Executors.newCachedThreadPool(), messageReceiver);
            final CompletableFuture<PriceIndexMessage> priceIndexFuture = new CompletableFuture<>();
            client.setPriceIndexListener(priceIndexFuture::complete);

            stub.waitForEndpoints(1, 10, TimeUnit.SECONDS);
            stub.getBackend().sendPriceIndex(topologyId, creationTime,
                PriceIndexMessage.newBuilder()
                    .setMarketId(999L)
                    .setTopologyId(topologyId)
                    .setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                    .addPayload(PriceIndexMessagePayload.newBuilder()
                            .setOid(123L)
                            .setPriceindexCurrent(0.5)
                            .setPriceindexProjected(1.5)
                    ).build());

            final PriceIndexMessage priceIndex = priceIndexFuture.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(topologyId, priceIndex.getTopologyId());
            Assert.assertEquals(creationTime, priceIndex.getSourceTopologyCreationTime());
        }
    }
}
