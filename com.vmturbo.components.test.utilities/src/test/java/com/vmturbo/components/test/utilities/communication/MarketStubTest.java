package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Test for market stub.
 */
public class MarketStubTest {

    private ExecutorService threadPool;

    @Before
    public void init() {
        threadPool = Executors.newCachedThreadPool();
    }

    @After
    public void clear() {
        threadPool.shutdownNow();
    }

    @Test
    public void testMarketStub()
            throws InterruptedException, ExecutionException, TimeoutException {
        final MarketStub stub = new MarketStub();
        try (final ComponentStubHost server = ComponentStubHost.newBuilder()
                .withNotificationStubs(stub)
                .build()) {

            server.start();

            final ComponentApiConnectionConfig connectionConfig =
                    ComponentApiConnectionConfig.newBuilder()
                            .setHostAndPort("localhost", ComponentUtils.GLOBAL_HTTP_PORT)
                            .build();
            final IMessageReceiver<MarketComponentNotification> messageReceiver =
                    new WebsocketNotificationReceiver<>(connectionConfig,
                            MarketComponentClient.WEBSOCKET_PATH, threadPool,
                            MarketComponentNotification::parseFrom);

            final MarketComponent component =
                    MarketComponentClient.rpcAndNotification(connectionConfig,
                            threadPool, messageReceiver);

            final CompletableFuture<ActionPlan> actionPlanFuture = new CompletableFuture<>();
            component.addActionsListener(actionPlanFuture::complete);

            stub.getBackend().waitForEndpoints(1, 10, TimeUnit.SECONDS);
            stub.getBackend().notifyActionsRecommended(ActionPlan.newBuilder().setId(1).build());

            final ActionPlan actionPlan = actionPlanFuture.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(1, actionPlan.getId());
        }
    }
}
