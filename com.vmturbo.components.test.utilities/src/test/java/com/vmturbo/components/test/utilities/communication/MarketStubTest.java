package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

public class MarketStubTest {


    @Test
    public void testMarketStub()
            throws InterruptedException, ExecutionException, TimeoutException {
        final MarketStub stub = new MarketStub();
        try (final ComponentStubHost server = ComponentStubHost.newBuilder()
                .withNotificationStubs(stub)
                .build()) {

            server.start();

            final MarketComponent component = MarketComponentClient.rpcAndNotification(
                    ComponentApiConnectionConfig.newBuilder()
                            .setHostAndPort("localhost", ComponentUtils.GLOBAL_HTTP_PORT)
                            .build(), Executors.newCachedThreadPool());

            final CompletableFuture<ActionPlan> actionPlanFuture = new CompletableFuture<>();
            component.addActionsListener(actionPlanFuture::complete);

            stub.getBackend().waitForEndpoints(1, 10, TimeUnit.SECONDS);
            stub.getBackend().notifyActionsRecommended(ActionPlan.newBuilder().setId(1).build());

            final ActionPlan actionPlan = actionPlanFuture.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(1, actionPlan.getId());
        }
    }
}
