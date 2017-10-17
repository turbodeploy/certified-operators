package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.NotImplementedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

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
    @Ignore("Temporarily disabled, while kafka is not accceptable from unit tests")
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

            if (true) {
                throw new NotImplementedException("Real Kafka message receiver should me " +
                        "implemented here");
            }
            final IMessageReceiver<ActionPlan> actionsReceiver = null;
            final IMessageReceiver<ProjectedTopology> topologyReceiver = null;

            final MarketComponent component =
                    MarketComponentClient.rpcAndNotification(connectionConfig,
                            threadPool, topologyReceiver, actionsReceiver);

            final CompletableFuture<ActionPlan> actionPlanFuture = new CompletableFuture<>();
            component.addActionsListener(actionPlanFuture::complete);

            stub.waitForEndpoints(1, 10, TimeUnit.SECONDS);
            stub.getBackend().notifyActionsRecommended(ActionPlan.newBuilder().setId(1).build());

            final ActionPlan actionPlan = actionPlanFuture.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(1, actionPlan.getId());
        }
    }
}
