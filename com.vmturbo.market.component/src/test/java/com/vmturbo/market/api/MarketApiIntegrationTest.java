package com.vmturbo.market.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;

/**
 * Integration test for Market API client and server.
 */
public class MarketApiIntegrationTest {

    private static final int TIMEOUT_MS = 30000;

    private static final Logger logger = LogManager.getLogger();

    private IntegrationTestServer integrationTestServer;

    private ExecutorService threadPool;

    private MarketNotificationSender notificationSender;

    protected MarketComponentNotificationReceiver market;

    @Rule
    public TestName testName = new TestName();

    @Captor
    private ArgumentCaptor<ActionPlan> actionCaptor;

    @Before
    public final void init() throws Exception {

        MockitoAnnotations.initMocks(this);

        Thread.currentThread().setName(testName.getMethodName() + "-main");
        logger.debug("Starting @Before");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                        .setNameFormat("clt-" + testName.getMethodName() + "-%d").build();
        threadPool = Executors.newCachedThreadPool(threadFactory);

        integrationTestServer = new IntegrationTestServer(testName, TestApiServerConfig.class);
        market = new MarketComponentNotificationReceiver(
                        integrationTestServer.getBean("projectedTopologySender"),
                        integrationTestServer.getBean("projectedEntityCostSender"),
                        integrationTestServer.getBean("projectedEntityRiCoverageSender"),
                        integrationTestServer.getBean("actionPlanSender"),
                        integrationTestServer.getBean("analysisSummarySender"),
                        integrationTestServer.getBean("analysisStatusSender"),
                        threadPool, 0);

        notificationSender = integrationTestServer.getBean(MarketNotificationSender.class);

        logger.debug("Finished @Before");
    }

    @After
    public final void shutdown() throws Exception {
        logger.debug("Starting @After");
        integrationTestServer.close();
        logger.debug("Finished @After");
    }

    /**
     * Test that action notifications on the server-side propagate to clients, and
     * clients return acks.
     *
     * @throws Exception
     *             If anything goes wrong.
     */
    @Test
    public void testNotifyActions() throws Exception {
        final ActionsListener listener = Mockito.mock(ActionsListener.class);
        market.addActionsListener(listener);

        ActionPlan actionPlan = ActionPlan.newBuilder()
            .setId(0L)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyType(TopologyType.REALTIME))))
            .addAction(createAction())
            .build();
        notificationSender.notifyActionsRecommended(actionPlan);

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onActionsReceived(actionCaptor.capture(), Mockito.any());

        final ActionPlan receivedActions = actionCaptor.getValue();
        assertThat(receivedActions, is(actionPlan));
    }

    /**
     * Test if projected reserved instance coverage notification is being sent and
     * is captured in test listener with the data that was sent
     *
     * @throws Exception
     *             If anything goes wrong.
     */
    @Test
    public void testNotifyRICoverage() throws Exception {
        // Original topology id
        long originalTopoId = 123l;
        // topology context of real time market
        long topoContextId = 456l;
        // Projected topology id
        long projectedTopologyId = 789l;
        // entity that has projected RI coverage data in map returned
        long entityId = 1l;
        // Reserved instance id that is covered in market
        long riId = 2l;
        // number of coupons used by market
        double numOfCoupons = 16d;
        CountDownLatch latch = new CountDownLatch(1);
        // Topology Info object to be used
        final ProjectedEntityRiReceiverForTest riCoverageReceiver =
                        new ProjectedEntityRiReceiverForTest();
        riCoverageReceiver.setLatch(latch);
        // Adding the new listener into market api
        market.addProjectedEntityRiCoverageListener(riCoverageReceiver);
        // Topology info object to be sent
        TopologyInfo topoInfo = TopologyInfo.newBuilder().setTopologyId(originalTopoId)
                        .setTopologyContextId(topoContextId).setTopologyType(TopologyType.REALTIME)
                        .build();
        // Entity RI coverage instance to be sent
        EntityReservedInstanceCoverage riCoverage = EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(entityId).putCouponsCoveredByRi(riId, numOfCoupons).build();
        Collection<EntityReservedInstanceCoverage> riCoverageColl = Lists.newArrayList(riCoverage);
        notificationSender.notifyProjectedEntityRiCoverage(topoInfo, projectedTopologyId,
                        riCoverageColl);
        // Wait for receiver here to receive and process the message
        latch.await();
        // After processing finishes, just assert, if receiver collected same data that was sent to it
        Assert.assertEquals("Original Topology id did not match between sender and receiver",
                        originalTopoId, riCoverageReceiver.getOrigTopoInfo().getTopologyId());
        Assert.assertEquals(
                        "Original Topology context id did not match between sender and receiver",
                        topoContextId, riCoverageReceiver.getOrigTopoInfo().getTopologyContextId());
        Assert.assertEquals("Projected Topology id did not match between sender and receiver",
                        projectedTopologyId, riCoverageReceiver.getProjectedTopoId());
        Assert.assertEquals("Original Topology type did not match between sender and receiver",
                        TopologyType.REALTIME,
                        riCoverageReceiver.getOrigTopoInfo().getTopologyType());

        for (EntityReservedInstanceCoverage coverage : riCoverageReceiver
                        .getReceivedCoverageList()) {
            Assert.assertEquals(
                            "Entity id did not match between sender and receiver of ri coverage",
                            entityId, coverage.getEntityId());
            for (Entry<Long, Double> entry : coverage.getCouponsCoveredByRiMap().entrySet()) {
                Assert.assertEquals(
                                "RI id did not match between sender and receiver of ri coverage",
                                entry.getKey().longValue(), riId);
                Assert.assertEquals(
                                "RI number of coupons covered did not match between sender and receiver of ri coverage",
                                entry.getValue(), numOfCoupons, 0.01d);
            }
        }
    }

    private Action createAction() {
        return Action.newBuilder().setId(0L).setDeprecatedImportance(0)
                        .setExplanation(ActionDTO.Explanation.newBuilder().build())
                        .setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                                        .setTarget(ActionEntity.newBuilder().setId(0L).setType(0)
                                                        .build())
                                        .addChanges(ChangeProvider.newBuilder()
                                                        .setSource(ActionEntity.newBuilder()
                                                                        .setId(0L).setType(0)
                                                                        .build())
                                                        .setDestination(ActionEntity.newBuilder()
                                                                        .setId(0L).setType(0)
                                                                        .build())
                                                        .build())
                                        .build()).build())
                        .build();
    }

}
