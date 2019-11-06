package com.vmturbo.plan.orchestrator.plan;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.Status;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProgress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Unit test to cover cases, initiated from action orchestrator.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
        classes = {PlanTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ActionListenerTest {

    private static final long ACTION_PLAN_ID = 12345L;
    private static final long PROJ_TOPO_ID = 34567L;
    /**
     * Source topology ID.
     */
    private static final int SOURCE_TOPOLOGY_ID = 2;

    @Autowired
    private PlanDao planDao;

    @Autowired
    private ActionsListener actionsListener;

    private long planId;

    @Before
    public void startUp() throws Exception {
        final CreatePlanRequest request = CreatePlanRequest.newBuilder().build();
        planId = planDao.createPlanInstance(request).getPlanId();
    }

    /**
     * Tests receiving update for an existing plan.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testUpdateExistingPlan() throws Exception {
        planDao.updatePlanInstance(planId, builder -> builder.setStatus(PlanStatus.RUNNING_ANALYSIS));
        final ActionsUpdated actionsUpdated =
            ActionsUpdated.newBuilder()
                .setActionPlanId(ACTION_PLAN_ID)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                    .setMarket(MarketActionPlanInfo.newBuilder()
                        .setSourceTopologyInfo(TopologyInfo.newBuilder()
                            .setTopologyContextId(planId))))
                .build();
        actionsListener.onActionsUpdated(actionsUpdated);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertTrue(ACTION_PLAN_ID == instance.getActionPlanIdList().get(0));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, instance.getStatus());
    }

    @Test
    public void testUpdateBuyRiPlan() throws Exception {
        planDao.updatePlanInstance(planId, builder -> builder.setStatus(PlanStatus.STARTING_BUY_RI));
        final ActionsUpdated actionsUpdated =
                ActionsUpdated.newBuilder().setActionPlanId(ACTION_PLAN_ID)
                .setActionPlanInfo(ActionPlanInfo.newBuilder().setBuyRi(BuyRIActionPlanInfo
                        .newBuilder().setTopologyContextId(planId))).build();
        actionsListener.onActionsUpdated(actionsUpdated);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertTrue(ACTION_PLAN_ID == instance.getActionPlanIdList().get(0));
        Assert.assertNotEquals(PlanStatus.STARTING_BUY_RI, instance.getStatus());
        // Check for both WAITING_FOR_RESULT and CONSTRUCTING_TOPOLOGY status since
        // PlanRpcService.startAnalysis submits a task to ExecutorService to update the
        // status.
        Assert.assertTrue(PlanStatus.CONSTRUCTING_TOPOLOGY == instance.getStatus() ||
                PlanStatus.WAITING_FOR_RESULT == instance.getStatus());
    }

    /**
     * Tests receiving action plan for unknown plan instance. It is expected not to change anything.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testUpdateNotExistingPlan() throws Exception {
        final ActionsUpdated actionsUpdated = ActionsUpdated.newBuilder()
            .setActionPlanId(ACTION_PLAN_ID)
            .setActionPlanInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planId + 1))))
            .build();
        actionsListener.onActionsUpdated(actionsUpdated);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertFalse(!instance.getActionPlanIdList().isEmpty());
    }

    /**
     * Tests reporting action plan for existing plan instance, which already have projected
     * topology received. It is expected, that plan instance is marked as completed.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testFinishedPlanPlan() throws Exception {
        planDao.updatePlanInstance(planId, builder -> builder.setStatus(PlanStatus.WAITING_FOR_RESULT)
                .setStatsAvailable(true)
                .setSourceTopologyId(SOURCE_TOPOLOGY_ID)
                .setProjectedTopologyId(PROJ_TOPO_ID)
                .setPlanProgress(PlanProgress.newBuilder()
                        .setProjectedRiCoverageStatus(Status.SUCCESS)
                        .setProjectedCostStatus(Status.SUCCESS)));
        final ActionsUpdated actionsUpdated = ActionsUpdated.newBuilder()
                .setActionPlanId(ACTION_PLAN_ID)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                        .setTopologyContextId(planId))))
                .build();
        actionsListener.onActionsUpdated(actionsUpdated);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertTrue(ACTION_PLAN_ID == instance.getActionPlanIdList().get(0));
        Assert.assertEquals(PROJ_TOPO_ID, instance.getProjectedTopologyId());
        Assert.assertEquals(PlanStatus.SUCCEEDED, instance.getStatus());
    }
}
