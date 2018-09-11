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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;

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
        final ActionPlan actionPlan =
                ActionPlan.newBuilder().setId(ACTION_PLAN_ID).setTopologyContextId(planId).build();
        actionsListener.onActionsReceived(actionPlan);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertEquals(ACTION_PLAN_ID, instance.getActionPlanId());
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, instance.getStatus());
    }

    /**
     * Tests receiving action plan for unknown plan instance. It is expected not to change anything.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testUpdateNotExistingPlan() throws Exception {
        final ActionPlan actionPlan = ActionPlan.newBuilder()
                .setId(ACTION_PLAN_ID)
                .setTopologyContextId(planId + 1)
                .build();
        actionsListener.onActionsReceived(actionPlan);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertFalse(instance.hasActionPlanId());
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
                .setProjectedTopologyId(PROJ_TOPO_ID));

        final ActionPlan actionPlan =
                ActionPlan.newBuilder().setId(ACTION_PLAN_ID).setTopologyContextId(planId).build();
        actionsListener.onActionsReceived(actionPlan);
        final PlanInstance instance = planDao.getPlanInstance(planId).get();
        Assert.assertEquals(ACTION_PLAN_ID, instance.getActionPlanId());
        Assert.assertEquals(PROJ_TOPO_ID, instance.getProjectedTopologyId());
        Assert.assertEquals(PlanStatus.SUCCEEDED, instance.getStatus());
    }
}
