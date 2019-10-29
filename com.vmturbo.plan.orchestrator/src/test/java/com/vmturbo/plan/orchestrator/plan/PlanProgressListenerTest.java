package com.vmturbo.plan.orchestrator.plan;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.Status;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProgress;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProgress.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Tests the methods in the plan progress listener.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
        classes = {PlanTestConfig.class})
public class PlanProgressListenerTest {

    /**
     * Projected topology ID.
     */
    public static final int PROJECTED_TOPOLOGY_ID = 5;
    /**
     * Action plan ID>
     */
    public static final int ACTION_PLAN_ID = 7;
    /**
     * Notification success.
     */
    public static final Status SUCCESS = Status.SUCCESS;
    /**
     * Notification fail.
     */
    public static final Status FAIL = Status.FAIL;
    /**
     * Plan succeeded.
     */
    public static final PlanStatus SUCCEEDED = PlanStatus.SUCCEEDED;
    /**
     * Plan failed.
     */
    public static final PlanStatus FAILED = PlanStatus.FAILED;

    /**
     * Tests get OCP with buy RI successfully.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatus() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), true, PROJECTED_TOPOLOGY_ID,
                ACTION_PLAN_ID, null, null));
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Tests get OCP with buy RI failure because of projected cost notification.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusNoProjectedCostNotification() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, SUCCESS), null,
                null, null, null, null));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI failure because of projected RI coverage notification.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusNoProjectedRICoverageNotification() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, null), null,
                null, null, null, null));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI failure because stats is not available.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusNoStats() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), null,
                PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, null));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI failure because there is no projected topology.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusNoProjectedTopology() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), true, null,
                ACTION_PLAN_ID, null, null));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI failure because action ID list is empty.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusEmptyActionIdList() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), true, null,
                ACTION_PLAN_ID, null, null));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Plan fails because projected cost failed.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusProjectedCostFail() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(FAIL, null), null,
                null, null, null, null));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Plan fails because projected RI coverage failed.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusProjectedRICoverageFail() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, FAIL), null,
                null, null, null, null));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns success if the plan already succeeded.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeSuccess() {
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan(
                null, null, null, null,
                SUCCEEDED, null));
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Returns success if the plan already failed.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeFail() {
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan(
                null, null, null, null,
                FAILED, null));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns success if plan orchestrator receives all the notifications successfully in OCP
     * type 1.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType1Success() {
        RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChange =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChange);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan
                (getPlanProgress(SUCCESS, SUCCESS), true, PROJECTED_TOPOLOGY_ID,
                        ACTION_PLAN_ID, null, scenario));
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Returns Fail if plan orchestrator receives projected cost failure in OCP
     * type 1.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType1FailProjectedCost() {
        RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChange =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChange);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan
                (getPlanProgress(FAIL, SUCCESS), true, PROJECTED_TOPOLOGY_ID,
                        ACTION_PLAN_ID, null, scenario));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns Fail if plan orchestrator receives projected cost failure in OCP
     * type 1.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType1FailProjectedRICoverage() {
        RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChange =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChange);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan
                (getPlanProgress(SUCCESS, FAIL), true, PROJECTED_TOPOLOGY_ID,
                        ACTION_PLAN_ID, null, scenario));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns the plan progress builder.
     *
     * @param projectedCostStatus       The status of the projected cost
     * @param projectedRICoverageStatus The status of the projected RI coverage
     * @return The plan progress builder
     */
    private static PlanProgress.Builder getPlanProgress(final Status projectedCostStatus,
                                                        final Status projectedRICoverageStatus) {
        Builder planProgressBuilder = PlanProgress.newBuilder();
        if (projectedCostStatus != null) {
            planProgressBuilder.setProjectedCostStatus(projectedCostStatus);
        }
        if (projectedRICoverageStatus != null) {
            planProgressBuilder
                    .setProjectedRiCoverageStatus(projectedRICoverageStatus);
        }
        return planProgressBuilder;
    }

    /**
     * Gets the plan.
     *
     * @param planProgress        The plan progress
     * @param isStatsAvailable    Is the stats available
     * @param projectedTopologyID The projected topology ID
     * @param actionPlanID        The projected plan ID
     * @param planStatus          The plan status
     * @param scenario            The scenario
     * @return The plan
     */
    private static PlanInstance.Builder getPlan(final PlanProgress.Builder planProgress,
                                                final Boolean isStatsAvailable,
                                                final Integer projectedTopologyID,
                                                final Integer actionPlanID,
                                                final PlanStatus planStatus,
                                                final Scenario.Builder scenario) {
        PlanInstance.Builder planInstance = PlanInstance.newBuilder();
        if (planProgress != null) {
            planInstance.setPlanProgress(planProgress);
        }
        if (isStatsAvailable != null) {
            planInstance.setStatsAvailable(isStatsAvailable);
        }
        if (projectedTopologyID != null) {
            planInstance.setProjectedTopologyId(projectedTopologyID);
        }
        if (actionPlanID != null) {
            planInstance.addActionPlanId(actionPlanID);
        }
        if (planStatus != null) {
            planInstance.setStatus(planStatus);
        }
        if (scenario != null) {
            planInstance.setScenario(scenario);
        }
        return planInstance;
    }

}
