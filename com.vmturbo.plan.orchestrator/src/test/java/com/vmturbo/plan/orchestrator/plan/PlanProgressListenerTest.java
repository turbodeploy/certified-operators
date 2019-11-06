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
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
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
    private static final int PROJECTED_TOPOLOGY_ID = 5;
    /**
     * Source topology ID.
     */
    private static final int SOURCE_TOPOLOGY_ID = 2;
    /**
     * Action plan ID.
     */
    private static final int ACTION_PLAN_ID = 7;
    /**
     * Notification success.
     */
    private static final Status SUCCESS = Status.SUCCESS;
    /**
     * Notification fail.
     */
    private static final Status FAIL = Status.FAIL;
    /**
     * Plan succeeded.
     */
    private static final PlanStatus SUCCEEDED = PlanStatus.SUCCEEDED;
    /**
     * Plan failed.
     */
    private static final PlanStatus FAILED = PlanStatus.FAILED;

    /**
     * Tests get OCP with buy RI and optimize services successfully.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatus() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), true, SOURCE_TOPOLOGY_ID,
                PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, null),
                true);
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Tests get OCP buy RI only.
     */
    @Test
    public void testGetOCPBuyRIOnlyPlanStatus() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, null),
                true, SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID,
                null, null), false);
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Tests get OCP with buy RI and optimize services failure because of projected cost
     * notification.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusNoProjectedCostNotification() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, SUCCESS), null,
                null, null, null, null,
                null), true);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI and optimize services failure because of projected RI coverage
     * notification.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusNoProjectedRICoverageNotification() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, null), null,
                null, null, null, null,
                null), true);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI and optimize services failure because stats is not available.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusNoStats() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), null,
                SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null,
                null), true);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP buy RI only failure because stats is not available.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusNoStats() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, null),
                null, SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID,
                null, null), false);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI and optimize services failure because there is no projected
     * topology.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusNoProjectedTopology() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), true, SOURCE_TOPOLOGY_ID,
                null, ACTION_PLAN_ID, null, null),
                true);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP buy RI only failure because there is no projected topology.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusNoProjectedTopology() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, null),
                true, SOURCE_TOPOLOGY_ID, null, ACTION_PLAN_ID,
                null, null), false);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP with buy RI and optimize services failure because action ID list is empty.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusEmptyActionIdList() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS), true, SOURCE_TOPOLOGY_ID,
                null, ACTION_PLAN_ID, null, null),
                true);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Tests get OCP buy RI only failure because action ID list is empty.
     */
    @Test
    public void testGetOCPWithBuyRIPlanStatusEmptyActionIdList() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, null),
                true, SOURCE_TOPOLOGY_ID, null, ACTION_PLAN_ID,
                null, null), false);
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, planStatus);
    }

    /**
     * Plan OCP type 1 fails because projected cost failed.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusProjectedCostFail() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(FAIL, null), null,
                SOURCE_TOPOLOGY_ID, null, null, null,
                null), true);
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Plan OCP type 1 fails because projected RI coverage failed.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusProjectedRICoverageFail() {
        PlanStatus planStatus = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, FAIL), null,
                SOURCE_TOPOLOGY_ID, null, null, null,
                null), true);
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns success if the plan already succeeded.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeSuccess() {
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan(
                null, null, null, null,
                null, SUCCEEDED, null));
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Returns failed if the plan already failed.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeFail() {
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan(
                null, null, null, null,
                null, FAILED, null));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns success if plan orchestrator receives all the notifications successfully in OCP
     * type 1.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType1Success() {
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChangeRI =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final Setting.Builder setting =
                Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());
        final SettingOverride.Builder scaleSetting =
                SettingOverride.newBuilder().setSetting(setting);
        final ScenarioChange.Builder scenarioChangeScale =
                ScenarioChange.newBuilder().setSettingOverride(scaleSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChangeRI)
                .addChanges(scenarioChangeScale);
        final Scenario.Builder scenario = Scenario.newBuilder().setScenarioInfo(scenarioInfo);
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(
                getPlan(getPlanProgress(SUCCESS, SUCCESS), true, SOURCE_TOPOLOGY_ID,
                        PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, scenario));
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Returns success if plan orchestrator receives all the notifications successfully in OCP
     * type 3.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType3Success() {
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChange =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChange);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan
                (getPlanProgress(null, null),
                        true, SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID,
                        null, scenario));
        Assert.assertEquals(SUCCEEDED, planStatus);
    }

    /**
     * Returns Fail if plan orchestrator receives projected cost failure in OCP
     * type 1.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType1FailProjectedCost() {
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChangeRI =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final Setting.Builder setting =
                Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());
        final SettingOverride.Builder scaleSetting =
                SettingOverride.newBuilder().setSetting(setting);
        final ScenarioChange.Builder scenarioChangeScale =
                ScenarioChange.newBuilder().setSettingOverride(scaleSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChangeRI)
                .addChanges(scenarioChangeScale);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan
                (getPlanProgress(FAIL, SUCCESS), true, SOURCE_TOPOLOGY_ID,
                        PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, scenario));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Returns Fail if plan orchestrator receives projected cost failure in OCP
     * type 1.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeOCPType1FailProjectedRICoverage() {
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChangeRI =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final Setting.Builder setting =
                Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());
        final SettingOverride.Builder scaleSetting =
                SettingOverride.newBuilder().setSetting(setting);
        final ScenarioChange.Builder scenarioChangeScale =
                ScenarioChange.newBuilder().setSettingOverride(scaleSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChangeRI)
                .addChanges(scenarioChangeScale);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(getPlan
                (getPlanProgress(SUCCESS, FAIL), true, SOURCE_TOPOLOGY_ID,
                        PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, scenario));
        Assert.assertEquals(FAILED, planStatus);
    }

    /**
     * Tests if the is OCP recognizes the OCP plans correctly.
     */
    @Test
    public void testIsOCP() {
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        Assert.assertTrue(PlanProgressListener.isOCP(getPlan(getPlanProgress(null,
                null), true, null,
                null, null, null, scenario)));
    }

    /**
     * Tests if the is OCP recognizes non-OCP plans correctly.
     */
    @Test
    public void testIsOCPNegative() {
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.CLOUD_MIGRATION_PLAN);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        Assert.assertFalse(PlanProgressListener.isOCP(getPlan(getPlanProgress(null,
                null), true, null,
                null, null, null, scenario)));
    }

    /**
     * Recognizes if is buy RI and optimize services recognizes OCP type 1 correctly.
     */
    @Test
    public void isBuyRIAndOptimizeServices() {
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChangeRI =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final Setting.Builder setting =
                Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());
        final SettingOverride.Builder scaleSetting =
                SettingOverride.newBuilder().setSetting(setting);
        final ScenarioChange.Builder scenarioChangeScale =
                ScenarioChange.newBuilder().setSettingOverride(scaleSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChangeRI)
                .addChanges(scenarioChangeScale);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        Assert.assertTrue(PlanProgressListener.isOCPOptimizeAndBuyRI(getPlan
                (getPlanProgress(null, null),
                        true, null, null,
                        null, null, scenario)));
    }

    /**
     * Recognizes if is buy RI and optimize services recognizes plan other than OCP type 1
     * correctly.
     */
    @Test
    public void isBuyRIAndOptimizeServicesNegative() {
        final Setting.Builder setting =
                Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());
        final SettingOverride.Builder scaleSetting =
                SettingOverride.newBuilder().setSetting(setting);
        final ScenarioChange.Builder scenarioChangeScale =
                ScenarioChange.newBuilder().setSettingOverride(scaleSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChangeScale);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        Assert.assertFalse(PlanProgressListener.isOCPOptimizeAndBuyRI(getPlan
                (getPlanProgress(null, null),
                        true, null, null,
                        null, null, scenario)));
    }

    /**
     * Recognizes if is buy RI only recognizes OCP type 3 correctly.
     */
    @Test
    public void testIsBuyRIOnly() {
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChange =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChange);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        Assert.assertTrue(PlanProgressListener.isOCPBuyRIOnly(getPlan
                (getPlanProgress(null, null),
                        true, null, null,
                        null, null, scenario)));
    }

    /**
     * Recognizes if is buy RI only recognizes plans other than OCP type 3 correctly.
     */
    @Test
    public void testIsBuyRIOnlyNegative() {
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN);
        final Scenario.Builder scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo);
        Assert.assertFalse(PlanProgressListener.isOCPBuyRIOnly(getPlan
                (getPlanProgress(null, null),
                        true, null, null,
                        null, null, scenario)));
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
                                                final Integer sourceTopologyID,
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
        if (sourceTopologyID != null) {
            planInstance.setSourceTopologyId(sourceTopologyID);
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
