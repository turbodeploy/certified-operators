package com.vmturbo.plan.orchestrator.plan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProgress;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProgress.Builder;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastFailure;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.plan.orchestrator.reservation.ReservationPlacementHandler;

/**
 * Tests the methods in the plan progress listener.
 */
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

        private static final long REALTIME_CONTEXT_ID = 777;

    private PlanDao planDao = mock(PlanDao.class);

    private PlanRpcService planRpcService = mock(PlanRpcService.class);

    private ReservationPlacementHandler reservationPlacementHandler =
        mock(ReservationPlacementHandler.class);

    private PlanProgressListener planProgressListener = new PlanProgressListener(
        planDao, planRpcService, reservationPlacementHandler, REALTIME_CONTEXT_ID);

    @Captor
    private ArgumentCaptor<Consumer<PlanInstance.Builder>> builderCaptor;

    /**
     * Common setup code to run before each test method.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test that the plan status gets updated correctly on receipt of a notification about a
     * topology broadcast by the Topology Processor.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testOnBroadcastPlan() throws Exception {
        when(planDao.updatePlanInstance(anyLong(), any())).thenReturn(PlanInstance.getDefaultInstance());

        final long planId = 123;
        TopologySummary topologySummary = TopologySummary.newBuilder()
            .setTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyContextId(planId))
            .setSuccess(TopologyBroadcastSuccess.getDefaultInstance())
            .build();
        planProgressListener.onTopologySummary(topologySummary);

        verify(planDao).updatePlanInstance(eq(planId), builderCaptor.capture());

        final Consumer<PlanInstance.Builder> builderModifier = builderCaptor.getValue();
        final PlanInstance.Builder constructingTopologyBuilder = PlanInstance.newBuilder()
            // This is the initial status once broadcast is queued.
            .setStatus(PlanStatus.CONSTRUCTING_TOPOLOGY);
        builderModifier.accept(constructingTopologyBuilder);
        assertThat(constructingTopologyBuilder.getStatus(), is(PlanStatus.RUNNING_ANALYSIS));
        assertThat(constructingTopologyBuilder.getPlanProgress().getSourceTopologySummary(),
            is(topologySummary));

        // It's VERY unlikely, but it's possible that for some reason the "broadcast success"
        // message got delayed, and we already processed a "later" message. In that case we
        // shouldn't reset the status back to the "RUNNING ANALYSIS" state.
        final PlanInstance.Builder unexpectedlyAdvancedBuilder = PlanInstance.newBuilder()
            .setStatus(PlanStatus.WAITING_FOR_RESULT);
        builderModifier.accept(unexpectedlyAdvancedBuilder);
        assertThat(unexpectedlyAdvancedBuilder.getStatus(), is(PlanStatus.WAITING_FOR_RESULT));
        assertThat(unexpectedlyAdvancedBuilder.getPlanProgress().getSourceTopologySummary(),
            is(topologySummary));
    }

    /**
     * Test that the plan status gets updated correctly on receipt of a notification about a
     * topology broadcast failure in the Topology Processor.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testOnBroadcastPlanFailure() throws Exception {
        when(planDao.updatePlanInstance(anyLong(), any())).thenReturn(PlanInstance.getDefaultInstance());

        final long planId = 123;
        final String errMsg = "problem.";
        TopologySummary topologySummary = TopologySummary.newBuilder()
            .setTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyContextId(planId))
            .setFailure(TopologyBroadcastFailure.newBuilder()
                .setErrorDescription(errMsg))
            .build();
        planProgressListener.onTopologySummary(topologySummary);

        verify(planDao).updatePlanInstance(eq(planId), builderCaptor.capture());

        final Consumer<PlanInstance.Builder> builderModifier = builderCaptor.getValue();
        final PlanInstance.Builder constructingTopologyBuilder = PlanInstance.newBuilder()
            // This is the initial status once broadcast is queued.
            .setStatus(PlanStatus.CONSTRUCTING_TOPOLOGY);
        builderModifier.accept(constructingTopologyBuilder);
        assertThat(constructingTopologyBuilder.getStatus(), is(PlanStatus.FAILED));
        assertThat(constructingTopologyBuilder.getStatusMessage(), is(errMsg));
    }

    /**
     * Test that a realtime topology broadcast success is ignored.
     */
    @Test
    public void testBroadcastSuccessRealtimeIgnored() {
        planProgressListener.onTopologySummary(TopologySummary.newBuilder()
            .setTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyContextId(REALTIME_CONTEXT_ID))
            .setSuccess(TopologyBroadcastSuccess.getDefaultInstance())
            .build());

        verifyZeroInteractions(planDao);
    }

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
        PlanInstance.Builder plan = getPlan(null, null, null, null,
                null, SUCCEEDED, null);
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(plan);
        Assert.assertEquals(SUCCEEDED, planStatus);
        Assert.assertTrue(plan.getEndTime() != 0);
    }

    /**
     * Returns failed if the plan already failed.
     */
    @Test
    public void testGetPlanStatusBasedOnPlanTypeFail() {
        PlanInstance.Builder plan = getPlan(
                null, null, null, null,
                null, FAILED, null);
        PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(plan);
        Assert.assertEquals(FAILED, planStatus);
        Assert.assertTrue(plan.getEndTime() != 0);
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
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(
            getPlan(getPlanProgress(null, null),
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
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(
            getPlan(getPlanProgress(FAIL, SUCCESS), true, SOURCE_TOPOLOGY_ID,
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
        final PlanStatus planStatus = PlanProgressListener.getPlanStatusBasedOnPlanType(
            getPlan(getPlanProgress(SUCCESS, FAIL), true, SOURCE_TOPOLOGY_ID,
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
        Assert.assertTrue(PlanProgressListener.isOCPOptimizeAndBuyRI(
            getPlan(getPlanProgress(null, null),
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
        Assert.assertFalse(PlanProgressListener.isOCPOptimizeAndBuyRI(
            getPlan(getPlanProgress(null, null),
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
        Assert.assertTrue(PlanProgressListener.isOCPBuyRIOnly(
            getPlan(getPlanProgress(null, null),
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
        Assert.assertFalse(PlanProgressListener.isOCPBuyRIOnly(
            getPlan(getPlanProgress(null, null),
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
     * @param sourceTopologyID    The source topology ID.
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
