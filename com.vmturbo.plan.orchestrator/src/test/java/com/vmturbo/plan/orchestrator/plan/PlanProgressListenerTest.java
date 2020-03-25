package com.vmturbo.plan.orchestrator.plan;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.matchers.CapturesArguments;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
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
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable.UpdateFailure;
import com.vmturbo.plan.orchestrator.reservation.ReservationPlacementHandler;

/**
 * Tests the methods in the plan progress listener.
 */
public class PlanProgressListenerTest {

    private static final long TIMEOUT = 30000;
    private static final long TOPOLOGY_ID = 1234L;
    private static final long ACT_PLAN_ID = 3456L;

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
     * Plan WAITING_FOR_RESULT state.
     */
    private static final PlanStatus WAITING_FOR_RESULT = PlanStatus.WAITING_FOR_RESULT;

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
        PlanStatus planStatus1 = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS).setAnalysisStatus(SUCCESS), true, SOURCE_TOPOLOGY_ID,
                PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, null),
                true);
        Assert.assertEquals(SUCCEEDED, planStatus1);
        PlanStatus planStatus2 = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(SUCCESS, SUCCESS).setAnalysisStatus(Status.UNKNOWN), true, SOURCE_TOPOLOGY_ID,
                PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null, null),
                true);
        Assert.assertEquals(WAITING_FOR_RESULT, planStatus2);
    }

    /**
     * Tests get OCP buy RI only.
     */
    @Test
    public void testGetOCPBuyRIOnlyPlanStatus() {
        // Buy RI only plan status should not be affected by Analysis run status.
        final RISetting.Builder riSetting = RISetting.newBuilder();
        final ScenarioChange.Builder scenarioChangeRI =
                ScenarioChange.newBuilder().setRiSetting(riSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).addChanges(scenarioChangeRI);
        final Scenario.Builder scenario = Scenario.newBuilder().setScenarioInfo(scenarioInfo);
        PlanStatus planStatus1 = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                getPlanProgress(null, null).setAnalysisStatus(SUCCESS),
                true, SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID,
                null, scenario), false);
        Assert.assertEquals(SUCCEEDED, planStatus1);
        PlanStatus planStatus2 = PlanProgressListener.getOCPWithBuyRIPlanStatus(getPlan(
                    getPlanProgress(null, null).setAnalysisStatus(Status.UNKNOWN),
                    true, SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID,
                    null, scenario), false);
        Assert.assertEquals(SUCCEEDED, planStatus2);
    }

    /**
     * Tests get OCP with buy RI and optimize services failure because of projected cost
     * notification.
     */
    @Test
    public void testGetOCPWithBuyRIAndOptimizeServicesPlanStatusNoProjectedCostNotification() {
        final Setting.Builder setting =
                Setting.newBuilder().setSettingSpecName(EntitySettingSpecs.Resize.getSettingName());
        final SettingOverride.Builder scaleSetting =
                SettingOverride.newBuilder().setSetting(setting);
        final ScenarioChange.Builder scenarioChangeScale =
                ScenarioChange.newBuilder().setSettingOverride(scaleSetting);
        final ScenarioInfo.Builder scenarioInfo = ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                .addChanges(scenarioChangeScale);
        final Scenario.Builder scenario = Scenario.newBuilder().setScenarioInfo(scenarioInfo);
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
                getPlan(getPlanProgress(SUCCESS, SUCCESS).setAnalysisStatus(SUCCESS), true,
                        SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, ACTION_PLAN_ID, null,
                        scenario));
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

    private PlanInstance.Builder setupPlanInstance(final long planId) throws NoSuchObjectException, IntegrityException {
        PlanInstance.Builder bldr = PlanInstance.newBuilder();
        bldr.setPlanId(planId);
        bldr.setStatus(PlanStatus.READY);
        when(planDao.getPlanInstance(planId)).thenAnswer(invocation -> Optional.of(bldr.build()));
        when(planDao.updatePlanInstance(eq(planId), any(Consumer.class)))
            .thenAnswer(invocation -> {
                invocation.getArgumentAt(1, Consumer.class).accept(bldr);
                return bldr.build();
            });
        return bldr;
    }

    /**
     * Tests finishing a plan, when firstly the projected topology is reported, and later - the
     * action plan is reported.
     *
     * @throws Exception if exceptions occur.
     */
    @Test
    public void testFinishTopologyAndHistoryThenActions() throws Exception {
        final long planId = 12312;
        final PlanInstance.Builder bldr = setupPlanInstance(planId);

        planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        planProgressListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());

        assertThat(bldr.getStatus(), is(PlanStatus.WAITING_FOR_RESULT));
        assertThat(bldr.getProjectedTopologyId(), is(TOPOLOGY_ID));

        planProgressListener.onActionsUpdated(
            ActionsUpdated.newBuilder()
                .setActionPlanId(ACT_PLAN_ID)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                    .setMarket(MarketActionPlanInfo.newBuilder()
                        .setSourceTopologyInfo(TopologyInfo.newBuilder()
                            .setTopologyContextId(planId)))).build());
        planProgressListener.onStatsAvailable(StatsAvailable.newBuilder().setTopologyContextId(1).build());
        planProgressListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
        planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                .setStatus(Status.SUCCESS)
                .build())
            .build());
        planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                .setStatus(Status.SUCCESS)
                .build())
            .build());

        assertPlan(bldr.build(), planId);
    }

    /**
     * Tests if the plan fails if the stats has any failure message.
     *
     * @throws Exception The exception
     */
    @Test
    public void testPlanFailsWhenStatsFails() throws Exception {
        final long planId = 123L;
        final PlanInstance.Builder bldr = setupPlanInstance(planId);

        planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        assertThat(bldr.getStatus(), is(PlanStatus.WAITING_FOR_RESULT));
        assertThat(bldr.getProjectedTopologyId(), is(TOPOLOGY_ID));

        final String err = "The stats failure message.";
        planProgressListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setUpdateFailure(UpdateFailure.newBuilder()
                .setErrorMessage(err).build())
            .setTopologyContextId(planId)
            .build());

        assertThat(bldr.getStatus(), is(PlanStatus.FAILED));
        assertThat(bldr.getStatusMessage(), is(err));
    }

    /**
     * Test finishing a plan when the projected topology and actions are received first,
     * and the history notification comes in later.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testFinishTopologyAndActionsThenHistory() throws Exception {
        final long planId = 12123121;
        final PlanInstance.Builder bldr = setupPlanInstance(planId);

        planProgressListener.onActionsUpdated(actionsUpdated(ACT_PLAN_ID, planId));
        planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);

        assertThat(bldr.getStatus(), is(PlanStatus.WAITING_FOR_RESULT));
        assertThat(bldr.getActionPlanIdList(), containsInAnyOrder(ACT_PLAN_ID));
        assertThat(bldr.getProjectedTopologyId(), is(TOPOLOGY_ID));


        planProgressListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());
        planProgressListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
        planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                .setStatus(Status.SUCCESS)
                .build())
            .build());
        planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                .setStatus(Status.SUCCESS)
                .build())
            .build());

        assertPlan(bldr.build(), planId);
    }

    /**
     * Tests when a topology is reported twice. It is expected, that plan is not reported as
     * finished.
     *
     * @throws Exception if exceptions occur.
     */
    @Test
    public void testDoubleTopologyReported() throws Exception {
        final long planId = 1312;
        final PlanInstance.Builder planBldr = setupPlanInstance(planId);

        planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        assertThat(planBldr.getStatus(), not(PlanStatus.SUCCEEDED));
    }

    /**
     * Tests concurrent reporting of action plan and projected topology. If something is wrong
     * with concurrency in this case, the test may fail from time to time.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testFinishTopologyAndActionsSimultaneously() throws Exception {
        final long planId = 123121;
        final PlanInstance.Builder bldr = setupPlanInstance(planId);
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final Future<?> actionReport = threadPool.submit(() -> {
                latch.await();
                planProgressListener.onActionsUpdated(actionsUpdated(ACT_PLAN_ID, planId));
                return null;
            });
            final Future<?> topologyReport = threadPool.submit(() -> {
                latch.await();
                planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
                return null;
            });
            final Future<?> historyStatsAvailable = threadPool.submit(() -> {
                latch.await();
                planProgressListener.onStatsAvailable(StatsAvailable.newBuilder()
                    .setTopologyContextId(planId)
                    .build());
                return null;
            });
            final Future<?> sourceTopologyAvailable = threadPool.submit(() -> {
                latch.await();
                planProgressListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
                return null;
            });
            final Future<?> costNotificationAvailable = threadPool.submit(() -> {
                latch.await();
                planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
                    .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                    .build());
                return null;
            });
            latch.countDown();

            actionReport.get();
            topologyReport.get();
            historyStatsAvailable.get();
            sourceTopologyAvailable.get();
            costNotificationAvailable.get();

            assertPlan(bldr.build(), planId);
        } finally {
            threadPool.shutdown();
        }
    }

    /**
     * Test that the {@link PlanProgressListener} ignores notifications about the real-time
     * topology.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testIgnoreRealtimeTopology() throws Exception {
        planProgressListener.onActionsUpdated(actionsUpdated(1, REALTIME_CONTEXT_ID));

        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
        planProgressListener.onProjectedTopologyAvailable(0, REALTIME_CONTEXT_ID);
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
        Mockito.verify(reservationPlacementHandler, Mockito.times(1))
            .updateReservationsFromLiveTopology(Mockito.anyLong(), Mockito.anyLong());
        planProgressListener.onProjectedTopologyFailure(0, REALTIME_CONTEXT_ID, "");
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
    }

    /**
     * Tests finishing a plan, when firstly the action plan is reported, and later - the
     * projected topology is reported.
     *
     * @throws Exception if exceptions occur.
     */
    @Test
    public void testFinishActionsAndHistoryThenTopology() throws Exception {
        final long planId = 12312;
        final PlanInstance.Builder bldr = setupPlanInstance(planId);
        planProgressListener.onActionsUpdated(actionsUpdated(ACT_PLAN_ID, planId));

        planProgressListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());

        assertThat(bldr.getStatus(), is(PlanStatus.WAITING_FOR_RESULT));
        assertThat(bldr.getActionPlanIdList(), containsInAnyOrder(ACT_PLAN_ID));

        planProgressListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);

        planProgressListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
        planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                .setStatus(Status.SUCCESS)
                .build())
            .build());
        planProgressListener.onCostNotificationReceived(CostNotification.newBuilder()
            .setStatusUpdate(StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                .setStatus(Status.SUCCESS)
                .build())
            .build());

        assertPlan(bldr.build(), planId);
    }

    @Nonnull
    private ActionsUpdated actionsUpdated(final long actionPlanId, final long planId) {
        return ActionsUpdated.newBuilder()
            .setActionPlanId(actionPlanId)
            .setActionPlanInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planId))))
            .build();
    }


    private static void assertPlan(@Nonnull final PlanInstance plan, final long planId) {
        Assert.assertEquals(PlanStatus.SUCCEEDED, plan.getStatus());
        Assert.assertEquals(planId, plan.getPlanId());
        Assert.assertEquals(TOPOLOGY_ID, plan.getProjectedTopologyId());
        Assert.assertTrue(ACT_PLAN_ID == plan.getActionPlanIdList().get(0));
    }

    /**
     * Creates a matcher to match only succeeded plan instances.
     *
     * @return matcher representation for verification
     */
    private static PlanInstance planSucceeded() {
        return new StatusMatcher(PlanStatus.SUCCEEDED).capture();
    }

    /**
     * Matcher to capture {@link PlanInstance}s of the specific status.
     */
    private static class StatusMatcher extends BaseMatcher<PlanInstance> implements
        CapturesArguments {

        private final List<PlanInstance> plans = new ArrayList<>();
        private final PlanStatus status;

        StatusMatcher(PlanStatus status) {
            this.status = status;
        }

        @Override
        public boolean matches(Object item) {
            final PlanInstance plan = (PlanInstance)item;
            return plan.getStatus() == status;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(status.toString());
        }

        public List<PlanInstance> getValues() {
            return plans;
        }

        public PlanInstance getValue() {
            Assert.assertEquals(1, plans.size());
            return plans.get(0);
        }

        @Override
        public void captureFrom(Object argument) {
            plans.add((PlanInstance)argument);
        }

        public PlanInstance capture() {
            return Mockito.argThat(this);
        }
    }
}
