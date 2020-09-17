package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.utils.StringConstants.AUTOMATIC;
import static com.vmturbo.common.protobuf.utils.StringConstants.DISABLED;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.StatusUpdate;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the interpretation of {@link PlanInstance} as
 * API-facing DTO's.
 */
public class MarketMapperTest {
    private static final String SCENARIO_NAME = "scenario";

    private static final long SCENARIO_ID = 7;

    private static final long PLAN_ID = 10;

    private static final long START_TIME = 0;

    private static final long END_TIME = 100000;

    private static final String CREATED_BY_USER = "12345";

    private static final String SCENARIO_TYPE = "SCENARIO_TYPE";

    private static final PlanInstance BASE = PlanInstance.newBuilder()
            .setPlanId(PLAN_ID)
            .setSourceTopologyId(1L)
            .setScenario(Scenario.newBuilder()
                    .setId(SCENARIO_ID)
                    .setScenarioInfo(ScenarioInfo.newBuilder()
                            .setName(SCENARIO_NAME)))
            .setStartTime(START_TIME)
            .buildPartial();

    private static final PlanInstance SUCCEEDED_INSTANCE = BASE.toBuilder()
            .setProjectedTopologyId(5L)
            .addActionPlanId(55L)
            .setStatus(PlanStatus.SUCCEEDED)
            .setEndTime(END_TIME)
            .build();


    private static final PlanInstance IN_PROGRESS_INSTANCE = BASE.toBuilder()
            .setStatus(PlanStatus.CONSTRUCTING_TOPOLOGY)
            .setCreatedByUser(CREATED_BY_USER)
            .build();

    private static final PlanInstance FAILED_INSTANCE = BASE.toBuilder()
            .setStatus(PlanStatus.FAILED)
            .setEndTime(END_TIME)
            .build();

    private ScenarioMapper scenarioMapper = Mockito.mock(ScenarioMapper.class);

    private MarketMapper marketMapper;

    @Before
    public void setup() throws Exception {
        marketMapper = new MarketMapper(scenarioMapper);

        // Since we use the same base scenario for all tests, configure
        // the mock scenario mapper to return something reasonable
        // for that scenario.
        final ScenarioApiDTO scenarioApiDTO = new ScenarioApiDTO();
        scenarioApiDTO.setUuid(Long.toString(SCENARIO_ID));
        scenarioApiDTO.setDisplayName(SCENARIO_NAME);
        scenarioApiDTO.setType(SCENARIO_TYPE);

        when(scenarioMapper.toScenarioApiDTO(Mockito.eq(BASE.getScenario())))
                .thenReturn(scenarioApiDTO);
    }

    @Test
    public void testDtoFromPlanInstanceInProgress() throws Exception {
        MarketApiDTO inProgressDto = marketMapper.dtoFromPlanInstance(IN_PROGRESS_INSTANCE);
        assertEquals(true, inProgressDto.getSaved());
        assertEquals(DateTimeUtil.toString(START_TIME), inProgressDto.getRunDate());
        assertEquals("RUNNING", inProgressDto.getState());
        Assert.assertTrue(inProgressDto.getStateProgress() < 100 && inProgressDto.getStateProgress() >= 0);
        Assert.assertNull(inProgressDto.getRunCompleteDate());
        assertEquals(Long.toString(SCENARIO_ID), inProgressDto.getScenario().getUuid());
        assertEquals(SCENARIO_NAME, inProgressDto.getScenario().getDisplayName());
        assertEquals(CREATED_BY_USER, inProgressDto.getScenario().getOwners().get(0).getUuid());
        assertEquals(SCENARIO_NAME, inProgressDto.getDisplayName());
    }

    @Test
    public void testDtoFromPlanInstanceSucceeded() throws Exception {
        MarketApiDTO succeededDto = marketMapper.dtoFromPlanInstance(SUCCEEDED_INSTANCE);
        assertEquals("Market", succeededDto.getClassName());
        assertEquals(true, succeededDto.getSaved());
        assertEquals(DateTimeUtil.toString(START_TIME), succeededDto.getRunDate());
        assertEquals("SUCCEEDED", succeededDto.getState());
        assertEquals(Integer.valueOf(100), succeededDto.getStateProgress());
        assertEquals(DateTimeUtil.toString(END_TIME), succeededDto.getRunCompleteDate());
        assertEquals(Long.toString(SCENARIO_ID), succeededDto.getScenario().getUuid());
        assertEquals(SCENARIO_NAME, succeededDto.getScenario().getDisplayName());
        assertEquals(SCENARIO_NAME, succeededDto.getDisplayName());
    }

    @Test
    public void testDtoFromPlanInstanceFailed() throws Exception {
        MarketApiDTO failedDto = marketMapper.dtoFromPlanInstance(FAILED_INSTANCE);
        assertEquals(true, failedDto.getSaved());
        assertEquals(DateTimeUtil.toString(START_TIME), failedDto.getRunDate());
        assertEquals("STOPPED", failedDto.getState());
        assertEquals(Integer.valueOf(100), failedDto.getStateProgress());
        assertEquals(DateTimeUtil.toString(END_TIME), failedDto.getRunCompleteDate());
        assertEquals(Long.toString(SCENARIO_ID), failedDto.getScenario().getUuid());
        assertEquals(SCENARIO_NAME, failedDto.getScenario().getDisplayName());
        assertEquals(SCENARIO_NAME, failedDto.getDisplayName());
    }

    /**
     * Tests displayName mapped from {@link PlanInstance} name.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDtoFromPlanInstanceGetsNameFromPlanInstance() throws Exception {
        //GIVEN
        String planName = "planName";
        PlanInstance planInstance = SUCCEEDED_INSTANCE.toBuilder().setName(planName).build();

        //WHEN
        MarketApiDTO dto = marketMapper.dtoFromPlanInstance(planInstance);

        //THEN
        assertEquals(dto.getDisplayName(), planName);
    }

    @Test
    public void testNotificationFromPlanInstanceInProgress() throws Exception {
        MarketNotification inProgress = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.WAITING_FOR_RESULT)
            .build());
        assertEquals(Long.toString(PLAN_ID), inProgress.getMarketId());
        Assert.assertTrue(inProgress.hasStatusProgressNotification());
        assertEquals(Status.RUNNING, inProgress.getStatusProgressNotification().getStatus());
    }

    @Test
    public void testNotificationFromPlanInstanceSucceeded() throws Exception {
        MarketNotification success = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.SUCCEEDED)
            .build());
        assertEquals(Long.toString(PLAN_ID), success.getMarketId());
        Assert.assertTrue(success.hasStatusNotification());
        assertEquals(Status.SUCCEEDED, success.getStatusNotification().getStatus());
    }

    @Test
    public void testNotificationFromPlanInstanceFailed() throws Exception {
        MarketNotification failure = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.FAILED)
            .build());
        assertEquals(Long.toString(PLAN_ID), failure.getMarketId());
        Assert.assertTrue(failure.hasStatusNotification());
        assertEquals(Status.STOPPED, failure.getStatusNotification().getStatus());
    }

    /**
     * Test return Status.RUNNING when PlanStatus.STARTING_BUY_RI.
     */
    @Test
    public void testNotificationWithPlanInstanceStartingBuyRI() {
        MarketNotification startBuyRi = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.STARTING_BUY_RI)
            .build());
        assertEquals(Status.RUNNING, startBuyRi.getStatusProgressNotification().getStatus());
    }

    /**
     * Test return Status.RUNNING when PlanStatus.BUY_RI_COMPLETED.
     */
    @Test
    public void testNotificationWithPlanInstanceStatusBuyRICompleted() {
        MarketNotification buyRICompleted = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.BUY_RI_COMPLETED)
            .build());
        assertEquals(Status.RUNNING, buyRICompleted.getStatusProgressNotification().getStatus());
    }

    /**
     * Make up a plan project with a main and related plan, along with a base scenario, and then
     * verify that the converted MarketApiDTO is correct.
     *
     * @throws Exception Thrown on translation error.
     */
    @Test
    public void dtoFromPlanProject() throws Exception {
        long projectId = 214056801136208L;
        // Consumption plan.
        long optimizedPlanId = 214191875872224L;
        // Allocation plan.
        long liftAndShiftPlanId = 214191875877168L;
        long optimizedScenarioId = 214191875861568L;
        long liftAndShiftScenarioId = 214191875876096L;
        String planName = "Test migration 1";

        // Create the project.
        final PlanProject.Builder projectBuilder = PlanProject.newBuilder()
                .setPlanProjectId(projectId)
                .setStatus(PlanProjectStatus.SUCCEEDED)
                .setPlanProjectInfo(PlanProjectInfo.newBuilder()
                        .setMainPlanId(optimizedPlanId)
                        .addRelatedPlanIds(liftAndShiftPlanId)
                        .build());
        final PlanProject planProject = projectBuilder.build();

        // Create both main plan (optimized) and related (Lift & Shift) plans.
        final PlanInstance optimizedPlan = getPlanInstance(optimizedPlanId,
                optimizedScenarioId, projectId, planName, CLOUD_MIGRATION_PLAN__CONSUMPTION);

        final Scenario optimizedScenario = optimizedPlan.getScenario();

        final PlanInstance liftAndShiftPlan = getPlanInstance(liftAndShiftPlanId,
                liftAndShiftScenarioId, projectId, planName, CLOUD_MIGRATION_PLAN__ALLOCATION);

        List<PlanInstance> relatedPlans = new ArrayList<>();
        relatedPlans.add(liftAndShiftPlan);

        // Get the market DTO for the project.
        final MarketApiDTO marketDTO = marketMapper.dtoFromPlanProject(planProject,
                optimizedPlan, relatedPlans, optimizedScenario);

        // Verify DTO settings. Main (optimized) plan settings are in the scenario. The
        // lift & shift plan is in the related markets list.
        assertEquals(planName, marketDTO.getDisplayName());
        ScenarioApiDTO scenarioDTO = marketDTO.getScenario();
        assertNotNull(scenarioDTO);
        assertEquals(String.format("%s_%s", planName, CLOUD_MIGRATION_PLAN__CONSUMPTION),
                scenarioDTO.getDisplayName());
        assertEquals(String.valueOf(optimizedScenarioId), scenarioDTO.getUuid());
        assertEquals(planName, marketDTO.getDisplayName());
        assertEquals(String.valueOf(optimizedPlanId), marketDTO.getUuid());
        assertEquals("SUCCEEDED", marketDTO.getState());
        assertEquals(Integer.valueOf(100), marketDTO.getStateProgress());
        assertEquals(1, marketDTO.getRelatedPlanMarkets().size());

        MarketApiDTO relatedMarketDTO = marketDTO.getRelatedPlanMarkets().get(0);
        assertEquals(String.valueOf(liftAndShiftPlanId), relatedMarketDTO.getUuid());
        ScenarioApiDTO relatedScenarioDTO = relatedMarketDTO.getScenario();
        assertNotNull(relatedScenarioDTO);
        assertEquals(String.format("%s_%s", planName, CLOUD_MIGRATION_PLAN__ALLOCATION),
                relatedScenarioDTO.getDisplayName());
        assertEquals(String.valueOf(liftAndShiftScenarioId), relatedScenarioDTO.getUuid());

        // Do an additional check for one more status.
        final PlanProject runProject = projectBuilder.setStatus(PlanProjectStatus.RUNNING)
                .build();
        final MarketApiDTO runMarketDTO = marketMapper.dtoFromPlanProject(runProject,
                optimizedPlan, relatedPlans, optimizedScenario);
        assertEquals("RUNNING", runMarketDTO.getState());
        assertEquals(Integer.valueOf(40), runMarketDTO.getStateProgress());
    }

    private PlanInstance getPlanInstance(long planId, long scenarioId, long projectId,
                                         @Nonnull final String planName,
                                         @Nonnull final String changeType) throws Exception {
        ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setSetting(Setting.newBuilder()
                                .setSettingSpecName(ConfigurableActionSettings
                                        .ResizeVcpuUpInBetweenThresholds.getSettingName())
                                .setEnumSettingValue(EnumSettingValue
                                        .newBuilder()
                                        .setValue(CLOUD_MIGRATION_PLAN__CONSUMPTION.equals(changeType)
                                                ? AUTOMATIC : DISABLED)
                                        .build()).build()).build())
                .build();
        final String scenarioName = String.format("%s_%s", planName, changeType);
        final Scenario scenario = Scenario.newBuilder()
                .setId(scenarioId)
                .setScenarioInfo(ScenarioInfo.newBuilder()
                        .addChanges(scenarioChange)
                        .setName(scenarioName).build())
                .build();

        final ScenarioApiDTO scenarioApiDTO = new ScenarioApiDTO();
        scenarioApiDTO.setUuid(Long.toString(scenarioId));
        scenarioApiDTO.setDisplayName(scenarioName);
        when(scenarioMapper.toScenarioApiDTO(Mockito.eq(scenario)))
                .thenReturn(scenarioApiDTO);

        return PlanInstance.newBuilder()
                .setName(planName)
                .setPlanId(planId)
                .setStatus(PlanStatus.READY)
                .setPlanProjectId(projectId)
                .setScenario(scenario)
                .build();
    }
}
