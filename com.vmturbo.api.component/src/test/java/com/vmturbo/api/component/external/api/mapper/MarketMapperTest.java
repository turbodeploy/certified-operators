package com.vmturbo.api.component.external.api.mapper;

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
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;

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

        Mockito.when(scenarioMapper.toScenarioApiDTO(Mockito.eq(BASE.getScenario())))
                .thenReturn(scenarioApiDTO);
    }

    @Test
    public void testDtoFromPlanInstanceInProgress() throws Exception {
        MarketApiDTO inProgressDto = marketMapper.dtoFromPlanInstance(IN_PROGRESS_INSTANCE);
        Assert.assertEquals(true, inProgressDto.getSaved());
        Assert.assertEquals(DateTimeUtil.toString(START_TIME), inProgressDto.getRunDate());
        Assert.assertEquals("RUNNING", inProgressDto.getState());
        Assert.assertTrue(inProgressDto.getStateProgress() < 100 && inProgressDto.getStateProgress() >= 0);
        Assert.assertNull(inProgressDto.getRunCompleteDate());
        Assert.assertEquals(Long.toString(SCENARIO_ID), inProgressDto.getScenario().getUuid());
        Assert.assertEquals(SCENARIO_NAME, inProgressDto.getScenario().getDisplayName());
        Assert.assertEquals(CREATED_BY_USER, inProgressDto.getScenario().getOwners().get(0).getUuid());
        Assert.assertEquals(SCENARIO_NAME, inProgressDto.getDisplayName());
    }

    @Test
    public void testDtoFromPlanInstanceSucceeded() throws Exception {
        MarketApiDTO succeededDto = marketMapper.dtoFromPlanInstance(SUCCEEDED_INSTANCE);
        Assert.assertEquals("Market", succeededDto.getClassName());
        Assert.assertEquals(true, succeededDto.getSaved());
        Assert.assertEquals(DateTimeUtil.toString(START_TIME), succeededDto.getRunDate());
        Assert.assertEquals("SUCCEEDED", succeededDto.getState());
        Assert.assertEquals(Integer.valueOf(100), succeededDto.getStateProgress());
        Assert.assertEquals(DateTimeUtil.toString(END_TIME), succeededDto.getRunCompleteDate());
        Assert.assertEquals(Long.toString(SCENARIO_ID), succeededDto.getScenario().getUuid());
        Assert.assertEquals(SCENARIO_NAME, succeededDto.getScenario().getDisplayName());
        Assert.assertEquals(SCENARIO_NAME, succeededDto.getDisplayName());
    }

    @Test
    public void testDtoFromPlanInstanceFailed() throws Exception {
        MarketApiDTO failedDto = marketMapper.dtoFromPlanInstance(FAILED_INSTANCE);
        Assert.assertEquals(true, failedDto.getSaved());
        Assert.assertEquals(DateTimeUtil.toString(START_TIME), failedDto.getRunDate());
        Assert.assertEquals("STOPPED", failedDto.getState());
        Assert.assertEquals(Integer.valueOf(100), failedDto.getStateProgress());
        Assert.assertEquals(DateTimeUtil.toString(END_TIME), failedDto.getRunCompleteDate());
        Assert.assertEquals(Long.toString(SCENARIO_ID), failedDto.getScenario().getUuid());
        Assert.assertEquals(SCENARIO_NAME, failedDto.getScenario().getDisplayName());
        Assert.assertEquals(SCENARIO_NAME, failedDto.getDisplayName());
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
        Assert.assertEquals(dto.getDisplayName(), planName);
    }

    @Test
    public void testNotificationFromPlanInstanceInProgress() throws Exception {
        MarketNotification inProgress = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.WAITING_FOR_RESULT)
            .build());
        Assert.assertEquals(Long.toString(PLAN_ID), inProgress.getMarketId());
        Assert.assertTrue(inProgress.hasStatusProgressNotification());
        Assert.assertEquals(Status.RUNNING, inProgress.getStatusProgressNotification().getStatus());
    }

    @Test
    public void testNotificationFromPlanInstanceSucceeded() throws Exception {
        MarketNotification success = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.SUCCEEDED)
            .build());
        Assert.assertEquals(Long.toString(PLAN_ID), success.getMarketId());
        Assert.assertTrue(success.hasStatusNotification());
        Assert.assertEquals(Status.SUCCEEDED, success.getStatusNotification().getStatus());
    }

    @Test
    public void testNotificationFromPlanInstanceFailed() throws Exception {
        MarketNotification failure = MarketMapper.notificationFromPlanStatus(StatusUpdate.newBuilder()
            .setPlanId(PLAN_ID)
            .setNewPlanStatus(PlanStatus.FAILED)
            .build());
        Assert.assertEquals(Long.toString(PLAN_ID), failure.getMarketId());
        Assert.assertTrue(failure.hasStatusNotification());
        Assert.assertEquals(Status.STOPPED, failure.getStatusNotification().getStatus());
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
        Assert.assertEquals(Status.RUNNING, startBuyRi.getStatusProgressNotification().getStatus());
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
        Assert.assertEquals(Status.RUNNING, buyRICompleted.getStatusProgressNotification().getStatus());
    }

}
