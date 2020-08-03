package com.vmturbo.plan.orchestrator.scheduled;

import static com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT;
import static com.vmturbo.plan.orchestrator.scheduled.PlanProjectScheduler.createCronTrigger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Daily;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.DayOfWeek;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Monthly;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.TimeOfRun;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Weekly;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanProject;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectDaoImpl;
import com.vmturbo.plan.orchestrator.project.PlanProjectExecutor;
import com.vmturbo.plan.orchestrator.project.PlanProjectInfoNotFoundException;
import com.vmturbo.plan.orchestrator.project.PlanProjectNotFoundException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Tests for the {@link PlanProjectScheduler} class.
 */
public class PlanProjectSchedulerTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Plan.PLAN);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private static final long DAILY_PLAN_PROJECT_ID = 1000L;
    private static final long WEEKLY_PLAN_PROJECT_ID = 1001L;
    private static final long MONTHLY_PLAN_PROJECT_ID = 1002L;
    private static final long INVALID_MONTHLY_PLAN_PROJECT_ID = 1003L;
    private static final long NOT_EXIST_PLAN_PROJECT_ID = 9999L;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private PlanProjectScheduler planProjectScheduler;
    private PlanProjectDao planProjectDao;
    private SystemPlanProjectLoader systemPlanProjectLoader;

    private static final String defaultHeadroomPlanProjectJsonFile = "systemPlanProjects.json";

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        planProjectDao = new PlanProjectDaoImpl(dsl, new IdentityInitializer(0));
        threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setThreadFactory(new ThreadFactoryBuilder()
                .setNameFormat("cluster-rollup-test-%d")
                .build());
        threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        threadPoolTaskScheduler.initialize();
        prepareDatabase();

        PlanProjectExecutor planProjectExecutor = Mockito.mock(PlanProjectExecutor.class);
        planProjectScheduler = spy(new PlanProjectScheduler(planProjectDao, threadPoolTaskScheduler,
                planProjectExecutor));

        systemPlanProjectLoader = new SystemPlanProjectLoader(planProjectDao,
                planProjectScheduler,
                defaultHeadroomPlanProjectJsonFile);
    }

    private void prepareDatabase() throws Exception {
        Recurrence dailyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        Recurrence weeklyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setWeekly(Weekly.newBuilder()
                                .setDayOfWeek(DayOfWeek.MON)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        Recurrence monthlyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setMonthly(Monthly.newBuilder()
                                .setDayOfMonth(1)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        Recurrence invalidMonthlyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setMonthly(Monthly.newBuilder()
                                .setDayOfMonth(99))) // 99 is not a valid day of month
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();

        populatePlanProjectTestData(invalidMonthlyRecurrence, INVALID_MONTHLY_PLAN_PROJECT_ID);
        populatePlanProjectTestData(dailyRecurrence, DAILY_PLAN_PROJECT_ID);
        populatePlanProjectTestData(weeklyRecurrence, WEEKLY_PLAN_PROJECT_ID);
        populatePlanProjectTestData(dailyRecurrence, MONTHLY_PLAN_PROJECT_ID);

    }

    /**
     * Create default system plan projects and verify default HEADROOM project is created and scheduled.
     */
    @Test
    public void testVerifyDefaultPlanProjectsCreated() {
        List<PlanProjectOuterClass.PlanProject> projectList =
                planProjectDao.getPlanProjectsByType(PlanProjectType.CLUSTER_HEADROOM);
        assertTrue(projectList.size() == 1);
        verifyScheduler(projectList.get(0).getPlanProjectId());
    }

    @Test
    public void testInitializePlanProjectSchedule() throws Exception {
        verifyScheduler(DAILY_PLAN_PROJECT_ID);
        verifyScheduler(WEEKLY_PLAN_PROJECT_ID);
        verifyScheduler(MONTHLY_PLAN_PROJECT_ID);
    }

    // verify invalid plan project will NOT prevent scheduling subsequent tasks.
    @Test
    public void testInitializePlanProjectScheduleWithInvalidRecurrence() throws Exception {
        Optional<PlanProjectSchedule> planProjectScheduleOptional = planProjectScheduler
                .getPlanProjectSchedule(INVALID_MONTHLY_PLAN_PROJECT_ID);
        assertFalse(planProjectScheduleOptional.isPresent());
    }

    @Test
    public void testSetPlanProjectSchedule() throws Exception {
        long planProjectId = 10013L;
        Recurrence dailyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(11))
                .build();
        populatePlanProjectTestData(dailyRecurrence, planProjectId);
        planProjectScheduler.setPlanProjectSchedule(planProjectId);
        verifyScheduler(planProjectId);
    }

    @Test
    public void testSetPlanProjectScheduleWithNotExistPlanProject() throws Exception {
        expectedException.expect(PlanProjectNotFoundException.class);
        planProjectScheduler.setPlanProjectSchedule(NOT_EXIST_PLAN_PROJECT_ID);
    }

    @Test
    public void testSetPlanProjectScheduleWithNotExistPlanProjectInfo() throws Exception {
        long planProjectId = 10014L;
        LocalDateTime curTime = LocalDateTime.now();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project Info")
                .build();
        PlanProject planProject = new
                PlanProject(planProjectId, curTime, curTime, toCreate, PlanProjectType.USER.name());
        dsl.newRecord(PLAN_PROJECT, planProject).store();
        expectedException.expect(PlanProjectInfoNotFoundException.class);
        planProjectScheduler.setPlanProjectSchedule(planProjectId);
    }

    @Test
    public void testGetPlanProjectSchedule() throws Exception {
        verifyScheduler(MONTHLY_PLAN_PROJECT_ID);
    }

    @Test
    public void testGetPlanProjectScheduleWithNotExistPlanProject() throws Exception {
        Optional<PlanProjectSchedule> scheduleOptional = planProjectScheduler
                .getPlanProjectSchedule(NOT_EXIST_PLAN_PROJECT_ID);
        assertFalse(scheduleOptional.isPresent());
    }

    @Test
    public void testCancelPlanProjectSchedule() throws Exception {
        long planProjectId = 10014L;
        Recurrence dailyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(11))
                .build();
        populatePlanProjectTestData(dailyRecurrence, planProjectId);
        PlanProjectSchedule schedule = planProjectScheduler.setPlanProjectSchedule(planProjectId);
        assertFalse(schedule.isCancelled());
        planProjectScheduler.cancelPlanProjectSchedule(planProjectId);
        assertTrue(schedule.isCancelled());
    }

    @Test
    public void testCancelPlanProjectScheduleWithNotExistPlanProject() throws Exception {
        Optional<PlanProjectSchedule> scheduleOptional = planProjectScheduler
                .cancelPlanProjectSchedule(NOT_EXIST_PLAN_PROJECT_ID);
        assertFalse(scheduleOptional.isPresent());
    }

    private void verifyScheduler(long planProjectId) {
        Optional<PlanProjectSchedule> planProjectScheduleOptional = planProjectScheduler
                .getPlanProjectSchedule(planProjectId);
        PlanProjectSchedule scheduler = planProjectScheduleOptional.orElse(null);
        assertNotNull(scheduler);
        assertEquals(planProjectId, scheduler.getPlanProjectId());
        assertFalse(scheduler.isCancelled());
        scheduler.cancel();
        assertTrue(scheduler.isCancelled());
    }

    private void populatePlanProjectTestData(Recurrence recurrence, long planProjecId) {
        LocalDateTime curTime = LocalDateTime.now();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project Info")
                .setRecurrence(recurrence)
                .build();

        PlanProject planProject = new
                PlanProject(planProjecId, curTime, curTime, toCreate, PlanProjectType.USER.name());
        dsl.newRecord(PLAN_PROJECT, planProject).store();
    }

    // Test run daily at 7PM
    // should return new CronTrigger("0 0 19 * * *")
    @Test
    public void testCreateDailyPlanCronTrigger() throws Exception {
        Recurrence dailyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(dailyRecurrence)
                .build();
        Optional<Trigger> trigger = createCronTrigger(toCreate);
        Trigger expectedTrigger = new CronTrigger("0 0 19 * * *");
        assertTrue(trigger.isPresent());
        assertEquals(trigger.get(), expectedTrigger);
    }

    // Test run weekly on Monday at 7PM
    // should return new CronTrigger("0 0 19 ? * 1")
    @Test
    public void testCreateWeeklyPlanCronTrigger() throws Exception {
        Recurrence weeklyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setWeekly(Weekly.newBuilder()
                                .setDayOfWeek(DayOfWeek.MON)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(weeklyRecurrence)
                .build();
        Optional<Trigger> trigger = createCronTrigger(toCreate);
        Trigger expectedTrigger = new CronTrigger("0 0 19 ? * 1");
        assertTrue(trigger.isPresent());
        assertEquals(trigger.get(), expectedTrigger);
    }

    // Test run Monthly on first day at 7PM
    // should return new CronTrigger("0 0 19 1 * ?")
    @Test
    public void testCreateMonthlyPlanCronTrigger() throws Exception {
        Recurrence monthlyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setMonthly(Monthly.newBuilder()
                                .setDayOfMonth(1)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(monthlyRecurrence)
                .build();
        Optional<Trigger> trigger = createCronTrigger(toCreate);
        Trigger expectedTrigger = new CronTrigger("0 0 19 1 * ?");
        assertTrue(trigger.isPresent());
        assertEquals(trigger.get(), expectedTrigger);
    }


    @Test
    public void testCreateDailyWithInvalidHour() throws Exception {
        Recurrence dailyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(24))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(dailyRecurrence)
                .build();
        expectedException.expect(IllegalArgumentException.class);
        Optional<Trigger> trigger = createCronTrigger(toCreate);
    }

    @Test
    public void testCreateWeeklyWithInvalidHour() throws Exception {
        Recurrence weeklyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setWeekly(Weekly.newBuilder()
                                .setDayOfWeek(DayOfWeek.MON)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(-1))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(weeklyRecurrence)
                .build();
        expectedException.expect(IllegalArgumentException.class);
        Optional<Trigger> trigger = createCronTrigger(toCreate);
    }

    @Test
    public void testCreateMonthlyWithInvalidHour() throws Exception {
        Recurrence monthlyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setMonthly(Monthly.newBuilder()
                                .setDayOfMonth(1)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(Integer.MAX_VALUE))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(monthlyRecurrence)
                .build();
        expectedException.expect(IllegalArgumentException.class);
        Optional<Trigger> trigger = createCronTrigger(toCreate);
    }

    @Test
    public void testCreateMonthlyWithInvalidMonthUpperBound() throws Exception {
        Recurrence monthlyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setMonthly(Monthly.newBuilder()
                                .setDayOfMonth(32)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(0))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(monthlyRecurrence)
                .build();
        expectedException.expect(IllegalArgumentException.class);
        Optional<Trigger> trigger = createCronTrigger(toCreate);
    }

    @Test
    public void testCreateMonthlyWithInvalidMonthLowerBound() throws Exception {
        Recurrence monthlyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder()
                        .setMonthly(Monthly.newBuilder()
                                .setDayOfMonth(-1)))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(0))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(monthlyRecurrence)
                .build();
        expectedException.expect(IllegalArgumentException.class);
        Optional<Trigger> trigger = createCronTrigger(toCreate);
    }

    @Test
    public void testCreateInvalidSchedule() throws Exception {
        // recurrence intentionally NOT setting schedule.
        Recurrence monthlyRecurrence = Recurrence.newBuilder()
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(0))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(monthlyRecurrence)
                .build();
        expectedException.expect(IllegalArgumentException.class);
        Optional<Trigger> trigger = createCronTrigger(toCreate);
    }
}
