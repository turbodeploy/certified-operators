package com.vmturbo.plan.orchestrator.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Daily;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Schedule;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.TimeOfRun;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit test for {@link PlanProjectDaoImpl}.
 */
@RunWith(Parameterized.class)
public class PlanProjectDaoImplTest extends MultiDbTestBase {

    /**
     * Provide test parameter values.
     *
     * @return parameter values
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public PlanProjectDaoImplTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Plan.PLAN, configurableDbDialect, dialect, "plan-orchestrator",
                TestPlanOrchestratorDBEndpointConfig::planEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Rule chain to manage Db provisioning and lifecycle.
     */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private PlanProjectDao planProjectDao;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        planProjectDao = new PlanProjectDaoImpl(dsl, new IdentityInitializer(0));
    }

    @Test
    public void testGetHeadroomPlanProjectList() throws Exception {
        testGetPlanProjectList(PlanProjectType.CLUSTER_HEADROOM);
    }

    @Test
    public void testGetCustomPlanProjectList() throws Exception {
        testGetPlanProjectList(PlanProjectType.USER);
    }

    private void testGetPlanProjectList(PlanProjectType planProjectType) throws Exception {
        List<PlanProjectOuterClass.PlanProject> projectList =
                planProjectDao.getPlanProjectsByType(planProjectType);

        int numOfHeadroomProjectsBeforeInsert = projectList.size();

        assertEquals(numOfHeadroomProjectsBeforeInsert, 0);

        PlanProjectOuterClass.Recurrence recurrence = PlanProjectOuterClass.Recurrence.newBuilder()
                .setSchedule(PlanProjectOuterClass.Recurrence.Schedule.newBuilder().setDaily(PlanProjectOuterClass.Recurrence.Daily.newBuilder()))
                .setTimeOfRun(PlanProjectOuterClass.Recurrence.TimeOfRun.newBuilder().setHour(5))
                .build();
        PlanProjectOuterClass.PlanProjectInfo planProjectInfo = PlanProjectOuterClass.PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(recurrence)
                .setType(planProjectType)
                .build();
        PlanProject result = planProjectDao.createPlanProject(planProjectInfo);
        assertEquals(PlanProjectStatus.READY, result.getStatus());

        projectList = planProjectDao.getPlanProjectsByType(planProjectType);
        assertEquals(projectList.size(), numOfHeadroomProjectsBeforeInsert + 1);

        Optional<PlanProject> createdPlanProject = projectList.stream()
            .filter(planProject -> planProject.getPlanProjectId() == result.getPlanProjectId())
            .findAny();

        assertTrue(createdPlanProject.isPresent());
        assertEquals(PlanProjectStatus.READY, createdPlanProject.get().getStatus());
    }

    @Test
    public void testCollectDiags() throws Exception {
        final PlanProjectOuterClass.Recurrence recurrence = PlanProjectOuterClass.Recurrence.newBuilder()
            .setSchedule(PlanProjectOuterClass.Recurrence.Schedule.newBuilder().setDaily(PlanProjectOuterClass.Recurrence.Daily.newBuilder()))
            .setTimeOfRun(PlanProjectOuterClass.Recurrence.TimeOfRun.newBuilder().setHour(5))
            .build();
        final PlanProjectOuterClass.PlanProjectInfo planProjectInfo1 = PlanProjectOuterClass.PlanProjectInfo.newBuilder()
            .setName("Plan Project 1")
            .setRecurrence(recurrence)
            .setType(PlanProjectType.USER)
            .build();
        final PlanProjectOuterClass.PlanProjectInfo planProjectInfo2 = PlanProjectOuterClass.PlanProjectInfo.newBuilder()
            .setName("Plan Project 2")
            .setRecurrence(recurrence)
            .setType(PlanProjectType.CLUSTER_HEADROOM)
            .build();

        planProjectDao.createPlanProject(planProjectInfo1);
        planProjectDao.createPlanProject(planProjectInfo2);

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        planProjectDao.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());

        final List<String> expected = planProjectDao.getAllPlanProjects().stream()
            .map(project -> PlanProjectDaoImpl.GSON.toJson(project, PlanProject.class))
            .collect(Collectors.toList());
        assertEquals(expected, diags.getAllValues());
    }

    @Test
    public void testRestoreFromDiags() throws Exception {

        final Recurrence recurrence = Recurrence.newBuilder()
            .setSchedule(Schedule.newBuilder().setDaily(Daily.getDefaultInstance()))
            .setTimeOfRun(TimeOfRun.newBuilder().setHour(5))
            .build();
        final PlanProjectInfo preexisting = PlanProjectInfo.newBuilder()
            .setName("preexisting")
            .setRecurrence(recurrence)
            .setType(PlanProjectType.USER)
            .build();
        planProjectDao.createPlanProject(preexisting);

        final List<String> diags = Arrays.asList(
                "{\"planProjectId\":\"1997789474912\",\"planProjectInfo\":{\"name\":"
                        + "\"Plan Project 1\",\"recurrence\":{\"schedule\":{\"daily\":{}},\"timeOfRun\":"
                        + "{\"hour\":5}},\"type\":\"USER\"},\"status\":\"SUCCEEDED\"}",
                "{\"planProjectId\":\"1997789479760\",\"planProjectInfo\":{\"name\":"
                        + "\"Plan Project 2\",\"recurrence\":{\"schedule\":{\"daily\":{}},\"timeOfRun\":"
                        + "{\"hour\":5}},\"type\":\"CLUSTER_HEADROOM\"},\"status\":\"SUCCEEDED\"}"
        );
        try {
            planProjectDao.restoreDiags(diags, null);
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting plan projects"));
        }

        final List<PlanProject> result = planProjectDao.getAllPlanProjects();

        assertEquals(2, result.size());
        assertTrue(result.stream()
            .noneMatch(found -> found.getPlanProjectInfo().equals(preexisting)));
        assertEquals(diags.stream()
            .map(serial -> PlanProjectDaoImpl.GSON.fromJson(serial, PlanProject.class))
            .collect(Collectors.toList()),
            result
        );

    }
}
