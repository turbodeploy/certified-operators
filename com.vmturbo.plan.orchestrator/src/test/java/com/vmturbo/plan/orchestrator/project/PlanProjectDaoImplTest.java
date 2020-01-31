package com.vmturbo.plan.orchestrator.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
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
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit test for {@link PlanProjectDaoImpl}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class PlanProjectDaoImplTest {

    @Autowired
    private TestSQLDatabaseConfig dbConfig;
    private PlanProjectDao planProjectDao;
    private Flyway flyway;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        planProjectDao = new PlanProjectDaoImpl(dbConfig.dsl(), new IdentityInitializer(0));
        prepareDatabase();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
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
        planProjectDao.createPlanProject(planProjectInfo);

        projectList = planProjectDao.getPlanProjectsByType(planProjectType);
        assertEquals(projectList.size(), numOfHeadroomProjectsBeforeInsert + 1);
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
            "{\"planProjectId\":\"1997789474912\",\"planProjectInfo\":{\"name\":" +
                "\"Plan Project 1\",\"recurrence\":{\"schedule\":{\"daily\":{}},\"timeOfRun\":" +
                "{\"hour\":5}},\"type\":\"USER\"}}",
            "{\"planProjectId\":\"1997789479760\",\"planProjectInfo\":{\"name\":" +
                "\"Plan Project 2\",\"recurrence\":{\"schedule\":{\"daily\":{}},\"timeOfRun\":" +
                "{\"hour\":5}},\"type\":\"CLUSTER_HEADROOM\"}}"
        );
        try {
            planProjectDao.restoreDiags(diags);
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

    @After
    public void teardown() {
        flyway.clean();
    }
}
