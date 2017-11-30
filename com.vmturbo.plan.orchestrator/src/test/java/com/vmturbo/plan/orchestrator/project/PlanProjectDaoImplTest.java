package com.vmturbo.plan.orchestrator.project;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
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
        List<PlanDTO.PlanProject> projectList =
                planProjectDao.getPlanProjectsByType(planProjectType);

        int numOfHeadroomProjectsBeforeInsert = projectList.size();

        assertEquals(numOfHeadroomProjectsBeforeInsert, 0);

        PlanDTO.Recurrence recurrence = PlanDTO.Recurrence.newBuilder()
                .setSchedule(PlanDTO.Recurrence.Schedule.newBuilder().setDaily(PlanDTO.Recurrence.Daily.newBuilder()))
                .setTimeOfRun(PlanDTO.Recurrence.TimeOfRun.newBuilder().setHour(5))
                .build();
        PlanDTO.PlanProjectInfo planProjectInfo = PlanDTO.PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(recurrence)
                .setType(planProjectType)
                .build();
        planProjectDao.createPlanProject(planProjectInfo);

        projectList = planProjectDao.getPlanProjectsByType(planProjectType);
        assertEquals(projectList.size(), numOfHeadroomProjectsBeforeInsert + 1);
    }

    @After
    public void teardown() {
        flyway.clean();
    }
}
