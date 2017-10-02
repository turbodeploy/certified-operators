package com.vmturbo.plan.orchestrator;

import java.time.LocalDateTime;
import java.util.Collections;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;
import static org.junit.Assert.assertEquals;

/**
 * Tests for database schema.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    loader = AnnotationConfigContextLoader.class,
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class DatabaseSchemaIT {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    protected Flyway flyway;
    protected DSLContext dsl;

    @Before
    public void prepare() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();

        IdentityGenerator.initPrefix(0);
    }

    @After
    public void cleanup() {
        flyway.clean();
    }

    @Test
    public void testCreatePlanProject() throws Exception {
        long projectId = IdentityGenerator.next();
        LocalDateTime now = LocalDateTime.now();

        dsl.insertInto(SCENARIO)
            .set(SCENARIO.ID, projectId)
            .set(SCENARIO.CREATE_TIME, now)
            .set(SCENARIO.UPDATE_TIME, now)
            .set(SCENARIO.SCENARIO_INFO, ScenarioInfo.newBuilder()
                .setName("foo")
                .build())
            .execute();

        Result<ScenarioRecord> results = dsl.selectFrom(SCENARIO)
            .where(SCENARIO.ID.eq(projectId))
            .fetch();

        assertEquals(Collections.singletonList(projectId), results.getValues(SCENARIO.ID));
        assertEquals("foo", results.getValues(SCENARIO.SCENARIO_INFO).get(0).getName());
    }
}