package com.vmturbo.plan.orchestrator;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;
import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.util.Collections;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Tests for database schema.
 */
public class DatabaseSchemaIT {
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

    protected DSLContext dsl = dbConfig.getDslContext();

    @Before
    public void prepare() throws Exception {
        IdentityGenerator.initPrefix(0);
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