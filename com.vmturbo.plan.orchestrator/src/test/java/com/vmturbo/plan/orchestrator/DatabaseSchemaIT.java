package com.vmturbo.plan.orchestrator;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests for database schema.
 */
@RunWith(Parameterized.class)
public class DatabaseSchemaIT extends MultiDbTestBase {

    /**
     * Parameters to control DB access during each test execution.
     *
     * @return parameter values
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new test class instance with test parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         {@link SQLDialect} to use
     * @throws SQLException                if a DB problem occurs
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interuptted
     */
    public DatabaseSchemaIT(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Plan.PLAN, configurableDbDialect, dialect, "plan-orchestrator",
                TestPlanOrchestratorDBEndpointConfig::planEndpoint);
        this.dsl = super.getDslContext();
    }

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