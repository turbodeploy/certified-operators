package com.vmturbo.plan.orchestrator;

import java.sql.SQLException;

import org.jooq.SQLDialect;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbSchemaTest;

/**
 * {@inheritDoc}.
 */
@RunWith(Parameterized.class)
public class PlanMultiDbSchemaTest extends MultiDbSchemaTest {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return new Object[][]{ DBENDPOINT_POSTGRES_PARAMS };
    }

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public PlanMultiDbSchemaTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Plan.PLAN, configurableDbDialect, dialect, "plan-orchestrator",
                TestPlanOrchestratorDBEndpointConfig::planEndpoint);
    }
}
