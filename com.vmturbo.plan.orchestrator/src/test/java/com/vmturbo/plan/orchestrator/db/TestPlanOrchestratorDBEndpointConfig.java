package com.vmturbo.plan.orchestrator.db;

import org.jooq.SQLDialect;

import com.vmturbo.plan.orchestrator.PlanOrchestratorDBEndpointConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestPlanOrchestratorDBEndpointConfig extends PlanOrchestratorDBEndpointConfig {

    /**
     * Create a test Plan Orchestrator endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint planEndpoint(SQLDialect dialect) {
        return new TestPlanOrchestratorDBEndpointConfig().testPlanEndpoint(dialect);
    }

    private DbEndpoint testPlanEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.planEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
