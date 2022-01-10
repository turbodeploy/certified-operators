package com.vmturbo.action.orchestrator;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestActionOrchestratorDbEndpointConfig extends ActionOrchestratorDbEndpointConfig {

    /**
     * Create a test AO endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint actionOrchestratorEndpoint(SQLDialect dialect) {
        return new TestActionOrchestratorDbEndpointConfig().testActionOrchestratorEndpoint(dialect);
    }

    private DbEndpoint testActionOrchestratorEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.actionOrchestratorEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
