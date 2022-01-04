package com.vmturbo.cost.component.db;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestCostDbEndpointConfig extends CostDbEndpointConfig {

    /**
     * Create a test Cost endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint costEndpoint(SQLDialect dialect) {
        return new TestCostDbEndpointConfig().testCostEndpoint(dialect);
    }

    private DbEndpoint testCostEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.costEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
