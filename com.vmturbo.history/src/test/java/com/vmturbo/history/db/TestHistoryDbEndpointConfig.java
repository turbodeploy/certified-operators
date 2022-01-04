package com.vmturbo.history.db;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestHistoryDbEndpointConfig extends HistoryDbEndpointConfig {

    /**
     * Create a test History endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint historyEndpoint(SQLDialect dialect) {
        return new TestHistoryDbEndpointConfig().testHistoryEndpoint(dialect);
    }

    private DbEndpoint testHistoryEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.historyEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }

}
