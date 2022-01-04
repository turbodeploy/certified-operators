package com.vmturbo.group.db;

import org.jooq.SQLDialect;

import com.vmturbo.group.GroupDBEndpointConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestGroupDBEndpointConfig extends GroupDBEndpointConfig {

    /**
     * Create a test Group endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint groupEndpoint(SQLDialect dialect) {
        return new TestGroupDBEndpointConfig().testGroupEndpoint(dialect);
    }

    private DbEndpoint testGroupEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.groupEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
