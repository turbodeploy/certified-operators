package com.vmturbo.auth.component;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestAuthDbEndpointConfig extends AuthDbEndpointConfig {

    /**
     * Create a test Auth endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint authDbEndpoint(SQLDialect dialect) {
        return new TestAuthDbEndpointConfig().testAuthDbEndpoint(dialect);
    }

    private DbEndpoint testAuthDbEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.authDbEndpoint("auth", "vmturbo", "vmturbo", "postgres", "root");
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
