package com.vmturbo.repository.plan.db;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestRepositoryDbEndpointConfig extends RepositoryDBEndpointConfig {

    /**
     * Create a test Repository endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint respositoryEndpoint(SQLDialect dialect) {
        return new TestRepositoryDbEndpointConfig().testRepositoryEndpoint(dialect);
    }

    private DbEndpoint testRepositoryEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return repositoryEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
