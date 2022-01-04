package com.vmturbo.clustermgr;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestClustermgrDbEndpointConfig extends ClustermgrDbEndpointConfig {

    /**
     * Create a test ClusterMgr endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint clusterMgrEndpoint(SQLDialect dialect) {
        return new TestClustermgrDbEndpointConfig().testClusterMgrEndpoint(dialect);
    }

    private DbEndpoint testClusterMgrEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.clusterMgrEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
