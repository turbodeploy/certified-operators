package com.vmturbo.topology.processor;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestTopologyProcessorDbEndpointConfig extends TopologyProcessorDbEndpointConfig {

    /**
     * Create a test Topology Processor endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint tpEndpoint(SQLDialect dialect) {
        return new TestTopologyProcessorDbEndpointConfig().testTpEndpoint(dialect);
    }

    private DbEndpoint testTpEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.tpEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
