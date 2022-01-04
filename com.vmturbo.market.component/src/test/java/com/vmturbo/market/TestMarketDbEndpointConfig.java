package com.vmturbo.market;

import org.jooq.SQLDialect;

import com.vmturbo.market.db.MarketDbEndpointConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to create DbEndpoint instances for tests.
 */
public class TestMarketDbEndpointConfig extends MarketDbEndpointConfig {

    /**
     * Create a test Market endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public static DbEndpoint marketEndpoint(SQLDialect dialect) {
        return new TestMarketDbEndpointConfig().testMarketEndpoint(dialect);
    }

    private DbEndpoint testMarketEndpoint(SQLDialect dialect) {
        super.sqlDialect = dialect;
        return super.marketEndpoint();
    }

    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }

}


