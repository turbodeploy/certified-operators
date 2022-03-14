package com.vmturbo.market;

import java.sql.SQLException;

import org.jooq.SQLDialect;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.market.component.db.Market;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbSchemaTest;

/**
 * {@inheritDoc}.
 */
@RunWith(Parameterized.class)
public class MarketMultiDbSchemaTest extends MultiDbSchemaTest {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return new Object[][]{ DBENDPOINT_POSTGRES_PARAMS };
    }

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public MarketMultiDbSchemaTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Market.MARKET, configurableDbDialect, dialect, "market",
                TestMarketDbEndpointConfig::marketEndpoint);
    }
}
