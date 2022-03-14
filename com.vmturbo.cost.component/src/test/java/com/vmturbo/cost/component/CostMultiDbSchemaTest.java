package com.vmturbo.cost.component;

import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.SQLDialect;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbSchemaTest;

/**
 * {@inheritDoc}.
 */
@RunWith(Parameterized.class)
public class CostMultiDbSchemaTest extends MultiDbSchemaTest {

    private static final List<Pair<String, String>> CASE_SENSITIVE_COLUMNS = ImmutableList.of(
            Pair.of("cost_tag", "tag_key"),
            Pair.of("cost_tag", "tag_value")
    );

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
    public CostMultiDbSchemaTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint, CASE_SENSITIVE_COLUMNS);
    }
}
