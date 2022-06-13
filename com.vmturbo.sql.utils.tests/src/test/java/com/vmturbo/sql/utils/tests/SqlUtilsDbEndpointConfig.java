package com.vmturbo.sql.utils.tests;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to define {@link DbEndpoint} needed for live DB tests.
 */
public class SqlUtilsDbEndpointConfig extends DbEndpointsConfig {

    /**
     * DB/schema name.
     */
    public static final String SQL_UTILS_SCHEMA_NAME = "sql_utils";

    private SqlUtilsDbEndpointConfig() {}

    /**
     * Provide access to a singleton instance.
     */
    public static SqlUtilsDbEndpointConfig config = new SqlUtilsDbEndpointConfig();

    /**
     * Create an endpoint for tests.
     *
     * @param dialect desired dialect
     * @return new endpoint
     */
    public DbEndpoint sqlUtilsEndpoint(SQLDialect dialect) {
        return dbEndpoint("test_utils", dialect)
                .withAccess(DbEndpointAccess.ALL)
                .withShouldProvision(true)
                .withRootAccessEnabled(true)
                .withDatabaseName(SQL_UTILS_SCHEMA_NAME)
                .withSchemaName(SQL_UTILS_SCHEMA_NAME)
                .build();
    }


    @Override
    public DbEndpointCompleter endpointCompleter() {
        return MultiDbTestBase.getTestCompleter();
    }
}
