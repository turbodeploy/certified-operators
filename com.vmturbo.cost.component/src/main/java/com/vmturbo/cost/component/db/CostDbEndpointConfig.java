package com.vmturbo.cost.component.db;

import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for cost interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class CostDbEndpointConfig extends DbEndpointsConfig {

    /**
     * If a database username is not provided, by default the component name will be used.
     * Since the component name is "com.vmturbo.cost.component" and the expected db username is "cost",
     * we must set the username explicitly.
     */
    private static final String costDbUsername = "cost";

    /**
     * Endpoint for accessing cost database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint costEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.cost", SQLDialect.MARIADB)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true))
                .withUserName(costDbUsername)
                .withMigrationLocations("db.migration").build();
    }

    /**
     * Get the {@link DSLContext} with {@link DbEndpoint}.
     *
     * @return {@link DSLContext}
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Bean
    public DSLContext dsl() throws SQLException, UnsupportedDialectException, InterruptedException {
        return costEndpoint().dslContext();
    }
}

