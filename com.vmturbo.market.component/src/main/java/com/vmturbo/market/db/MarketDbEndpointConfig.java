package com.vmturbo.market.db;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for market interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class MarketDbEndpointConfig extends DbEndpointsConfig {

    /**
     * If a database username is not provided, by default the component name will be used.
     * Since the component name is "market-component" and the expected db username is "market",
     * we must set the username explicitly.
     */
    private static final String marketDbUsername = "market";

    /**
     * Endpoint for accessing market database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint marketEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.market", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true))
                .withUserName(marketDbUsername)
                // TODO this is needed because we have not created the new migration structure yet
                // remove once the integration with postgres is done
                .withMigrationLocations("db.migration")
                .build();
    }
}
