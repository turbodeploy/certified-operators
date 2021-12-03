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
                .build();
    }
}
