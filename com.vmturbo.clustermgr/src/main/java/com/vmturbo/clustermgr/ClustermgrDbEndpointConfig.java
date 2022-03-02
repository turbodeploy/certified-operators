package com.vmturbo.clustermgr;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for clustermgr interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class ClustermgrDbEndpointConfig extends DbEndpointsConfig {

    /**
     * Endpoint for accessing clustermgr database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint clusterMgrEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.clusterMgr", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true))
                .withFlywayCallbacks(flywayCallbacks())
                .build();
    }

    private FlywayCallback[] flywayCallbacks() {
        switch (sqlDialect) {
            case MARIADB:
                return new FlywayCallback[]{};
            case POSTGRES:
                return new FlywayCallback[]{};
            default:
                return new FlywayCallback[]{};
        }
    }
}
