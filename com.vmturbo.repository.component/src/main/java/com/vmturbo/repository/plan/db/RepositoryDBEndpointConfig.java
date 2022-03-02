package com.vmturbo.repository.plan.db;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for repository interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class RepositoryDBEndpointConfig extends DbEndpointsConfig {

    /**
     * Explicitly declare Repository DB username.
     */
    private static final String repositoryDbUsername = "repository";

    /**
     * Endpoint for accessing repository database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint repositoryEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.repository", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true)
                // workaround since the Environment doesn't contain repositoryDbUsername
                // fixEndpointForMultiDb can't find this property from spring environment
                .withUserName(repositoryDbUsername))
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
