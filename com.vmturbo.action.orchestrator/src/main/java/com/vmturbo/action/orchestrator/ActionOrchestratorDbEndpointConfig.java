package com.vmturbo.action.orchestrator;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.action.orchestrator.flyway.V1v24Callback;
import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * Configuration for repository interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class ActionOrchestratorDbEndpointConfig extends DbEndpointsConfig {

    /**
     * Explicitly declare Action Orchestrator DB username.
     */
    private static final String aoDbUsername = "action";

    /**
     * Endpoint for accessing repository database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint actionOrchestratorEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.action-orchestrator", sqlDialect)
                .withShouldProvision(true)
                .withRootAccessEnabled(true)
                .withAccess(DbEndpointAccess.ALL)
                .withFlywayCallbacks(flywayCallbacks())
                // workaround since the Environment doesn't contain aoDbUsername
                // fixEndpointForMultiDb can't find this property from spring environment
                .withUserName(aoDbUsername))
                .build();
    }

    private FlywayCallback[] flywayCallbacks() {
        switch (sqlDialect) {
            case MARIADB:
                return new FlywayCallback[]{
                    // V1.19 migration was replaced due to performance-related failures at large
                    // installations - see OM-67686
                    new ResetMigrationChecksumCallback("1.19",
                            ImmutableSet.of(1998263184, -814613695), 1120921943),
                    // V1.24 migration checksum needs to change.
                    new V1v24Callback()
                };
            case POSTGRES:
                return new FlywayCallback[]{};
            default:
                return new FlywayCallback[]{};

        }
    }
}
