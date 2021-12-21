package com.vmturbo.action.orchestrator;

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
                // workaround since the Environment doesn't contain aoDbUsername
                // fixEndpointForMultiDb can't find this property from spring environment
                .withUserName(aoDbUsername)
                // TODO this is needed because we have not created the new migration structure yet
                //  remove once the integration with postgres is done
                .withMigrationLocations("db.migration"))
                .build();
    }
}
