package com.vmturbo.plan.orchestrator;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for plan orchestrator component interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class PlanOrchestratorDBEndpointConfig extends DbEndpointsConfig {

    private static final String PLAN_ORCHESTRATOR_USER_NAME = "plan";

    /**
     * Endpoint for accessing plan orchestrator database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint planEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.plan", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true)
                .withUserName(PLAN_ORCHESTRATOR_USER_NAME))
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
