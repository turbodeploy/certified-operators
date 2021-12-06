package com.vmturbo.plan.orchestrator;

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

    /**
     * DB user name accessible to given schema. This is needed since the component name
     * "topology-processor" is different from the db username, and topologyProcessorDbUsername is
     * not provided by operator. If not provided, DbEndpoint will use component name as username.
     */
    private static final String planOrchestratorDbUsername = "plan";

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
                // workaround since the Environment doesn't contain topologyProcessorDbUsername
                // fixEndpointForMultiDb can't find this property from spring environment
                .withUserName(planOrchestratorDbUsername)
                // TODO this is needed because we have not created the new migration structure yet
                //  remove once the integration with postgres is done
                .withMigrationLocations("db.migration"))
                .build();
    }

}
