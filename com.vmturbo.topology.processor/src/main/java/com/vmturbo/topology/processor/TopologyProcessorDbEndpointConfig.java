package com.vmturbo.topology.processor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for topology-processor component interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class TopologyProcessorDbEndpointConfig extends DbEndpointsConfig {

    /**
     * DB user name accessible to given schema. This is needed since the component name
     * "topology-processor" is different from the db username, and topologyProcessorDbUsername is
     * not provided by operator. If not provided, DbEndpoint will use component name as username.
     */
    private static final String topologyProcessorDbUsername = "topology_processor";

    /**
     * Endpoint for accessing topology_processor database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint tpEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.topology_processor", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true)
                // workaround since the Environment doesn't contain topologyProcessorDbUsername
                // fixEndpointForMultiDb can't find this property from spring environment
                .withUserName(topologyProcessorDbUsername))
                .build();
    }
}

