package com.vmturbo.clustermgr;

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
public class ClustermgrDBConfig2 extends DbEndpointsConfig {

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
                .build();
    }
}
