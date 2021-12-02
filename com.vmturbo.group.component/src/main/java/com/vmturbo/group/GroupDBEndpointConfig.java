package com.vmturbo.group;

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
public class GroupDBEndpointConfig extends DbEndpointsConfig {

    /**
     * Endpoint for accessing repository database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint groupEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.group", sqlDialect)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true))
                .build();
    }
}
