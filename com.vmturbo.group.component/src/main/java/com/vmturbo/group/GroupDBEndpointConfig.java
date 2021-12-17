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
     * DB user name accessible to given schema. This is needed since the component name
     * "group" is different from the db username, and group_component is
     * not provided by operator. If not provided, DbEndpoint will use component name as username.
     */
    private static final String groupDbUsername = "group_component";

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
                .withUserName(groupDbUsername)
                .build();
    }
}
