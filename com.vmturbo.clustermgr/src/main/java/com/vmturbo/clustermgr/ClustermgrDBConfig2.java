package com.vmturbo.clustermgr;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointBuilder;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration for clustermgr interaction with db through {@link DbEndpoint}.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class ClustermgrDBConfig2 extends DbEndpointsConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${clustermgrDbUsername:clustermgr}")
    private String clustermgrDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${clustermgrDbPassword:}")
    private String clustermgrDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:clustermgr}")
    private String dbSchemaName;

    @Value("${dbHost}")
    private String dbHost;

    /**
     * Endpoint for accessing clustermgr database.
     *
     * @return endpoint instance
     */
    @Bean
    public DbEndpoint clusterMgrEndpoint() {
        final DbEndpointBuilder dbEndpointBuilder = dbEndpoint("dbs.clusterMgr", sqlDialect)
                .withUserName(clustermgrDbUsername)
                .withSchemaName(dbSchemaName)
                .withShouldProvision(true)
                .withAccess(DbEndpointAccess.ALL)
                .withRootAccessEnabled(true)
                //todo: different location for mariadb/postgres
                .withMigrationLocations("db.migration");
        if (!Strings.isEmpty(clustermgrDbPassword)) {
            dbEndpointBuilder.withPassword(clustermgrDbPassword);
        }
        if (!Strings.isEmpty(dbHost)) {
            dbEndpointBuilder.withHost(dbHost);
        }
        return dbEndpointBuilder.build();
    }
}
