package com.vmturbo.history.db;

import org.jooq.SQLDialect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Config class to establish DB access for history compoennt, based on {@link DbEndpoint} facility.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class HistoryDbEndpointConfig extends DbEndpointsConfig {

    private static final String HISTORY_SCHEMA_NAME = "vmtdb";

    /**
     * Create a {@link DbEndpoint} for accessing history DB.
     *
     * @return DbEndpoint
     */
    @Bean
    public DbEndpoint historyEndpoint() {
        return fixEndpointForMultiDb(dbEndpoint("dbs.history", SQLDialect.MARIADB)
                // TODO remove next line as part of OM-77149
                .withMigrationLocations("db.migration")
                .withShouldProvision(true)
                .withRootAccessEnabled(true)
                .withAccess(DbEndpointAccess.ALL)
                .withDatabaseName(HISTORY_SCHEMA_NAME)
                .withSchemaName(HISTORY_SCHEMA_NAME))
                .build();
    }
}
