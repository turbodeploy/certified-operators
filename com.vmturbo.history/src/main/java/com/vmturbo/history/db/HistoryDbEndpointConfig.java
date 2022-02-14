package com.vmturbo.history.db;

import org.jooq.SQLDialect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointBuilder;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.sql.utils.PostgresPlugins;

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
        DbEndpointBuilder builder = fixEndpointForMultiDb(dbEndpoint("dbs.history", sqlDialect)
                .withShouldProvision(true)
                .withRootAccessEnabled(true)
                .withAccess(DbEndpointAccess.ALL)
                .withDatabaseName(HISTORY_SCHEMA_NAME)
                .withSchemaName(HISTORY_SCHEMA_NAME));
        if (sqlDialect == SQLDialect.POSTGRES) {
            builder = builder.withPlugins(PostgresPlugins.PARTMAN_4_6_0);
        }
        return builder.build();
    }
}
