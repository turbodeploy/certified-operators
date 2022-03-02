package com.vmturbo.history.db;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.history.flyway.MigrationCallbackForVersion121;
import com.vmturbo.history.flyway.ResetChecksumsForMyIsamInfectedMigrations;
import com.vmturbo.history.flyway.V1_28_1_And_V1_35_1_Callback;
import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointBuilder;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.sql.utils.PostgresPlugins;
import com.vmturbo.sql.utils.flyway.ForgetMigrationCallback;

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
                .withSchemaName(HISTORY_SCHEMA_NAME))
                .withFlywayCallbacks(flywayCallbacks());
        if (sqlDialect == SQLDialect.POSTGRES) {
            builder = builder.withPlugins(PostgresPlugins.PARTMAN_4_6_0);
        }
        return builder.build();
    }

    private FlywayCallback[] flywayCallbacks() {
        switch (sqlDialect) {
            case MARIADB:
                return new FlywayCallback[]{
                    // V1.27 migrations collided when 7.17 and 7.21 branches were merged
                    new ForgetMigrationCallback("1.27"),
                    // three migrations were changed in order to remove mention of MyISAM DB engine
                    new ResetChecksumsForMyIsamInfectedMigrations(),
                    // V1.28.1 and V1.35.1 java migrations needed to change
                    // V1.28.1 formerly supplied a checksum but no longer does
                    new V1_28_1_And_V1_35_1_Callback(),
                    // V1.21 checksum has to change
                    new MigrationCallbackForVersion121()
                };
            case POSTGRES:
                return new FlywayCallback[]{};
            default:
                return new FlywayCallback[]{};
        }
    }

}
