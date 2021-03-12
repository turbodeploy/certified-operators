package com.vmturbo.action.orchestrator;

import java.util.Optional;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.util.Strings;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * Configuration for action-orchestrator component interaction with a schema.
 */
@Configuration
public class ActionOrchestratorDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${actionDbUsername:action}")
    private String actionDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${actionDbPassword:}")
    private String actionDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:action}")
    private String dbSchemaName;

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, actionDbUsername,
                Optional.ofNullable(!Strings.isEmpty(actionDbPassword) ? actionDbPassword : null));
    }

    /**
     * Flyway callbacks required for action-orchestrator.
     *
     * @return active callbacks
     */
    @Primary
    @Bean
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[]{
                // V1.19 migration was replaced due to performance-related failures at large
                // installations - see OM-67686
                new ResetMigrationChecksumCallback("1.19",
                        ImmutableSet.of(1998263184, -814613695), 1120921943)
        };
    }
}


