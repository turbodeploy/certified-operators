package com.vmturbo.action.orchestrator;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

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
}
