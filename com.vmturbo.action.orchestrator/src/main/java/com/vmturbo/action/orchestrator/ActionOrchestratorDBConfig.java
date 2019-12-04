package com.vmturbo.action.orchestrator;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
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

    @Bean
    @Override
    public DataSource dataSource() {
        // If no db password specified, use root password by default.
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
            authRetryDelaySecs);
        String dbPassword = !Strings.isEmpty(actionDbPassword) ?
            actionDbPassword : dbPasswordUtil.getSqlDbRootPassword();
        return dataSourceConfig(dbSchemaName, actionDbUsername, dbPassword);
    }
}
