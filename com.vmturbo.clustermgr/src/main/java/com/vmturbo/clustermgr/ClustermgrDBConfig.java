package com.vmturbo.clustermgr;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for clustermgr interaction with a schema.
 */
@Configuration
public class ClustermgrDBConfig extends SQLDatabaseConfig {
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

    /**
     * Initialize plan-orchestrator DB config by running flyway migration and creating a user.
     *
     * @return DataSource of plan-orchestrator DB.
     */
    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, clustermgrDbUsername, Optional.ofNullable(
                !Strings.isEmpty(clustermgrDbPassword) ? clustermgrDbPassword : null));
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}
