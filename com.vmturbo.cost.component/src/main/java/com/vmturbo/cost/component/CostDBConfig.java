package com.vmturbo.cost.component;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for cost component interaction with a schema.
 */
@Configuration
public class CostDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${costDbUsername:cost}")
    private String costDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${costDbPassword:}")
    private String costDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:cost}")
    private String dbSchemaName;

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, costDbUsername, Optional.ofNullable(
                !Strings.isEmpty(costDbPassword) ? costDbPassword : null));
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}
